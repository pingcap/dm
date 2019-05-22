// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package worker

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/unit"
	"github.com/pingcap/dm/loader"
	"github.com/pingcap/dm/mydumper"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/pingcap/dm/syncer"
	"github.com/pingcap/errors"
	"github.com/siddontang/go/sync2"

	// hack for glide update, remove it later
	_ "github.com/pingcap/tidb-tools/pkg/check"
	_ "github.com/pingcap/tidb-tools/pkg/dbutil"
	_ "github.com/pingcap/tidb-tools/pkg/utils"
)

// createUnits creates process units base on task mode
func createUnits(cfg *config.SubTaskConfig) []unit.Unit {
	us := make([]unit.Unit, 0, 3)
	switch cfg.Mode {
	case config.ModeAll:
		us = append(us, mydumper.NewMydumper(cfg))
		us = append(us, loader.NewLoader(cfg))
		us = append(us, syncer.NewSyncer(cfg))
	case config.ModeFull:
		// NOTE: maybe need another checker in the future?
		us = append(us, mydumper.NewMydumper(cfg))
		us = append(us, loader.NewLoader(cfg))
	case config.ModeIncrement:
		us = append(us, syncer.NewSyncer(cfg))
	default:
		log.Errorf("[subtask] unsupported task mode %s", cfg.Mode)
	}
	return us
}

// SubTask represents a sub task of data migration
type SubTask struct {
	cfg *config.SubTaskConfig

	initialized sync2.AtomicBool

	sync.RWMutex
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	units    []unit.Unit // units do job one by one
	currUnit unit.Unit
	prevUnit unit.Unit

	stage  pb.Stage          // stage of current sub task
	result *pb.ProcessResult // the process result, nil when is processing

	// only support sync one DDL lock one time, refine if needed
	DDLInfo      chan *pb.DDLInfo // DDL info pending to sync
	ddlLockInfo  *pb.DDLLockInfo  // DDL lock info which waiting other dm-workers to sync
	cacheDDLInfo *pb.DDLInfo
}

// NewSubTask creates a new SubTask
func NewSubTask(cfg *config.SubTaskConfig) *SubTask {
	return NewSubTaskWithStage(cfg, pb.Stage_New)
}

// NewSubTaskWithStage creates a new SubTask with stage
func NewSubTaskWithStage(cfg *config.SubTaskConfig, stage pb.Stage) *SubTask {
	st := SubTask{
		cfg:   cfg,
		units: createUnits(cfg),
		stage: stage,
	}
	taskState.WithLabelValues(st.cfg.Name).Set(float64(st.stage))
	return &st
}

// Init initializes the sub task processing units
func (st *SubTask) Init() error {
	if len(st.units) < 1 {
		return errors.Errorf("subtask %s has no dm units for mode %s", st.cfg.Name, st.cfg.Mode)
	}

	st.DDLInfo = make(chan *pb.DDLInfo, 1)

	// when error occurred, initialized units should be closed
	// when continue sub task from loader / syncer, ahead units should be closed
	var needCloseUnits []unit.Unit
	defer func() {
		for _, u := range needCloseUnits {
			u.Close()
		}

		st.initialized.Set(true)
	}()

	// every unit does base initialization in `Init`, and this must pass before start running the sub task
	// other setups can be done in `Process`, like Loader's prepare which depends on Mydumper's output
	// but setups in `Process` should be treated carefully, let it's compatible with Pause / Resume
	for i, u := range st.units {
		err := u.Init()
		if err != nil {
			// when init fail, other units initialized before should be closed
			for j := 0; j < i; j++ {
				needCloseUnits = append(needCloseUnits, st.units[j])
			}
			return errors.Annotatef(err, "fail to initial unit %s of subtask %s ", u.Type(), st.cfg.Name)
		}
	}

	// if the sub task ran before, some units may be skipped
	var skipIdx = 0
	for i := len(st.units) - 1; i > 0; i-- {
		u := st.units[i]
		isFresh, err := u.IsFreshTask()
		if err != nil {
			return errors.Annotatef(err, "fail to get fresh status of subtask %s %s", st.cfg.Name, u.Type())
		} else if !isFresh {
			skipIdx = i
			log.Infof("[subtask] %s run %s dm-unit before, continue with it", st.cfg.Name, u.Type())
			break
		}
	}

	needCloseUnits = st.units[:skipIdx]
	st.units = st.units[skipIdx:]

	st.setCurrUnit(st.units[0])
	return nil
}

// Run runs the sub task
func (st *SubTask) Run() {
	if st.Stage() == pb.Stage_Finished || st.Stage() == pb.Stage_Running {
		log.Warnf("[subtask] %s is %s", st.cfg.Name, st.Stage())
		return
	}

	err := st.Init()
	if err != nil {
		log.Errorf("[subtask] fail to initial %v", err)
		st.fail(errors.ErrorStack(err))
		return
	}

	st.run()
}

func (st *SubTask) run() {
	st.setStage(pb.Stage_Paused)
	err := st.unitTransWaitCondition()
	if err != nil {
		log.Errorf("[subtask] wait condition error: %v", err)
		st.fail(errors.ErrorStack(err))
		return
	}

	st.setStage(pb.Stage_Running)
	st.setResult(nil) // clear previous result
	cu := st.CurrUnit()
	log.Infof("[subtask] %s start running %s dm-unit", st.cfg.Name, cu.Type())
	st.ctx, st.cancel = context.WithCancel(context.Background())
	pr := make(chan pb.ProcessResult, 1)
	st.wg.Add(1)
	go st.fetchResult(pr)
	go cu.Process(st.ctx, pr)

	st.wg.Add(1)
	go st.fetchUnitDDLInfo(st.ctx)
}

// fetchResult fetches units process result
// when dm-unit report an error, we need to re-Process the sub task
func (st *SubTask) fetchResult(pr chan pb.ProcessResult) {
	defer st.wg.Done()

retry:
	select {
	case <-st.ctx.Done():
		return
	case result := <-pr:
		st.setResult(&result) // save result
		st.cancel()           // dm-unit finished, canceled or error occurred, always cancel processing

		if len(result.Errors) == 0 && st.Stage() == pb.Stage_Paused {
			return // paused by external request
		}

		var (
			cu    = st.CurrUnit()
			stage pb.Stage
		)
		if len(result.Errors) == 0 {
			if result.IsCanceled {
				stage = pb.Stage_Stopped // canceled by user
			} else {
				stage = pb.Stage_Finished // process finished with no error
			}
		} else {
			/* TODO
			it's a poor and very rough retry feature, the main reason is that
			the concurrency control of the sub task module is very confusing and needs to be optimized.
			After improving its state transition and concurrency control,
			I will optimize the implementation of retry feature.
			*/
			if st.retryErrors(result.Errors, cu) {
				log.Warnf("[subtask] %s (%s) retry on error %v, waiting 10 seconds!", st.cfg.Name, cu.Type(), result.Errors)
				st.ctx, st.cancel = context.WithCancel(context.Background())
				time.Sleep(10 * time.Second)
				go cu.Resume(st.ctx, pr)
				goto retry
			}

			stage = pb.Stage_Paused // error occurred, paused
		}
		st.setStage(stage)

		log.Infof("[subtask] %s dm-unit %s process returned with stage %s, status %s", st.cfg.Name, cu.Type(), stage.String(), st.StatusJSON())

		switch stage {
		case pb.Stage_Finished:
			cu.Close()
			nu := st.getNextUnit()
			if nu == nil {
				// Now, when finished, it only stops the process
				// if needed, we can refine to Close it
				log.Infof("[subtask] %s all process units finished", st.cfg.Name)
			} else {
				log.Infof("[subtask] %s switching to next dm-unit %s", st.cfg.Name, nu.Type())
				st.setCurrUnit(nu)
				// NOTE: maybe need a Lock mechanism for sharding scenario
				st.run() // re-run for next process unit
			}
		case pb.Stage_Stopped:
		case pb.Stage_Paused:
			for _, err := range result.Errors {
				log.Errorf("[subtask] %s dm-unit %s process error with type %v:\n %v",
					st.cfg.Name, cu.Type(), err.Type, err.Msg)
			}
		}
	}
}

// setCurrUnit set current dm unit to ut and returns previous unit
func (st *SubTask) setCurrUnit(ut unit.Unit) unit.Unit {
	st.Lock()
	defer st.Unlock()
	pu := st.currUnit
	st.currUnit = ut
	st.prevUnit = pu
	return pu
}

// CurrUnit returns current dm unit
func (st *SubTask) CurrUnit() unit.Unit {
	st.RLock()
	defer st.RUnlock()
	return st.currUnit
}

// PrevUnit returns dm previous unit
func (st *SubTask) PrevUnit() unit.Unit {
	st.RLock()
	defer st.RUnlock()
	return st.prevUnit
}

// closeUnits closes all un-closed units (current unit and all the subsequent units)
func (st *SubTask) closeUnits() {
	st.RLock()
	defer st.RUnlock()
	var (
		cu  = st.currUnit
		cui = -1
	)

	for i, u := range st.units {
		if u == cu {
			cui = i
			break
		}
	}
	if cui < 0 {
		return
	}
	for i := cui; i < len(st.units); i++ {
		u := st.units[i]
		log.Infof("[syncer] closing process unit %s", u.Type())
		u.Close()
	}
}

// getNextUnit gets the next process unit from st.units
// if no next unit, return nil
func (st *SubTask) getNextUnit() unit.Unit {
	var (
		nu  unit.Unit
		cui = len(st.units)
		cu  = st.CurrUnit()
	)
	for i, u := range st.units {
		if u == cu {
			cui = i
		}
		if i == cui+1 {
			nu = u
			break
		}
	}
	return nu
}

func (st *SubTask) setStage(stage pb.Stage) {
	st.Lock()
	defer st.Unlock()
	st.stage = stage
	taskState.WithLabelValues(st.cfg.Name).Set(float64(st.stage))
}

// stageCAS sets stage to newStage if its current value is oldStage
func (st *SubTask) stageCAS(oldStage, newStage pb.Stage) bool {
	st.Lock()
	defer st.Unlock()
	if st.stage == oldStage {
		st.stage = newStage
		taskState.WithLabelValues(st.cfg.Name).Set(float64(st.stage))
		return true
	}
	return false
}

// setStageIfNot sets stage to newStage if its current value is not oldStage, similar to CAS
func (st *SubTask) setStageIfNot(oldStage, newStage pb.Stage) bool {
	st.Lock()
	defer st.Unlock()
	if st.stage != oldStage {
		st.stage = newStage
		taskState.WithLabelValues(st.cfg.Name).Set(float64(st.stage))
		return true
	}
	return false
}

// Stage returns the stage of the sub task
func (st *SubTask) Stage() pb.Stage {
	st.RLock()
	defer st.RUnlock()
	return st.stage
}

func (st *SubTask) setResult(result *pb.ProcessResult) {
	st.Lock()
	defer st.Unlock()
	st.result = result
}

// Result returns the result of the sub task
func (st *SubTask) Result() *pb.ProcessResult {
	st.RLock()
	defer st.RUnlock()
	return st.result
}

// Close stops the sub task
func (st *SubTask) Close() {
	log.Infof("[subtask] %s is closing", st.cfg.Name)
	if st.cancel == nil {
		log.Infof("[subtask] not run yet, no need to close")
		return
	}

	st.cancel()
	st.closeUnits() // close all un-closed units
	st.setStageIfNot(pb.Stage_Finished, pb.Stage_Stopped)
	st.wg.Wait()
}

// Pause pauses the running sub task
func (st *SubTask) Pause() error {
	if !st.stageCAS(pb.Stage_Running, pb.Stage_Paused) {
		return errors.NotValidf("current stage is not running")
	}

	st.cancel()
	st.wg.Wait() // wait fetchResult return

	cu := st.CurrUnit()
	cu.Pause()

	log.Infof("[subtask] %s paused with %s dm-unit", st.cfg.Name, cu.Type())
	return nil
}

// Resume resumes the paused sub task
// similar to Run
func (st *SubTask) Resume() error {
	if !st.initialized.Get() {
		st.Run()
		return nil
	}

	// NOTE: this may block if user resume a task
	err := st.unitTransWaitCondition()
	if err != nil {
		log.Errorf("[subtask] wait condition error: %v", err)
		st.setStage(pb.Stage_Paused)
		return errors.Trace(err)
	}

	if !st.stageCAS(pb.Stage_Paused, pb.Stage_Running) {
		return errors.NotValidf("current stage is not paused")
	}

	st.setResult(nil) // clear previous result
	cu := st.CurrUnit()
	log.Infof("[subtask] %s resuming with %s dm-unit", st.cfg.Name, cu.Type())

	st.ctx, st.cancel = context.WithCancel(context.Background())
	pr := make(chan pb.ProcessResult, 1)
	st.wg.Add(1)
	go st.fetchResult(pr)
	go cu.Resume(st.ctx, pr)

	st.wg.Add(1)
	go st.fetchUnitDDLInfo(st.ctx)
	return nil
}

// Update update the sub task's config
func (st *SubTask) Update(cfg *config.SubTaskConfig) error {
	if !st.stageCAS(pb.Stage_Paused, pb.Stage_Paused) { // only test for Paused
		return errors.Errorf("can only update task on Paused stage, but current stage is %s", st.Stage().String())
	}

	// update all units' configuration, if SubTask itself has configuration need to update, do it later
	for _, u := range st.units {
		err := u.Update(cfg)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// fetchUnitDDLInfo fetches DDL info from current processing unit
// when unit switched, returns and starts fetching again for new unit
func (st *SubTask) fetchUnitDDLInfo(ctx context.Context) {
	defer st.wg.Done()

	cu := st.CurrUnit()
	syncer2, ok := cu.(*syncer.Syncer)
	if !ok {
		return
	}

	// discard previous saved DDLInfo
	// when process unit resuming, un-resolved DDL will send again
	for len(st.DDLInfo) > 0 {
		<-st.DDLInfo
	}

	for {
		select {
		case <-ctx.Done():
			return
		case info, ok := <-syncer2.DDLInfo():
			if !ok {
				return
			}
			select {
			case <-ctx.Done():
				return
			case st.DDLInfo <- info:
			}
		}
	}
}

// SendBackDDLInfo sends DDL info back for pending
func (st *SubTask) SendBackDDLInfo(ctx context.Context, info *pb.DDLInfo) bool {
	select {
	case <-ctx.Done():
		return false
	case st.DDLInfo <- info:
		return true
	}
}

// ExecuteDDL requests current unit to execute a DDL
func (st *SubTask) ExecuteDDL(ctx context.Context, req *pb.ExecDDLRequest) error {
	// NOTE: check current stage?
	cu := st.CurrUnit()
	syncer2, ok := cu.(*syncer.Syncer)
	if !ok {
		return errors.Errorf("only syncer support ExecuteDDL, but current unit is %s", cu.Type().String())
	}
	chResp, err := syncer2.ExecuteDDL(ctx, req)
	if err != nil {
		return errors.Trace(err)
	}

	// also any timeout
	timeout := time.Duration(syncer.MaxDDLConnectionTimeoutMinute)*time.Minute + 30*time.Second
	ctxTimeout, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	select {
	case err = <-chResp: // block until complete ddl execution
		return errors.Trace(err)
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case <-ctxTimeout.Done():
		return errors.New("ExecuteDDL timeout, try use `query-status` to query whether the DDL is still blocking")
	}
}

// SaveDDLLockInfo saves a DDLLockInfo
func (st *SubTask) SaveDDLLockInfo(info *pb.DDLLockInfo) error {
	st.Lock()
	defer st.Unlock()
	if st.ddlLockInfo != nil {
		return errors.AlreadyExistsf("DDLLockInfo for task %s", info.Task)
	}
	st.ddlLockInfo = info
	return nil
}

// SetSyncerSQLOperator sets an operator to syncer.
func (st *SubTask) SetSyncerSQLOperator(ctx context.Context, req *pb.HandleSubTaskSQLsRequest) error {
	syncUnit, ok := st.currUnit.(*syncer.Syncer)
	if !ok {
		return errors.Errorf("such operation is only available for syncer, but now syncer is not running. current unit is %s", st.currUnit.Type())
	}

	// special handle for INJECT
	if req.Op == pb.SQLOp_INJECT {
		return syncUnit.InjectSQLs(ctx, req.Args)
	}

	return errors.Trace(syncUnit.SetSQLOperator(req))
}

// ClearDDLLockInfo clears current DDLLockInfo
func (st *SubTask) ClearDDLLockInfo() {
	st.Lock()
	defer st.Unlock()
	st.ddlLockInfo = nil
}

// DDLLockInfo returns current DDLLockInfo, maybe nil
func (st *SubTask) DDLLockInfo() *pb.DDLLockInfo {
	st.RLock()
	defer st.RUnlock()
	return st.ddlLockInfo
}

// UpdateFromConfig updates config for `From`
func (st *SubTask) UpdateFromConfig(cfg *config.SubTaskConfig) error {
	st.Lock()
	defer st.Unlock()

	if sync, ok := st.currUnit.(*syncer.Syncer); ok {
		err := sync.UpdateFromConfig(cfg)
		if err != nil {
			return errors.Trace(err)
		}
	}

	st.cfg.From = cfg.From

	return nil
}

// CheckUnit checks whether current unit is sync unit
func (st *SubTask) CheckUnit() bool {
	st.Lock()
	defer st.Unlock()

	flag := true

	if _, ok := st.currUnit.(*syncer.Syncer); !ok {
		flag = false
	}

	return flag
}

// SaveDDLInfo saves a CacheDDLInfo.
func (st *SubTask) SaveDDLInfo(info *pb.DDLInfo) error {
	st.Lock()
	defer st.Unlock()
	if st.cacheDDLInfo != nil {
		return errors.AlreadyExistsf("CacheDDLInfo for task %s", info.Task)
	}
	st.cacheDDLInfo = info
	return nil
}

// GetDDLInfo returns current CacheDDLInfo.
func (st *SubTask) GetDDLInfo() *pb.DDLInfo {
	st.RLock()
	defer st.RUnlock()
	return st.cacheDDLInfo
}

// ClearDDLInfo clears current CacheDDLInfo.
func (st *SubTask) ClearDDLInfo() {
	st.Lock()
	defer st.Unlock()
	st.cacheDDLInfo = nil
}

// unitTransWaitCondition waits when transferring from current unit to next unit.
// Currently there is only one wait condition
// from Load unit to Sync unit, wait for relay-log catched up with mydumper binlog position.
func (st *SubTask) unitTransWaitCondition() error {
	pu := st.PrevUnit()
	cu := st.CurrUnit()
	if pu != nil && pu.Type() == pb.UnitType_Load && cu.Type() == pb.UnitType_Sync {
		log.Infof("[subtask] %s wait condition between %s and %s", st.cfg.Name, pu.Type(), cu.Type())
		hub := GetConditionHub()
		ctx, cancel := context.WithTimeout(hub.w.ctx, 5*time.Minute)
		defer cancel()

		loadStatus := pu.Status().(*pb.LoadStatus)
		pos1, err := utils.DecodeBinlogPosition(loadStatus.MetaBinlog)
		if err != nil {
			return errors.Trace(err)
		}
		for {
			relayStatus := hub.w.relayHolder.Status()
			pos2, err := utils.DecodeBinlogPosition(relayStatus.RelayBinlog)
			if err != nil {
				return errors.Trace(err)
			}
			if pos1.Compare(*pos2) <= 0 {
				break
			}
			log.Debugf("loader end binlog pos: %s, relay binlog pos: %s, wait for catchup", pos1, pos2)

			select {
			case <-ctx.Done():
				return errors.Errorf("wait relay catchup timeout, loader end binlog pos: %s, relay binlog pos: %s", pos1, pos2)
			case <-time.After(time.Millisecond * 50):
			}
		}
		log.Info("relay binlog pos catchup loader end binlog pos")
	}
	return nil
}

func (st *SubTask) retryErrors(errors []*pb.ProcessError, current unit.Unit) bool {
	retry := true
	switch current.Type() {
	case pb.UnitType_Sync:
		for _, err := range errors {
			if strings.Contains(err.Msg, "invalid connection") {
				continue
			}
			retry = false
		}
	default:
		retry = false
	}

	return retry
}

func (st *SubTask) fail(message string) {
	st.setStage(pb.Stage_Paused)
	st.setResult(&pb.ProcessResult{
		Errors: []*pb.ProcessError{
			{
				Type: pb.ErrorType_UnknownError,
				Msg:  message,
			},
		},
	})
}
