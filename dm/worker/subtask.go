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
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/siddontang/go/sync2"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/unit"
	"github.com/pingcap/dm/dumpling"
	"github.com/pingcap/dm/loader"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/shardddl/pessimism"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/pingcap/dm/syncer"
)

// createRealUnits is subtask units initializer
// it can be used for testing
var createUnits = createRealUnits

// createRealUnits creates process units base on task mode
func createRealUnits(cfg *config.SubTaskConfig, etcdClient *clientv3.Client) []unit.Unit {
	failpoint.Inject("mockCreateUnitsDumpOnly", func(_ failpoint.Value) {
		log.L().Info("create mock worker units with dump unit only", zap.String("failpoint", "mockCreateUnitsDumpOnly"))
		failpoint.Return([]unit.Unit{dumpling.NewDumpling(cfg)})
	})

	us := make([]unit.Unit, 0, 3)
	switch cfg.Mode {
	case config.ModeAll:
		us = append(us, dumpling.NewDumpling(cfg))
		us = append(us, loader.NewLoader(cfg))
		us = append(us, syncer.NewSyncer(cfg, etcdClient))
	case config.ModeFull:
		// NOTE: maybe need another checker in the future?
		us = append(us, dumpling.NewDumpling(cfg))
		us = append(us, loader.NewLoader(cfg))
	case config.ModeIncrement:
		us = append(us, syncer.NewSyncer(cfg, etcdClient))
	default:
		log.L().Error("unsupported task mode", zap.String("subtask", cfg.Name), zap.String("task mode", cfg.Mode))
	}
	return us
}

// SubTask represents a sub task of data migration
type SubTask struct {
	cfg *config.SubTaskConfig

	initialized sync2.AtomicBool

	l log.Logger

	sync.RWMutex
	wg sync.WaitGroup
	// ctx is used for the whole subtask. It will be created only when we new a subtask.
	ctx    context.Context
	cancel context.CancelFunc
	// currCtx is used for one loop. It will be created each time we use st.run/st.Resume
	currCtx    context.Context
	currCancel context.CancelFunc

	units    []unit.Unit // units do job one by one
	currUnit unit.Unit
	prevUnit unit.Unit

	stage  pb.Stage          // stage of current sub task
	result *pb.ProcessResult // the process result, nil when is processing

	etcdClient *clientv3.Client
}

// NewSubTask is subtask initializer
// it can be used for testing
var NewSubTask = NewRealSubTask

// NewRealSubTask creates a new SubTask
func NewRealSubTask(cfg *config.SubTaskConfig, etcdClient *clientv3.Client) *SubTask {
	return NewSubTaskWithStage(cfg, pb.Stage_New, etcdClient)
}

// NewSubTaskWithStage creates a new SubTask with stage
func NewSubTaskWithStage(cfg *config.SubTaskConfig, stage pb.Stage, etcdClient *clientv3.Client) *SubTask {
	ctx, cancel := context.WithCancel(context.Background())
	st := SubTask{
		cfg:        cfg,
		stage:      stage,
		l:          log.With(zap.String("subtask", cfg.Name)),
		ctx:        ctx,
		cancel:     cancel,
		etcdClient: etcdClient,
	}
	taskState.WithLabelValues(st.cfg.Name, st.cfg.SourceID).Set(float64(st.stage))
	return &st
}

// Init initializes the sub task processing units
func (st *SubTask) Init() error {
	st.units = createUnits(st.cfg, st.etcdClient)
	if len(st.units) < 1 {
		return terror.ErrWorkerNoAvailUnits.Generate(st.cfg.Name, st.cfg.Mode)
	}

	initializeUnitSuccess := true
	// when error occurred, initialized units should be closed
	// when continue sub task from loader / syncer, ahead units should be closed
	var needCloseUnits []unit.Unit
	defer func() {
		for _, u := range needCloseUnits {
			u.Close()
		}

		st.initialized.Set(initializeUnitSuccess)
	}()

	// every unit does base initialization in `Init`, and this must pass before start running the sub task
	// other setups can be done in `Process`, like Loader's prepare which depends on Mydumper's output
	// but setups in `Process` should be treated carefully, let it's compatible with Pause / Resume
	for i, u := range st.units {
		ctx, cancel := context.WithTimeout(context.Background(), unit.DefaultInitTimeout)
		err := u.Init(ctx)
		cancel()
		if err != nil {
			initializeUnitSuccess = false
			// when init fail, other units initialized before should be closed
			for j := 0; j < i; j++ {
				needCloseUnits = append(needCloseUnits, st.units[j])
			}
			return terror.Annotatef(err, "fail to initial unit %s of subtask %s ", u.Type(), st.cfg.Name)
		}
	}

	// if the sub task ran before, some units may be skipped
	var skipIdx = 0
	for i := len(st.units) - 1; i > 0; i-- {
		u := st.units[i]
		ctx, cancel := context.WithTimeout(context.Background(), unit.DefaultInitTimeout)
		isFresh, err := u.IsFreshTask(ctx)
		cancel()
		if err != nil {
			initializeUnitSuccess = false
			return terror.Annotatef(err, "fail to get fresh status of subtask %s %s", st.cfg.Name, u.Type())
		} else if !isFresh {
			skipIdx = i
			st.l.Info("continue unit", zap.Stringer("unit", u.Type()))
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
		st.l.Warn("prepare to run", zap.Stringer("stage", st.Stage()))
		return
	}

	err := st.Init()
	if err != nil {
		st.l.Error("fail to initial subtask", log.ShortError(err))
		st.fail(err)
		return
	}

	st.run()
}

func (st *SubTask) run() {
	st.setStage(pb.Stage_Running)
	ctx, cancel := context.WithCancel(st.ctx)
	st.setCurrCtx(ctx, cancel)
	err := st.unitTransWaitCondition(ctx)
	if err != nil {
		st.l.Error("wait condition", log.ShortError(err))
		st.fail(err)
		return
	} else if ctx.Err() != nil {
		return
	}

	st.setResult(nil) // clear previous result
	cu := st.CurrUnit()
	st.l.Info("start to run", zap.Stringer("unit", cu.Type()))
	pr := make(chan pb.ProcessResult, 1)
	st.wg.Add(1)
	go st.fetchResult(pr)
	go cu.Process(ctx, pr)
}

func (st *SubTask) setCurrCtx(ctx context.Context, cancel context.CancelFunc) {
	st.Lock()
	// call previous cancel func for safety
	if st.currCancel != nil {
		st.currCancel()
	}
	st.currCtx = ctx
	st.currCancel = cancel
	st.Unlock()
}

func (st *SubTask) callCurrCancel() {
	st.RLock()
	st.currCancel()
	st.RUnlock()
}

// fetchResult fetches units process result
// when dm-unit report an error, we need to re-Process the sub task
func (st *SubTask) fetchResult(pr chan pb.ProcessResult) {
	defer st.wg.Done()

	select {
	case <-st.ctx.Done():
		// should not use st.currCtx, because will do st.currCancel when Pause task,
		// and this function will return, and the unit's Process maybe still running.
		return
	case result := <-pr:
		// filter the context canceled error
		errs := make([]*pb.ProcessError, 0, 2)
		for _, err := range result.Errors {
			if !unit.IsCtxCanceledProcessErr(err) {
				errs = append(errs, err)
			}
		}
		result.Errors = errs

		st.setResult(&result) // save result
		st.callCurrCancel()   // dm-unit finished, canceled or error occurred, always cancel processing

		if len(result.Errors) == 0 && st.Stage() == pb.Stage_Pausing {
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
			stage = pb.Stage_Paused // error occurred, paused
		}
		st.setStage(stage)

		st.l.Info("unit process returned", zap.Stringer("unit", cu.Type()), zap.Stringer("stage", stage), zap.String("status", st.StatusJSON()))

		switch stage {
		case pb.Stage_Finished:
			cu.Close()
			nu := st.getNextUnit()
			if nu == nil {
				// Now, when finished, it only stops the process
				// if needed, we can refine to Close it
				st.l.Info("all process units finished")
			} else {
				st.l.Info("switching to next unit", zap.Stringer("unit", cu.Type()))
				st.setCurrUnit(nu)
				// NOTE: maybe need a Lock mechanism for sharding scenario
				st.run() // re-run for next process unit
			}
		case pb.Stage_Stopped:
		case pb.Stage_Paused:
			for _, err := range result.Errors {
				st.l.Error("unit process error", zap.Stringer("unit", cu.Type()), zap.Reflect("error information", err))
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
		st.l.Info("closing unit process", zap.Stringer("unit", cu.Type()))
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
	taskState.WithLabelValues(st.cfg.Name, st.cfg.SourceID).Set(float64(st.stage))
}

// stageCAS sets stage to newStage if its current value is oldStage
func (st *SubTask) stageCAS(oldStage, newStage pb.Stage) bool {
	st.Lock()
	defer st.Unlock()

	if st.stage == oldStage {
		st.stage = newStage
		taskState.WithLabelValues(st.cfg.Name, st.cfg.SourceID).Set(float64(st.stage))
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
		taskState.WithLabelValues(st.cfg.Name, st.cfg.SourceID).Set(float64(st.stage))
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
	st.l.Info("closing")
	if st.Stage() == pb.Stage_Stopped {
		st.l.Info("subTask is already closed, no need to close")
		return
	}

	st.cancel()
	st.closeUnits() // close all un-closed units
	st.removeLabelValuesWithTaskInMetrics(st.cfg.Name, st.cfg.SourceID)
	st.wg.Wait()
	st.setStageIfNot(pb.Stage_Finished, pb.Stage_Stopped)
}

// Pause pauses the running sub task
func (st *SubTask) Pause() error {
	if !st.stageCAS(pb.Stage_Running, pb.Stage_Pausing) {
		return terror.ErrWorkerNotRunningStage.Generate(st.Stage().String())
	}

	st.callCurrCancel()
	st.wg.Wait() // wait fetchResult return

	cu := st.CurrUnit()
	cu.Pause()

	st.l.Info("paused", zap.Stringer("unit", cu.Type()))
	st.setStage(pb.Stage_Paused)
	return nil
}

// Resume resumes the paused sub task
// similar to Run
func (st *SubTask) Resume() error {
	if !st.initialized.Get() {
		st.Run()
		return nil
	}

	if !st.stageCAS(pb.Stage_Paused, pb.Stage_Resuming) {
		return terror.ErrWorkerNotPausedStage.Generate(st.Stage().String())
	}

	ctx, cancel := context.WithCancel(st.ctx)
	st.setCurrCtx(ctx, cancel)
	// NOTE: this may block if user resume a task
	err := st.unitTransWaitCondition(ctx)
	if err != nil {
		st.l.Error("wait condition", log.ShortError(err))
		st.setStage(pb.Stage_Paused)
		return err
	} else if ctx.Err() != nil {
		// ctx.Err() != nil means this context is canceled in other go routine,
		// that go routine will change the stage, so don't need to set stage to paused here.
		return nil
	}

	st.setResult(nil) // clear previous result
	cu := st.CurrUnit()
	st.l.Info("resume with unit", zap.Stringer("unit", cu.Type()))

	pr := make(chan pb.ProcessResult, 1)
	st.wg.Add(1)
	go st.fetchResult(pr)
	go cu.Resume(ctx, pr)

	st.setStage(pb.Stage_Running)
	return nil
}

// Update update the sub task's config
func (st *SubTask) Update(cfg *config.SubTaskConfig) error {
	if !st.stageCAS(pb.Stage_Paused, pb.Stage_Paused) { // only test for Paused
		return terror.ErrWorkerUpdateTaskStage.Generate(st.Stage().String())
	}

	// update all units' configuration, if SubTask itself has configuration need to update, do it later
	for _, u := range st.units {
		err := u.Update(cfg)
		if err != nil {
			return err
		}
	}

	return nil
}

// OperateSchema operates schema for an upstream table.
func (st *SubTask) OperateSchema(ctx context.Context, req *pb.OperateWorkerSchemaRequest) (schema string, err error) {
	if st.Stage() != pb.Stage_Paused {
		return "", terror.ErrWorkerNotPausedStage.Generate(st.Stage().String())
	}

	syncUnit, ok := st.currUnit.(*syncer.Syncer)
	if !ok {
		return "", terror.ErrWorkerOperSyncUnitOnly.Generate(st.currUnit.Type())
	}

	return syncUnit.OperateSchema(ctx, req)
}

// UpdateFromConfig updates config for `From`
func (st *SubTask) UpdateFromConfig(cfg *config.SubTaskConfig) error {
	st.Lock()
	defer st.Unlock()

	if sync, ok := st.currUnit.(*syncer.Syncer); ok {
		err := sync.UpdateFromConfig(cfg)
		if err != nil {
			return err
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

// ShardDDLInfo returns the current shard DDL info.
func (st *SubTask) ShardDDLInfo() *pessimism.Info {
	st.RLock()
	defer st.RUnlock()

	cu := st.currUnit
	syncer2, ok := cu.(*syncer.Syncer)
	if !ok {
		return nil
	}

	return syncer2.ShardDDLInfo()
}

// ShardDDLOperation returns the current shard DDL lock operation.
func (st *SubTask) ShardDDLOperation() *pessimism.Operation {
	st.RLock()
	defer st.RUnlock()

	cu := st.currUnit
	syncer2, ok := cu.(*syncer.Syncer)
	if !ok {
		return nil
	}

	return syncer2.ShardDDLOperation()
}

// unitTransWaitCondition waits when transferring from current unit to next unit.
// Currently there is only one wait condition
// from Load unit to Sync unit, wait for relay-log catched up with mydumper binlog position.
func (st *SubTask) unitTransWaitCondition(subTaskCtx context.Context) error {
	pu := st.PrevUnit()
	cu := st.CurrUnit()
	if pu != nil && pu.Type() == pb.UnitType_Load && cu.Type() == pb.UnitType_Sync {
		st.l.Info("wait condition between two units", zap.Stringer("previous unit", pu.Type()), zap.Stringer("unit", cu.Type()))
		hub := GetConditionHub()

		if hub.w.relayHolder == nil {
			return nil
		}

		waitRelayCatchupTimeout := 5 * time.Minute
		ctx, cancel := context.WithTimeout(hub.w.ctx, waitRelayCatchupTimeout)
		defer cancel()

		loadStatus := pu.Status().(*pb.LoadStatus)
		pos1, err := utils.DecodeBinlogPosition(loadStatus.MetaBinlog)
		if err != nil {
			return terror.WithClass(err, terror.ClassDMWorker)
		}
		for {
			relayStatus := hub.w.relayHolder.Status()
			pos2, err := utils.DecodeBinlogPosition(relayStatus.RelayBinlog)
			if err != nil {
				return terror.WithClass(err, terror.ClassDMWorker)
			}
			if pos1.Compare(*pos2) <= 0 {
				break
			}
			st.l.Debug("wait relay to catchup", zap.Stringer("load end position", pos1), zap.Stringer("relay position", pos2))

			select {
			case <-ctx.Done():
				return terror.ErrWorkerWaitRelayCatchupTimeout.Generate(waitRelayCatchupTimeout, pos1, pos2)
			case <-subTaskCtx.Done():
				return nil
			case <-time.After(time.Millisecond * 50):
			}
		}
		st.l.Info("relay binlog pos catchup loader end binlog pos")
	}
	return nil
}

func (st *SubTask) fail(err error) {
	st.setStage(pb.Stage_Paused)
	st.setResult(&pb.ProcessResult{
		Errors: []*pb.ProcessError{
			unit.NewProcessError(err),
		},
	})
}

// HandleError handle error for syncer unit
func (st *SubTask) HandleError(ctx context.Context, req *pb.HandleWorkerErrorRequest) error {
	syncUnit, ok := st.currUnit.(*syncer.Syncer)
	if !ok {
		return terror.ErrWorkerOperSyncUnitOnly.Generate(st.currUnit.Type())
	}

	err := syncUnit.HandleError(ctx, req)
	if err != nil {
		return err
	}

	if st.Stage() == pb.Stage_Paused {
		err = st.Resume()
	}
	return err
}
