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
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/siddontang/go/sync2"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/tracing"
	"github.com/pingcap/dm/relay/purger"
)

var (
	// sub tasks may changed, so we re-FetchDDLInfo at intervals
	reFetchInterval = 10 * time.Second

	closedFalse int32
	closedTrue  int32 = 1
)

// Worker manages sub tasks and process units for data migration
type Worker struct {
	// ensure no other operation can be done when closing (we can use `WatGroup`/`Context` to archive this)
	sync.RWMutex

	wg     sync.WaitGroup
	closed sync2.AtomicInt32

	// context created when Worker created, and canceled when closing
	ctx    context.Context
	cancel context.CancelFunc

	cfg *config.MysqlConfig
	l   log.Logger

	subTaskHolder *subTaskHolder

	relayHolder RelayHolder
	relayPurger purger.Purger

	tracer *tracing.Tracer

	taskStatusChecker TaskStatusChecker
	configFile        string
}

// NewWorker creates a new Worker
func NewWorker(cfg *config.MysqlConfig) (w *Worker, err error) {
	w = &Worker{
		cfg:           cfg,
		relayHolder:   NewRelayHolder(cfg),
		tracer:        tracing.InitTracerHub(cfg.Tracer),
		subTaskHolder: newSubTaskHolder(),
		l:             log.With(zap.String("component", "worker controller")),
	}
	w.ctx, w.cancel = context.WithCancel(context.Background())
	w.closed.Set(closedTrue)

	defer func(w2 *Worker) {
		if err != nil { // when err != nil, `w` will become nil in this func, so we pass `w` in defer.
			// release resources, NOTE: we need to refactor New/Init/Start/Close for components later.
			w2.cancel()
			w2.subTaskHolder.closeAllSubTasks()
		}
	}(w)

	// initial relay holder
	purger, err := w.relayHolder.Init([]purger.PurgeInterceptor{
		w,
	})
	if err != nil {
		return nil, err
	}
	w.relayPurger = purger

	// initial task status checker
	if w.cfg.Checker.CheckEnable {
		tsc := NewTaskStatusChecker(w.cfg.Checker, w)
		err = tsc.Init()
		if err != nil {
			return nil, err
		}
		w.taskStatusChecker = tsc
	}

	InitConditionHub(w)

	w.l.Info("initialized", zap.Reflect("cfg", cfg))

	return w, nil
}

// Start starts working
func (w *Worker) Start() {

	// start relay
	w.relayHolder.Start()

	// start purger
	w.relayPurger.Start()

	// start task status checker
	if w.cfg.Checker.CheckEnable {
		w.taskStatusChecker.Start()
	}

	// start tracer
	if w.tracer.Enable() {
		w.tracer.Start()
	}

	w.wg.Add(1)
	defer w.wg.Done()

	w.l.Info("start running")

	ticker := time.NewTicker(5 * time.Second)
	w.closed.Set(closedFalse)
	defer ticker.Stop()
	for {
		select {
		case <-w.ctx.Done():
			w.l.Info("status print process exits!")
			return
		case <-ticker.C:
			w.l.Debug("runtime status", zap.String("status", w.StatusJSON("")))
		}
	}
}

// Close stops working and releases resources
func (w *Worker) Close() {
	w.Lock()
	defer w.Unlock()

	if w.closed.Get() == closedTrue {
		w.l.Warn("already closed")
		return
	}

	// cancel status output ticker and wait for return
	w.cancel()
	w.wg.Wait()

	// close all sub tasks
	w.subTaskHolder.closeAllSubTasks()

	// close relay
	w.relayHolder.Close()

	// close purger
	w.relayPurger.Close()

	// close task status checker
	if w.cfg.Checker.CheckEnable {
		w.taskStatusChecker.Close()
	}

	// close tracer
	if w.tracer.Enable() {
		w.tracer.Stop()
	}

	w.closed.Set(closedTrue)
	w.l.Info("Stop worker")
}

// StartSubTask creates a sub task an run it
func (w *Worker) StartSubTask(cfg *config.SubTaskConfig) error {
	w.Lock()
	defer w.Unlock()

	if w.closed.Get() == closedTrue {
		return terror.ErrWorkerAlreadyClosed.Generate()
	}

	if w.relayPurger.Purging() {
		return terror.ErrWorkerRelayIsPurging.Generate(cfg.Name)
	}

	if w.subTaskHolder.findSubTask(cfg.Name) != nil {
		return terror.ErrWorkerSubTaskExists.Generate(cfg.Name)
	}

	// copy some config item from dm-worker's config
	w.copyConfigFromWorker(cfg)
	cfgDecrypted, err := cfg.DecryptPassword()
	if err != nil {
		return terror.WithClass(err, terror.ClassDMWorker)
	}

	w.l.Info("started sub task", zap.Stringer("config", cfgDecrypted))
	st := NewSubTask(cfgDecrypted)
	w.subTaskHolder.recordSubTask(st)
	st.Run()
	return nil
}

// UpdateSubTask update config for a sub task
func (w *Worker) UpdateSubTask(cfg *config.SubTaskConfig) error {
	w.Lock()
	defer w.Unlock()

	if w.closed.Get() == closedTrue {
		return terror.ErrWorkerAlreadyClosed.Generate()
	}

	st := w.subTaskHolder.findSubTask(cfg.Name)
	if st == nil {
		return terror.ErrWorkerSubTaskNotFound.Generate(cfg.Name)
	}

	w.l.Info("update sub task", zap.String("task", cfg.Name))
	return st.Update(cfg)
}

// OperateSubTask stop/resume/pause  sub task
func (w *Worker) OperateSubTask(name string, op pb.TaskOp) error {
	w.Lock()
	defer w.Unlock()

	if w.closed.Get() == closedTrue {
		return terror.ErrWorkerAlreadyClosed.Generate()
	}

	st := w.subTaskHolder.findSubTask(name)
	if st == nil {
		return terror.ErrWorkerSubTaskNotFound.Generate(name)
	}

	var err error
	switch op {
	case pb.TaskOp_Stop:
		w.l.Info("stop sub task", zap.String("task", name))
		st.Close()
		w.subTaskHolder.removeSubTask(name)
	case pb.TaskOp_Pause:
		w.l.Info("pause sub task", zap.String("task", name))
		err = st.Pause()
	case pb.TaskOp_Resume:
		w.l.Info("resume sub task", zap.String("task", name))
		err = st.Resume()
	case pb.TaskOp_AutoResume:
		w.l.Info("auto_resume sub task", zap.String("task", name))
		err = st.Resume()
	default:
		err = terror.ErrWorkerUpdateTaskStage.Generatef("invalid operate %s on subtask %v", op, name)
	}

	return err
}

// QueryStatus query worker's sub tasks' status
func (w *Worker) QueryStatus(name string) []*pb.SubTaskStatus {
	w.RLock()
	defer w.RUnlock()

	if w.closed.Get() == closedTrue {
		w.l.Warn("querying status from a closed worker")
		return nil
	}

	return w.Status(name)
}

// QueryError query worker's sub tasks' error
func (w *Worker) QueryError(name string) []*pb.SubTaskError {
	w.RLock()
	defer w.RUnlock()

	if w.closed.Get() == closedTrue {
		w.l.Warn("querying error from a closed worker")
		return nil
	}

	return w.Error(name)
}

// HandleSQLs implements Handler.HandleSQLs.
func (w *Worker) HandleSQLs(ctx context.Context, req *pb.HandleSubTaskSQLsRequest) error {
	if w.closed.Get() == closedTrue {
		return terror.ErrWorkerAlreadyClosed.Generate()
	}

	st := w.subTaskHolder.findSubTask(req.Name)
	if st == nil {
		return terror.ErrWorkerSubTaskNotFound.Generate(req.Name)
	}

	return st.SetSyncerSQLOperator(ctx, req)
}

// FetchDDLInfo fetches all sub tasks' DDL info which pending to sync
func (w *Worker) FetchDDLInfo(ctx context.Context) *pb.DDLInfo {
	if w.closed.Get() == closedTrue {
		w.l.Warn("fetching DDLInfo from a closed worker")
		return nil
	}

	// sub tasks can be changed by StartSubTask / StopSubTask, so we retry doFetch at intervals
	// maybe we can refine later and just re-do when sub tasks changed really
	result := make(chan *pb.DDLInfo, 1)
	for {
		newCtx, cancel := context.WithTimeout(ctx, reFetchInterval)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.doFetchDDLInfo(newCtx, result)
			cancel() // cancel when doFetchDDLInfo returned
		}()

		<-newCtx.Done() // wait for timeout or canceled

		if ctx.Err() == context.Canceled {
			return nil // canceled from external
		}

		wg.Wait()
		if len(result) > 0 {
			return <-result
		}
	}
}

// doFetchDDLInfo does fetching DDL info for all sub tasks concurrently
func (w *Worker) doFetchDDLInfo(ctx context.Context, ch chan<- *pb.DDLInfo) {
	sts := w.subTaskHolder.getAllSubTasks()
	cases := make([]reflect.SelectCase, 0, len(sts)+1)
	for _, st := range sts {
		cases = append(cases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(st.DDLInfo), // select all sub tasks' chan
		})
	}

	cases = append(cases, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ctx.Done()), // also need select context
	})

	_, value, ok := reflect.Select(cases)
	if !ok {
		for _, st := range sts {
			// NOTE: Can you guarantee that each DDLInfo you get is different?
			if st.GetDDLInfo() == nil {
				continue
			}
			ch <- st.GetDDLInfo()
			break
		}
		return
	}

	v, ok := value.Interface().(*pb.DDLInfo)
	if !ok {
		return // should not go here
	}

	st := w.subTaskHolder.findSubTask(v.Task)
	if st != nil {
		st.SaveDDLInfo(v)
		w.l.Info("save DDLInfo into subTasks")
		ch <- v
	} else {
		w.l.Warn("can not find specified subtask", zap.String("task", v.Task))
	}
}

// RecordDDLLockInfo records the current DDL lock info which pending to sync
func (w *Worker) RecordDDLLockInfo(info *pb.DDLLockInfo) error {
	if w.closed.Get() == closedTrue {
		return terror.ErrWorkerAlreadyClosed.Generate()
	}

	st := w.subTaskHolder.findSubTask(info.Task)
	if st == nil {
		return terror.ErrWorkerSubTaskNotFound.Generatef("sub task for DDLLockInfo %+v not found", info)
	}
	return st.SaveDDLLockInfo(info)
}

// ExecuteDDL executes (or ignores) DDL (in sharding DDL lock, requested by dm-master)
func (w *Worker) ExecuteDDL(ctx context.Context, req *pb.ExecDDLRequest) error {
	if w.closed.Get() == closedTrue {
		return terror.ErrWorkerAlreadyClosed.Generate()
	}

	st := w.subTaskHolder.findSubTask(req.Task)
	if st == nil {
		return terror.ErrWorkerSubTaskNotFound.Generate(req.Task)
	}

	info := st.DDLLockInfo()
	if info == nil || info.ID != req.LockID {
		return terror.ErrWorkerDDLLockInfoNotFound.Generate(req.LockID)
	}

	err := st.ExecuteDDL(ctx, req)
	if err == nil {
		st.ClearDDLLockInfo() // remove DDL lock info
		st.ClearDDLInfo()
		w.l.Info("ExecuteDDL remove cacheDDLInfo")
	}
	return err
}

// BreakDDLLock breaks current blocking DDL lock and/or remove current DDLLockInfo
func (w *Worker) BreakDDLLock(ctx context.Context, req *pb.BreakDDLLockRequest) error {
	if w.closed.Get() == closedTrue {
		return terror.ErrWorkerAlreadyClosed.Generate()
	}

	st := w.subTaskHolder.findSubTask(req.Task)
	if st == nil {
		return terror.ErrWorkerSubTaskNotFound.Generate(req.Task)
	}

	if len(req.RemoveLockID) > 0 {
		info := st.DDLLockInfo()
		if info == nil || info.ID != req.RemoveLockID {
			return terror.ErrWorkerDDLLockInfoNotFound.Generate(req.RemoveLockID)
		}
		st.ClearDDLLockInfo() // remove DDL lock info
		st.ClearDDLInfo()
		w.l.Info("BreakDDLLock remove cacheDDLInfo")
	}

	if req.ExecDDL && req.SkipDDL {
		return terror.ErrWorkerExecSkipDDLConflict.Generate()
	}

	if !req.ExecDDL && !req.SkipDDL {
		return nil // not need to execute or skip
	}

	execReq := &pb.ExecDDLRequest{
		Task:   req.Task,
		LockID: req.RemoveLockID, // force to operate, even if lockID mismatch
		Exec:   false,
	}
	if req.ExecDDL {
		execReq.Exec = true
	}

	return st.ExecuteDDL(ctx, execReq)
}

// SwitchRelayMaster switches relay unit's master server
func (w *Worker) SwitchRelayMaster(ctx context.Context, req *pb.SwitchRelayMasterRequest) error {
	if w.closed.Get() == closedTrue {
		return terror.ErrWorkerAlreadyClosed.Generate()
	}

	return w.relayHolder.SwitchMaster(ctx, req)
}

// OperateRelay operates relay unit
func (w *Worker) OperateRelay(ctx context.Context, req *pb.OperateRelayRequest) error {
	if w.closed.Get() == closedTrue {
		return terror.ErrWorkerAlreadyClosed.Generate()
	}

	return w.relayHolder.Operate(ctx, req)
}

// PurgeRelay purges relay log files
func (w *Worker) PurgeRelay(ctx context.Context, req *pb.PurgeRelayRequest) error {
	if w.closed.Get() == closedTrue {
		return terror.ErrWorkerAlreadyClosed.Generate()
	}

	return w.relayPurger.Do(ctx, req)
}

// ForbidPurge implements PurgeInterceptor.ForbidPurge
func (w *Worker) ForbidPurge() (bool, string) {
	if w.closed.Get() == closedTrue {
		return false, ""
	}

	// forbid purging if some sub tasks are paused, so we can debug the system easily
	// This function is not protected by `w.RWMutex`, which may lead to sub tasks information
	// not up to date, but do not affect correctness.
	for _, st := range w.subTaskHolder.getAllSubTasks() {
		stage := st.Stage()
		if stage == pb.Stage_New || stage == pb.Stage_Paused {
			return true, fmt.Sprintf("sub task %s current stage is %s", st.cfg.Name, stage.String())
		}
	}
	return false, ""
}

// QueryConfig returns worker's config
func (w *Worker) QueryConfig(ctx context.Context) (*config.MysqlConfig, error) {
	w.RLock()
	defer w.RUnlock()

	if w.closed.Get() == closedTrue {
		return nil, terror.ErrWorkerAlreadyClosed.Generate()
	}

	return w.cfg.Clone(), nil
}

// UpdateRelayConfig update subTask ans relay unit configure online
func (w *Worker) UpdateRelayConfig(ctx context.Context, content string) error {
	w.Lock()
	defer w.Unlock()

	if w.closed.Get() == closedTrue {
		return terror.ErrWorkerAlreadyClosed.Generate()
	}

	stage := w.relayHolder.Stage()
	if stage == pb.Stage_Finished || stage == pb.Stage_Stopped {
		return terror.ErrWorkerRelayUnitStage.Generate(stage.String())
	}

	sts := w.subTaskHolder.getAllSubTasks()

	// Check whether subtask is running syncer unit
	for _, st := range sts {
		isRunning := st.CheckUnit()
		if !isRunning {
			return terror.ErrWorkerNoSyncerRunning.Generate()
		}
	}

	// No need to store config in local
	newCfg := &config.MysqlConfig{}

	err := newCfg.Parse(content)
	if err != nil {
		return err
	}

	if newCfg.SourceID != w.cfg.SourceID {
		return terror.ErrWorkerCannotUpdateSourceID.Generate()
	}

	w.l.Info("update relay config", zap.Stringer("new config", newCfg))
	cloneCfg, _ := newCfg.DecryptPassword()

	// Update SubTask configure
	// NOTE: we only update `DB.Config` in SubTaskConfig now
	for _, st := range sts {
		cfg := config.NewSubTaskConfig()

		cfg.From = cloneCfg.From
		cfg.From.Adjust()

		stage := st.Stage()
		if stage == pb.Stage_Paused {
			err = st.UpdateFromConfig(cfg)
			if err != nil {
				return err
			}
		} else if stage == pb.Stage_Running {
			err = st.Pause()
			if err != nil {
				return err
			}

			err = st.UpdateFromConfig(cfg)
			if err != nil {
				return err
			}

			err = st.Resume()
			if err != nil {
				return err
			}
		}
	}

	w.l.Info("update relay config of subtasks successfully.")

	// Update relay unit configure
	err = w.relayHolder.Update(ctx, cloneCfg)
	if err != nil {
		return err
	}

	w.cfg.From = newCfg.From
	w.cfg.AutoFixGTID = newCfg.AutoFixGTID
	w.cfg.Charset = newCfg.Charset

	if w.configFile == "" {
		w.configFile = "dm-worker.toml"
	}
	content, err = w.cfg.Toml()
	if err != nil {
		return err
	}

	w.l.Info("update relay config successfully, save config to local file", zap.String("local file", w.configFile))

	return nil
}

// MigrateRelay migrate relay unit
func (w *Worker) MigrateRelay(ctx context.Context, binlogName string, binlogPos uint32) error {
	w.Lock()
	defer w.Unlock()
	stage := w.relayHolder.Stage()
	if stage == pb.Stage_Running {
		err := w.relayHolder.Operate(ctx, &pb.OperateRelayRequest{Op: pb.RelayOp_PauseRelay})
		if err != nil {
			return err
		}
	} else if stage == pb.Stage_Stopped {
		return terror.ErrWorkerMigrateStopRelay.Generate()
	}
	err := w.relayHolder.Migrate(ctx, binlogName, binlogPos)
	if err != nil {
		return err
	}
	return nil
}

// copyConfigFromWorker copies config items from dm-worker to sub task
func (w *Worker) copyConfigFromWorker(cfg *config.SubTaskConfig) {
	cfg.From = w.cfg.From

	cfg.Flavor = w.cfg.Flavor
	cfg.ServerID = w.cfg.ServerID
	cfg.RelayDir = w.cfg.RelayDir
	cfg.EnableGTID = w.cfg.EnableGTID

	// we can remove this from SubTaskConfig later, because syncer will always read from relay
	cfg.AutoFixGTID = w.cfg.AutoFixGTID

	// log config items, mydumper unit use it

}

// getAllSubTaskStatus returns all subtask status of this worker, note the field
// in subtask status is not completed, only includes `Name`, `Stage` and `Result` now
func (w *Worker) getAllSubTaskStatus() map[string]*pb.SubTaskStatus {
	sts := w.subTaskHolder.getAllSubTasks()
	result := make(map[string]*pb.SubTaskStatus, len(sts))
	for name, st := range sts {
		st.RLock()
		result[name] = &pb.SubTaskStatus{
			Name:   name,
			Stage:  st.stage,
			Result: proto.Clone(st.result).(*pb.ProcessResult),
		}
		st.RUnlock()
	}
	return result
}
