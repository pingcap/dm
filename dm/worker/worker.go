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
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/siddontang/go/sync2"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/etcdutil"
	"github.com/pingcap/dm/pkg/ha"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/tracing"
	"github.com/pingcap/dm/relay/purger"
)

var (
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

	cfg *config.SourceConfig
	l   log.Logger

	subTaskHolder *subTaskHolder

	relayHolder RelayHolder
	relayPurger purger.Purger

	tracer *tracing.Tracer

	taskStatusChecker TaskStatusChecker
	configFile        string

	etcdClient *clientv3.Client

	name string
}

// NewWorker creates a new Worker
func NewWorker(cfg *config.SourceConfig, etcdClient *clientv3.Client, name string) (w *Worker, err error) {
	w = &Worker{
		cfg:           cfg,
		tracer:        tracing.InitTracerHub(cfg.Tracer),
		subTaskHolder: newSubTaskHolder(),
		l:             log.With(zap.String("component", "worker controller")),
		etcdClient:    etcdClient,
		name:          name,
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

	if cfg.EnableRelay {
		// initial relay holder, the cfg's password need decrypt
		w.relayHolder = NewRelayHolder(cfg)
		purger1, err1 := w.relayHolder.Init([]purger.PurgeInterceptor{
			w,
		})
		if err1 != nil {
			return nil, err1
		}
		w.relayPurger = purger1
	}

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

	w.l.Info("initialized", zap.Stringer("cfg", cfg))

	return w, nil
}

// Start starts working
func (w *Worker) Start(startRelay bool) {

	if w.cfg.EnableRelay && startRelay {
		log.L().Info("relay is started")
		// start relay
		w.relayHolder.Start()

		// start purger
		w.relayPurger.Start()
	}

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

	w.wg.Add(1)
	go func() {
		w.runBackgroundJob(w.ctx)
		w.wg.Done()
	}()

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
	if w.closed.Get() == closedTrue {
		w.l.Warn("already closed")
		return
	}

	// cancel status output ticker and wait for return
	w.cancel()
	w.wg.Wait()

	w.Lock()
	defer w.Unlock()

	// close all sub tasks
	w.subTaskHolder.closeAllSubTasks()

	if w.relayHolder != nil {
		// close relay
		w.relayHolder.Close()
	}

	if w.relayPurger != nil {
		// close purger
		w.relayPurger.Close()
	}

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
func (w *Worker) StartSubTask(cfg *config.SubTaskConfig) {
	w.Lock()
	defer w.Unlock()

	// copy some config item from dm-worker's source config
	copyConfigFromSource(cfg, w.cfg)
	// directly put cfg into subTaskHolder
	// the unique of subtask should be assured by etcd
	st := NewSubTask(cfg, w.etcdClient)
	w.subTaskHolder.recordSubTask(st)
	if w.closed.Get() == closedTrue {
		st.fail(terror.ErrWorkerAlreadyClosed.Generate())
		return
	}

	cfg2, err := cfg.DecryptPassword()
	if err != nil {
		st.fail(errors.Annotate(err, "start sub task"))
		return
	}
	st.cfg = cfg2

	if w.relayPurger != nil && w.relayPurger.Purging() {
		st.fail(terror.ErrWorkerRelayIsPurging.Generate(cfg.Name))
		return
	}

	w.l.Info("started sub task", zap.Stringer("config", cfg2))
	st.Run()
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

func (w *Worker) resetSubtaskStage(etcdCli *clientv3.Client) (int64, error) {
	subTaskStages, subTaskCfgm, revSubTask, err := ha.GetSubTaskStageConfig(etcdCli, w.cfg.SourceID)
	if err != nil {
		return 0, err
	}
	// use sts to check which subtask has no subtaskCfg or subtaskStage now
	sts := w.subTaskHolder.getAllSubTasks()
	for name, subtaskCfg := range subTaskCfgm {
		stage, ok := subTaskStages[name]
		if ok {
			// TODO: right operation sequences may get error when we get etcdErrCompact, need to handle it later
			// For example, Expect: Running -(pause)-> Paused -(resume)-> Running
			// we get an etcd compact error at the first running. If we try to "resume" it now, we will get an error
			opType, err2 := w.operateSubTaskStage(stage, subtaskCfg)
			if err2 != nil {
				opErrCounter.WithLabelValues(w.name, opType).Inc()
				log.L().Error("fail to operate subtask stage", zap.Stringer("stage", stage),
					zap.String("task", subtaskCfg.Name), zap.Error(err2))

			}
			delete(sts, name)
		}
	}
	// remove subtasks without subtask config or subtask stage
	for name := range sts {
		err = w.OperateSubTask(name, pb.TaskOp_Stop)
		if err != nil {
			opErrCounter.WithLabelValues(w.name, pb.TaskOp_Stop.String()).Inc()
			log.L().Error("fail to stop subtask", zap.String("task", name), zap.Error(err))
		}
	}
	return revSubTask, nil
}

func (w *Worker) observeSubtaskStage(ctx context.Context, etcdCli *clientv3.Client, rev int64) error {
	var wg sync.WaitGroup

	for {
		subTaskStageCh := make(chan ha.Stage, 10)
		subTaskErrCh := make(chan error, 10)
		wg.Add(1)
		// use ctx1, cancel1 to make sure old watcher has been released
		ctx1, cancel1 := context.WithCancel(ctx)
		go func() {
			defer func() {
				close(subTaskStageCh)
				close(subTaskErrCh)
				wg.Done()
			}()
			ha.WatchSubTaskStage(ctx1, etcdCli, w.cfg.SourceID, rev+1, subTaskStageCh, subTaskErrCh)
		}()
		err := w.handleSubTaskStage(ctx1, subTaskStageCh, subTaskErrCh)
		cancel1()
		wg.Wait()

		if etcdutil.IsRetryableError(err) {
			rev = 0
			retryNum := 1
			for rev == 0 {
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(500 * time.Millisecond):
					rev, err = w.resetSubtaskStage(etcdCli)
					if err != nil {
						log.L().Error("resetSubtaskStage is failed, will retry later", zap.Error(err), zap.Int("retryNum", retryNum))
					}
				}
				retryNum++
			}
		} else {
			if err != nil {
				log.L().Error("observeSubtaskStage is failed and will quit now", zap.Error(err))
			} else {
				log.L().Info("observeSubtaskStage will quit now")
			}
			return err
		}
	}
}

func (w *Worker) handleSubTaskStage(ctx context.Context, stageCh chan ha.Stage, errCh chan error) error {
	closed := false
	for {
		select {
		case <-ctx.Done():
			closed = true
		case stage, ok := <-stageCh:
			if !ok {
				closed = true
			}
			opType, err := w.operateSubTaskStageWithoutConfig(stage)
			if err != nil {
				opErrCounter.WithLabelValues(w.name, opType).Inc()
				log.L().Error("fail to operate subtask stage", zap.Stringer("stage", stage), zap.Error(err))
				if etcdutil.IsRetryableError(err) {
					return err
				}
			}
		case err, ok := <-errCh:
			if !ok {
				closed = true
			}
			// TODO: deal with err
			log.L().Error("WatchSubTaskStage received an error", zap.Error(err))
			if etcdutil.IsRetryableError(err) {
				return err
			}
		}
		if closed {
			log.L().Info("worker is closed, handleSubTaskStage will quit now")
			return nil
		}
	}
}

// operateSubTaskStage returns TaskOp.String() additionally to record metrics
func (w *Worker) operateSubTaskStage(stage ha.Stage, subTaskCfg config.SubTaskConfig) (string, error) {
	var op pb.TaskOp
	switch {
	case stage.Expect == pb.Stage_Running:
		if st := w.subTaskHolder.findSubTask(stage.Task); st == nil {
			w.StartSubTask(&subTaskCfg)
			log.L().Info("load subtask", zap.String("sourceID", subTaskCfg.SourceID), zap.String("task", subTaskCfg.Name))
			// error is nil, opErrTypeBeforeOp will be ignored
			return opErrTypeBeforeOp, nil
		}
		op = pb.TaskOp_Resume
	case stage.Expect == pb.Stage_Paused:
		op = pb.TaskOp_Pause
	case stage.IsDeleted:
		op = pb.TaskOp_Stop
	}
	return op.String(), w.OperateSubTask(stage.Task, op)
}

// operateSubTaskStageWithoutConfig returns TaskOp additionally to record metrics
func (w *Worker) operateSubTaskStageWithoutConfig(stage ha.Stage) (string, error) {
	var subTaskCfg config.SubTaskConfig
	if stage.Expect == pb.Stage_Running {
		if st := w.subTaskHolder.findSubTask(stage.Task); st == nil {
			tsm, _, err := ha.GetSubTaskCfg(w.etcdClient, stage.Source, stage.Task, stage.Revision)
			if err != nil {
				// TODO: need retry
				return opErrTypeBeforeOp, terror.Annotate(err, "fail to get subtask config from etcd")
			}
			var ok bool
			if subTaskCfg, ok = tsm[stage.Task]; !ok {
				return opErrTypeBeforeOp, terror.ErrWorkerFailToGetSubtaskConfigFromEtcd.Generate(stage.Task)
			}
		}
	}
	return w.operateSubTaskStage(stage, subTaskCfg)
}

func (w *Worker) observeRelayStage(ctx context.Context, etcdCli *clientv3.Client, rev int64) error {
	var wg sync.WaitGroup
	for {
		relayStageCh := make(chan ha.Stage, 10)
		relayErrCh := make(chan error, 10)
		wg.Add(1)
		// use ctx1, cancel1 to make sure old watcher has been released
		ctx1, cancel1 := context.WithCancel(ctx)
		go func() {
			defer func() {
				close(relayStageCh)
				close(relayErrCh)
				wg.Done()
			}()
			ha.WatchRelayStage(ctx1, etcdCli, w.cfg.SourceID, rev+1, relayStageCh, relayErrCh)
		}()
		err := w.handleRelayStage(ctx1, relayStageCh, relayErrCh)
		cancel1()
		wg.Wait()

		if etcdutil.IsRetryableError(err) {
			rev = 0
			retryNum := 1
			for rev == 0 {
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(500 * time.Millisecond):
					stage, rev1, err1 := ha.GetRelayStage(etcdCli, w.cfg.SourceID)
					if err1 != nil {
						log.L().Error("get source bound from etcd failed, will retry later", zap.Error(err1), zap.Int("retryNum", retryNum))
						break
					}
					rev = rev1
					if stage.IsEmpty() {
						stage.IsDeleted = true
					}
					opType, err1 := w.operateRelayStage(ctx, stage)
					if err1 != nil {
						opErrCounter.WithLabelValues(w.name, opType).Inc()
						log.L().Error("fail to operate relay", zap.Stringer("stage", stage), zap.Error(err1))
					}
				}
				retryNum++
			}
		} else {
			if err != nil {
				log.L().Error("observeRelayStage is failed and will quit now", zap.Error(err))
			} else {
				log.L().Info("observeRelayStage will quit now")
			}
			return err
		}
	}
}

func (w *Worker) handleRelayStage(ctx context.Context, stageCh chan ha.Stage, errCh chan error) error {
OUTER:
	for {
		select {
		case <-ctx.Done():
			break OUTER
		case stage, ok := <-stageCh:
			if !ok {
				break OUTER
			}
			opType, err := w.operateRelayStage(ctx, stage)
			if err != nil {
				opErrCounter.WithLabelValues(w.name, opType).Inc()
				log.L().Error("fail to operate relay", zap.Stringer("stage", stage), zap.Error(err))
			}
		case err, ok := <-errCh:
			if !ok {
				break OUTER
			}
			log.L().Error("WatchRelayStage received an error", zap.Error(err))
			if etcdutil.IsRetryableError(err) {
				return err
			}
		}
	}
	log.L().Info("worker is closed, handleRelayStage will quit now")
	return nil
}

// operateRelayStage returns RelayOp.String() additionally to record metrics
// *RelayOp is nil only when error is nil, so record on error will not meet nil-pointer deference
func (w *Worker) operateRelayStage(ctx context.Context, stage ha.Stage) (string, error) {
	var op pb.RelayOp
	switch {
	case stage.Expect == pb.Stage_Running:
		if w.relayHolder.Stage() == pb.Stage_New {
			w.relayHolder.Start()
			w.relayPurger.Start()
			return opErrTypeBeforeOp, nil
		}
		op = pb.RelayOp_ResumeRelay
	case stage.Expect == pb.Stage_Paused:
		op = pb.RelayOp_PauseRelay
	case stage.IsDeleted:
		op = pb.RelayOp_StopRelay
	}
	return op.String(), w.OperateRelay(ctx, &pb.OperateRelayRequest{Op: op})
}

// SwitchRelayMaster switches relay unit's master server
func (w *Worker) SwitchRelayMaster(ctx context.Context, req *pb.SwitchRelayMasterRequest) error {
	if w.closed.Get() == closedTrue {
		return terror.ErrWorkerAlreadyClosed.Generate()
	}

	if w.relayHolder != nil {
		return w.relayHolder.SwitchMaster(ctx, req)
	}

	w.l.Warn("enable-relay is false, ignore switch relay master")
	return nil
}

// OperateRelay operates relay unit
func (w *Worker) OperateRelay(ctx context.Context, req *pb.OperateRelayRequest) error {
	if w.closed.Get() == closedTrue {
		return terror.ErrWorkerAlreadyClosed.Generate()
	}

	if w.relayHolder != nil {
		return w.relayHolder.Operate(ctx, req)
	}

	w.l.Warn("enable-relay is false, ignore operate relay")
	return nil
}

// PurgeRelay purges relay log files
func (w *Worker) PurgeRelay(ctx context.Context, req *pb.PurgeRelayRequest) error {
	if w.closed.Get() == closedTrue {
		return terror.ErrWorkerAlreadyClosed.Generate()
	}

	if w.relayPurger != nil {
		return w.relayPurger.Do(ctx, req)
	}

	w.l.Warn("enable-relay is false, ignore purge relay")
	return nil
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
func (w *Worker) QueryConfig(ctx context.Context) (*config.SourceConfig, error) {
	w.RLock()
	defer w.RUnlock()

	if w.closed.Get() == closedTrue {
		return nil, terror.ErrWorkerAlreadyClosed.Generate()
	}

	return w.cfg.Clone(), nil
}

// UpdateRelayConfig update subTask ans relay unit configure online
// TODO: update the function name like `UpdateConfig`, and update the cmd in dmctl from `update-relay` to `update-worker-config`
func (w *Worker) UpdateRelayConfig(ctx context.Context, content string) error {
	w.Lock()
	defer w.Unlock()

	if w.closed.Get() == closedTrue {
		return terror.ErrWorkerAlreadyClosed.Generate()
	}

	if w.relayHolder != nil {
		stage := w.relayHolder.Stage()
		if stage == pb.Stage_Finished || stage == pb.Stage_Stopped {
			return terror.ErrWorkerRelayUnitStage.Generate(stage.String())
		}
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
	newCfg := config.NewSourceConfig()

	err := newCfg.ParseYaml(content)
	if err != nil {
		return err
	}

	if newCfg.SourceID != w.cfg.SourceID {
		return terror.ErrWorkerCannotUpdateSourceID.Generate()
	}

	w.l.Info("update config", zap.Stringer("new config", newCfg))
	cloneCfg := newCfg.DecryptPassword()

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

	w.l.Info("update config of subtasks successfully.")

	// Update relay unit configure
	if w.relayHolder != nil {
		err = w.relayHolder.Update(ctx, cloneCfg)
		if err != nil {
			return err
		}
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

	w.l.Info("update config successfully, save config to local file", zap.String("local file", w.configFile))

	return nil
}

// MigrateRelay migrate relay unit
func (w *Worker) MigrateRelay(ctx context.Context, binlogName string, binlogPos uint32) error {
	w.Lock()
	defer w.Unlock()

	if w.relayHolder == nil {
		w.l.Warn("relay holder is nil, ignore migrate relay")
		return nil
	}

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

// OperateSchema operates schema for an upstream table.
func (w *Worker) OperateSchema(ctx context.Context, req *pb.OperateWorkerSchemaRequest) (schema string, err error) {
	w.Lock()
	defer w.Unlock()

	if w.closed.Get() == closedTrue {
		return "", terror.ErrWorkerAlreadyClosed.Generate()
	}

	st := w.subTaskHolder.findSubTask(req.Task)
	if st == nil {
		return "", terror.ErrWorkerSubTaskNotFound.Generate(req.Task)
	}

	return st.OperateSchema(ctx, req)
}

// copyConfigFromSource copies config items from source config to sub task
func copyConfigFromSource(cfg *config.SubTaskConfig, sourceCfg *config.SourceConfig) {
	cfg.From = sourceCfg.From

	cfg.Flavor = sourceCfg.Flavor
	cfg.ServerID = sourceCfg.ServerID
	cfg.RelayDir = sourceCfg.RelayDir
	cfg.EnableGTID = sourceCfg.EnableGTID
	cfg.UseRelay = sourceCfg.EnableRelay

	// we can remove this from SubTaskConfig later, because syncer will always read from relay
	cfg.AutoFixGTID = sourceCfg.AutoFixGTID
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

// HandleError handle worker error
func (w *Worker) HandleError(ctx context.Context, req *pb.HandleWorkerErrorRequest) error {
	w.Lock()
	defer w.Unlock()

	if w.closed.Get() == closedTrue {
		return terror.ErrWorkerAlreadyClosed.Generate()
	}

	st := w.subTaskHolder.findSubTask(req.Task)
	if st == nil {
		return terror.ErrWorkerSubTaskNotFound.Generate(req.Task)
	}

	return st.HandleError(ctx, req)
}
