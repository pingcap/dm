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
	"path"
	"reflect"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/failpoint"
	"github.com/siddontang/go/sync2"
	"github.com/syndtr/goleveldb/leveldb"
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
	sync.RWMutex
	wg     sync.WaitGroup
	closed sync2.AtomicInt32

	// context created when Worker created, and canceled when closing
	ctx    context.Context
	cancel context.CancelFunc

	cfg *Config
	l   log.Logger

	subTasks map[string]*SubTask

	relayHolder RelayHolder
	relayPurger purger.Purger

	meta   *Metadata
	db     *leveldb.DB
	tracer *tracing.Tracer

	taskStatusChecker TaskStatusChecker
}

// NewWorker creates a new Worker
func NewWorker(cfg *Config) (*Worker, error) {
	w := &Worker{
		cfg:         cfg,
		relayHolder: NewRelayHolder(cfg),
		tracer:      tracing.InitTracerHub(cfg.Tracer),
		subTasks:    make(map[string]*SubTask),
		l:           log.With(zap.String("component", "worker controller")),
	}

	// initial relay holder
	purger, err := w.relayHolder.Init([]purger.PurgeInterceptor{
		w,
	})
	if err != nil {
		return nil, err
	}
	w.relayPurger = purger

	// initial task status checker
	tsc := NewTaskStatusChecker(w.cfg.Checker, w)
	err = tsc.Init()
	if err != nil {
		return nil, err
	}
	w.taskStatusChecker = tsc

	// try upgrade from an older version
	dbDir := path.Join(w.cfg.MetaDir, "kv")
	err = tryUpgrade(dbDir)
	if err != nil {
		return nil, terror.Annotatef(err, "try to upgrade from any older version to %s", currentWorkerVersion)
	}

	// open kv db
	w.db, err = openDB(dbDir, defaultKVConfig)
	if err != nil {
		return nil, err
	}

	// initial metadata
	w.meta, err = NewMetadata(dbDir, w.db)
	if err != nil {
		return nil, err
	}

	InitConditionHub(w)

	err = w.restoreSubTask()
	if err != nil {
		return nil, err
	}

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

	w.ctx, w.cancel = context.WithCancel(context.Background())

	w.l.Info("initialzed")

	return w, nil
}

// Start starts working
func (w *Worker) Start() {
	if w.closed.Get() == closedTrue {
		w.l.Warn("already closed")
		return
	}

	w.wg.Add(2)
	defer w.wg.Done()

	go func() {
		defer w.wg.Done()
		w.handleTask()
	}()

	w.l.Info("start running")

	ticker := time.NewTicker(5 * time.Second)
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
	for _, st := range w.subTasks {
		st.Close()
	}

	w.subTasks = nil

	// close relay
	w.relayHolder.Close()

	// close purger
	w.relayPurger.Close()

	// clase task status checker
	if w.cfg.Checker.CheckEnable {
		w.taskStatusChecker.Close()
	}

	// close meta
	w.meta.Close()

	// close kv db
	if w.db != nil {
		w.db.Close()
	}

	// close tracer
	if w.tracer.Enable() {
		w.tracer.Stop()
	}

	w.closed.Set(closedTrue)
}

// StartSubTask creates a sub task an run it
func (w *Worker) StartSubTask(cfg *config.SubTaskConfig) (int64, error) {
	w.Lock()
	defer w.Unlock()

	// copy some config item from dm-worker's config
	w.copyConfigFromWorker(cfg)
	cfgStr, err := cfg.Toml()
	if err != nil {
		return 0, terror.Annotatef(err, "encode subtask %+v into toml format", cfg)
	}

	opLogID, err := w.operateSubTask(&pb.TaskMeta{
		Op:   pb.TaskOp_Start,
		Name: cfg.Name,
		Task: append([]byte{}, cfgStr...),
	})
	if err != nil {
		return 0, err
	}

	return opLogID, nil
}

// UpdateSubTask update config for a sub task
func (w *Worker) UpdateSubTask(cfg *config.SubTaskConfig) (int64, error) {
	w.Lock()
	defer w.Unlock()

	cfgStr, err := cfg.Toml()
	if err != nil {
		return 0, terror.Annotatef(err, "encode subtask %+v into toml format", cfg)
	}

	opLogID, err := w.operateSubTask(&pb.TaskMeta{
		Op:   pb.TaskOp_Update,
		Name: cfg.Name,
		Task: append([]byte{}, cfgStr...),
	})
	if err != nil {
		return 0, err
	}

	return opLogID, nil
}

// OperateSubTask stop/resume/pause  sub task
func (w *Worker) OperateSubTask(name string, op pb.TaskOp) (int64, error) {
	w.Lock()
	defer w.Unlock()

	opLogID, err := w.operateSubTask(&pb.TaskMeta{
		Name: name,
		Op:   op,
	})
	if err != nil {
		return 0, err
	}

	return opLogID, nil
}

// not thread safe
func (w *Worker) operateSubTask(task *pb.TaskMeta) (int64, error) {
	if w.closed.Get() == closedTrue {
		return 0, terror.ErrWorkerAlreadyClosed.Generate()
	}

	opLogID, err := w.meta.AppendOperation(task)
	if err != nil {
		return 0, terror.Annotatef(err, "%s task %s, something wrong with saving operation log", task.Op, task.Name)
	}

	w.l.Info("operate subtask", zap.Stringer("operation", task.Op), zap.String("task", task.Name))
	return opLogID, nil
}

// QueryStatus query worker's sub tasks' status
func (w *Worker) QueryStatus(name string) []*pb.SubTaskStatus {
	if w.closed.Get() == closedTrue {
		w.l.Warn("querying status from a closed worker")
		return nil
	}

	return w.Status(name)
}

// QueryError query worker's sub tasks' error
func (w *Worker) QueryError(name string) []*pb.SubTaskError {
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

	st := w.findSubTask(req.Name)
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
	w.RLock()
	cases := make([]reflect.SelectCase, 0, len(w.subTasks)+1)
	for _, st := range w.subTasks {
		cases = append(cases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(st.DDLInfo), // select all sub tasks' chan
		})
	}
	w.RUnlock()

	cases = append(cases, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ctx.Done()), // also need select context
	})

	_, value, ok := reflect.Select(cases)
	if !ok {
		w.RLock()
		for _, st := range w.subTasks {
			// NOTE: Can you guarantee that each DDLInfo you get is different?
			if st.GetDDLInfo() == nil {
				continue
			}
			ch <- st.GetDDLInfo()
			break
		}
		w.RUnlock()
		return
	}

	v, ok := value.Interface().(*pb.DDLInfo)
	if !ok {
		return // should not go here
	}

	w.RLock()
	w.subTasks[v.Task].SaveDDLInfo(v)
	w.RUnlock()
	w.l.Info("save DDLInfo into subTasks")

	ch <- v
}

// RecordDDLLockInfo records the current DDL lock info which pending to sync
func (w *Worker) RecordDDLLockInfo(info *pb.DDLLockInfo) error {
	if w.closed.Get() == closedTrue {
		return terror.ErrWorkerAlreadyClosed.Generate()
	}

	st := w.findSubTask(info.Task)
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

	st := w.findSubTask(req.Task)
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

	st := w.findSubTask(req.Task)
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

	w.RLock()
	defer w.RUnlock()

	// forbid purging if some sub tasks are paused
	// so we can debug the system easily
	for _, st := range w.subTasks {
		stage := st.Stage()
		if stage == pb.Stage_New || stage == pb.Stage_Paused {
			return true, fmt.Sprintf("sub task %s current stage is %s", st.cfg.Name, stage.String())
		}
	}
	return false, ""
}

// QueryConfig returns worker's config
func (w *Worker) QueryConfig(ctx context.Context) (*Config, error) {
	if w.closed.Get() == closedTrue {
		return nil, terror.ErrWorkerAlreadyClosed.Generate()
	}
	w.RLock()
	defer w.RUnlock()

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

	// Check whether subtask is running syncer unit
	for _, st := range w.subTasks {
		isRunning := st.CheckUnit()
		if !isRunning {
			return terror.ErrWorkerNoSyncerRunning.Generate()
		}
	}

	// Save configure to local file.
	newCfg := NewConfig()
	err := newCfg.UpdateConfigFile(content)
	if err != nil {
		return err
	}

	err = newCfg.Reload()
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
	for _, st := range w.subTasks {
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

	if w.cfg.ConfigFile == "" {
		w.cfg.ConfigFile = "dm-worker.toml"
	}
	content, err = w.cfg.Toml()
	if err != nil {
		return err
	}
	w.cfg.UpdateConfigFile(content)

	w.l.Info("update relay config successfully, save config to local file", zap.String("local file", w.cfg.ConfigFile))

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
	cfg.LogLevel = w.cfg.LogLevel
	cfg.LogFile = w.cfg.LogFile
}

// findSubTask finds sub task by name
func (w *Worker) findSubTask(name string) *SubTask {
	w.RLock()
	defer w.RUnlock()

	return w.subTasks[name]
}

func (w *Worker) getPausedSubTasks() []*pb.TaskStatus {
	w.RLock()
	defer w.RUnlock()
	result := make([]*pb.TaskStatus, 0)
	for name, st := range w.subTasks {
		st.RLock()
		if st.stage == pb.Stage_Paused {
			result = append(result, &pb.TaskStatus{
				Name:   name,
				Stage:  pb.Stage_Paused,
				Result: proto.Clone(st.result).(*pb.ProcessResult),
			})
		}
		st.RUnlock()
	}
	return result
}

func (w *Worker) restoreSubTask() error {
	tasks := w.meta.LoadTaskMeta()

	w.Lock()
	defer w.Unlock()
	w.subTasks = make(map[string]*SubTask)
	for name, task := range tasks {
		taskCfg := new(config.SubTaskConfig)
		if err := taskCfg.Decode(string(task.Task)); err != nil {
			return terror.Annotatef(err, "decode subtask config %s error in restoreSubTask", task.Task)
		}

		cfgDecrypted, err := taskCfg.DecryptPassword()
		if err != nil {
			return err
		}

		w.l.Info("prepare to restore sub task", zap.Stringer("config", cfgDecrypted))

		var st *SubTask
		if task.GetStage() == pb.Stage_Running || task.GetStage() == pb.Stage_New {
			st = NewSubTaskWithStage(cfgDecrypted, pb.Stage_New)
			st.Run()
		} else {
			st = NewSubTaskWithStage(cfgDecrypted, task.Stage)
		}

		w.subTasks[name] = st
	}

	return nil
}

var maxRetryCount = 10

func (w *Worker) handleTask() {
	var handleTaskInterval = time.Second
	failpoint.Inject("handleTaskInternal", func(val failpoint.Value) {
		if milliseconds, ok := val.(int); ok {
			handleTaskInterval = time.Duration(milliseconds) * time.Millisecond
			w.l.Info("set handleTaskInterval", zap.String("failpoint", "handleTaskInternal"), zap.Int("value", milliseconds))
		}
	})
	ticker := time.NewTicker(handleTaskInterval)
	defer ticker.Stop()

	retryCnt := 0

Loop:
	for {
		select {
		case <-w.ctx.Done():
			w.l.Info("handle task process exits!")
			return
		case <-ticker.C:
			w.Lock()
			if w.closed.Get() == closedTrue {
				w.Unlock()
				return
			}

			opLog := w.meta.PeekLog()
			if opLog == nil {
				w.Unlock()
				continue
			}

			w.l.Info("start to execute operation", zap.Reflect("oplog", opLog))

			st, exist := w.subTasks[opLog.Task.Name]
			var err error
			switch opLog.Task.Op {
			case pb.TaskOp_Start:
				if exist {
					err = terror.ErrWorkerSubTaskExists.Generate(opLog.Task.Name)
					break
				}

				if w.relayPurger.Purging() {
					if retryCnt < maxRetryCount {
						retryCnt++
						w.l.Warn("relay log purger is purging, cannot start subtask, would try again later", zap.String("task", opLog.Task.Name))
						w.Unlock()
						continue Loop
					}

					retryCnt = 0
					err = terror.ErrWorkerRelayIsPurging.Generate(opLog.Task.Name)
					break
				}

				retryCnt = 0
				taskCfg := new(config.SubTaskConfig)
				if err1 := taskCfg.Decode(string(opLog.Task.Task)); err1 != nil {
					err = terror.Annotate(err1, "decode subtask config error in handleTask")
					break
				}

				var cfgDecrypted *config.SubTaskConfig
				cfgDecrypted, err = taskCfg.DecryptPassword()
				if err != nil {
					err = terror.WithClass(err, terror.ClassDMWorker)
					break
				}

				w.l.Info("started sub task", zap.Stringer("config", cfgDecrypted))
				st = NewSubTask(cfgDecrypted)
				w.subTasks[opLog.Task.Name] = st
				st.Run()

			case pb.TaskOp_Update:
				if !exist {
					err = terror.ErrWorkerSubTaskNotFound.Generate(opLog.Task.Name)
					break
				}

				taskCfg := new(config.SubTaskConfig)
				if err1 := taskCfg.Decode(string(opLog.Task.Task)); err1 != nil {
					err = terror.Annotate(err1, "decode subtask config error in handleTask")
					break
				}

				w.l.Info("updated sub task", zap.String("task", opLog.Task.Name), zap.Stringer("new config", taskCfg))
				err = st.Update(taskCfg)
			case pb.TaskOp_Stop:
				if !exist {
					err = terror.ErrWorkerSubTaskNotFound.Generate(opLog.Task.Name)
					break
				}

				w.l.Info("stop sub task", zap.String("task", opLog.Task.Name))
				st.Close()
				delete(w.subTasks, opLog.Task.Name)
			case pb.TaskOp_Pause:
				if !exist {
					err = terror.ErrWorkerSubTaskNotFound.Generate(opLog.Task.Name)
					break
				}

				w.l.Info("pause sub task", zap.String("task", opLog.Task.Name))
				err = st.Pause()
			case pb.TaskOp_Resume:
				if !exist {
					err = terror.ErrWorkerSubTaskNotFound.Generate(opLog.Task.Name)
					break
				}

				w.l.Info("resume sub task", zap.String("task", opLog.Task.Name))
				err = st.Resume()
			case pb.TaskOp_AutoResume:
				if !exist {
					err = terror.ErrWorkerSubTaskNotFound.Generate(opLog.Task.Name)
					break
				}

				w.l.Info("auto_resume sub task", zap.String("task", opLog.Task.Name))
				err = st.Resume()
			}

			w.l.Info("end to execute operation", zap.Int64("oplog ID", opLog.Id), log.ShortError(err))

			if err != nil {
				opLog.Message = err.Error()
			} else {
				opLog.Task.Stage = st.Stage()
				opLog.Success = true
			}

			// fill current task config
			if len(opLog.Task.Task) == 0 {
				tm := w.meta.GetTask(opLog.Task.Name)
				if tm == nil {
					w.l.Warn("task meta not found", zap.String("task", opLog.Task.Name))
				} else {
					opLog.Task.Task = append([]byte{}, tm.Task...)
				}
			}

			err = w.meta.MarkOperation(opLog)
			w.Unlock()
			if err != nil {
				w.l.Error("fail to mark subtask operation", zap.Reflect("oplog", opLog))
			}
		}
	}
}
