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

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/streamer"
	"github.com/pingcap/errors"
	"github.com/siddontang/go/sync2"
	"github.com/syndtr/goleveldb/leveldb"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
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

	subTasks map[string]*SubTask

	relayHolder *RelayHolder
	relayPurger *purger.Purger

	meta   *Metadata
	db     *leveldb.DB
	tracer *tracing.Tracer
}

// NewWorker creates a new Worker
func NewWorker(cfg *Config) (*Worker, error) {
	w := &Worker{
		cfg:         cfg,
		relayHolder: NewRelayHolder(cfg),
		tracer:      tracing.InitTracerHub(cfg.Tracer),
		subTasks:    make(map[string]*SubTask),
	}

	// initial relay purger
	operators := []purger.RelayOperator{
		w.relayHolder,
		streamer.GetReaderHub(),
	}
	interceptors := []purger.PurgeInterceptor{
		w,
	}
	w.relayPurger = purger.NewPurger(cfg.Purge, cfg.RelayDir, operators, interceptors)

	// initial relay holder
	err := w.relayHolder.Init()
	if err != nil {
		return nil, errors.Trace(err)
	}

	// open kv db
	dbDir := path.Join(w.cfg.MetaDir, "kv")
	w.db, err = openDB(dbDir, defaultKVConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// initial metadata
	w.meta, err = NewMetadata(dbDir, w.db)
	if err != nil {
		return nil, errors.Trace(err)
	}

	InitConditionHub(w)

	err = w.restoreSubTask()
	if err != nil {
		return nil, errors.Trace(err)
	}

	// start relay
	w.relayHolder.Start()

	// start purger
	w.relayPurger.Start()

	// start tracer
	if w.tracer.Enable() {
		w.tracer.Start()
	}

	w.ctx, w.cancel = context.WithCancel(context.Background())

	log.Info("[worker] initialzed")

	return w, nil
}

// Start starts working
func (w *Worker) Start() {
	if w.closed.Get() == closedTrue {
		log.Warn("worker already closed")
		return
	}

	w.wg.Add(2)
	defer w.wg.Done()

	go func() {
		defer w.wg.Done()
		w.handleTask()
	}()

	log.Info("[worker] start running")

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-w.ctx.Done():
			log.Infof("[worker] status print process exits!")
			return
		case <-ticker.C:
			log.Debugf("[worker] status \n%s", w.StatusJSON(""))
		}
	}
}

// Close stops working and releases resources
func (w *Worker) Close() {
	w.Lock()
	defer w.Unlock()

	if w.closed.Get() == closedTrue {
		log.Warn("worker already closed")
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

	// close meta
	w.meta.Close()

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
		return 0, errors.Annotatef(err, "encode subtask %+v into toml format", cfg)
	}

	opLogID, err := w.operateSubTask(&pb.TaskMeta{
		Op:   pb.TaskOp_Start,
		Name: cfg.Name,
		Task: append([]byte{}, cfgStr...),
	})
	if err != nil {
		return 0, errors.Trace(err)
	}

	return opLogID, nil
}

// UpdateSubTask update config for a sub task
func (w *Worker) UpdateSubTask(cfg *config.SubTaskConfig) (int64, error) {
	w.Lock()
	defer w.Unlock()

	cfgStr, err := cfg.Toml()
	if err != nil {
		return 0, errors.Annotatef(err, "encode subtask %+v into toml format", cfg)
	}

	opLogID, err := w.operateSubTask(&pb.TaskMeta{
		Op:   pb.TaskOp_Update,
		Name: cfg.Name,
		Task: append([]byte{}, cfgStr...),
	})
	if err != nil {
		return 0, errors.Trace(err)
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
		return 0, errors.Trace(err)
	}

	return opLogID, nil
}

// not thread safe
func (w *Worker) operateSubTask(task *pb.TaskMeta) (int64, error) {
	if w.closed.Get() == closedTrue {
		return 0, errors.NotValidf("worker already closed")
	}

	opLogID, err := w.meta.AppendOperation(task)
	if err != nil {
		return 0, errors.Annotatef(err, "%s task %s, something wrong with saving operation log", task.Op, task.Name)
	}

	log.Infof("[worker] %s task %v", task.Op, task.Name)
	return opLogID, nil
}

// QueryStatus query worker's sub tasks' status
func (w *Worker) QueryStatus(name string) []*pb.SubTaskStatus {
	if w.closed.Get() == closedTrue {
		log.Warn("[worker] querying status from a closed worker")
		return nil
	}

	return w.Status(name)
}

// QueryError query worker's sub tasks' error
func (w *Worker) QueryError(name string) []*pb.SubTaskError {
	if w.closed.Get() == closedTrue {
		log.Warn("[worker] querying error from a closed worker")
		return nil
	}

	return w.Error(name)
}

// HandleSQLs implements Handler.HandleSQLs.
func (w *Worker) HandleSQLs(ctx context.Context, req *pb.HandleSubTaskSQLsRequest) error {
	if w.closed.Get() == closedTrue {
		return errors.NotValidf("worker already closed")
	}

	st := w.findSubTask(req.Name)
	if st == nil {
		return errors.NotFoundf("sub task with name %s", req.Name)
	}

	return errors.Trace(st.SetSyncerSQLOperator(ctx, req))
}

// FetchDDLInfo fetches all sub tasks' DDL info which pending to sync
func (w *Worker) FetchDDLInfo(ctx context.Context) *pb.DDLInfo {
	if w.closed.Get() == closedTrue {
		log.Warn("[worker] fetching DDLInfo from a closed worker")
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
	log.Infof("[worker] save DDLInfo into subTasks")

	ch <- v
}

// SendBackDDLInfo sends sub tasks' DDL info back to pending
func (w *Worker) SendBackDDLInfo(ctx context.Context, info *pb.DDLInfo) bool {
	if w.closed.Get() == closedTrue {
		log.Warnf("[worker] sending DDLInfo %v back to a closed worker", info)
		return false
	}

	st := w.findSubTask(info.Task)
	if st == nil {
		return false
	}
	return st.SendBackDDLInfo(ctx, info)
}

// RecordDDLLockInfo records the current DDL lock info which pending to sync
func (w *Worker) RecordDDLLockInfo(info *pb.DDLLockInfo) error {
	if w.closed.Get() == closedTrue {
		return errors.NotValidf("worker already closed")
	}

	st := w.findSubTask(info.Task)
	if st == nil {
		return errors.NotFoundf("sub task for DDLLockInfo %+v", info)
	}
	return st.SaveDDLLockInfo(info)
}

// ExecuteDDL executes (or ignores) DDL (in sharding DDL lock, requested by dm-master)
func (w *Worker) ExecuteDDL(ctx context.Context, req *pb.ExecDDLRequest) error {
	if w.closed.Get() == closedTrue {
		return errors.NotValidf("worker already closed")
	}

	st := w.findSubTask(req.Task)
	if st == nil {
		return errors.NotFoundf("sub task %v", req.Task)
	}

	info := st.DDLLockInfo()
	if info == nil || info.ID != req.LockID {
		return errors.NotFoundf("DDLLockInfo with ID %s", req.LockID)
	}

	err := st.ExecuteDDL(ctx, req)
	if err == nil {
		st.ClearDDLLockInfo() // remove DDL lock info
		st.ClearDDLInfo()
		log.Infof("[worker] ExecuteDDL remove cacheDDLInfo")
	}
	return err
}

// BreakDDLLock breaks current blocking DDL lock and/or remove current DDLLockInfo
func (w *Worker) BreakDDLLock(ctx context.Context, req *pb.BreakDDLLockRequest) error {
	if w.closed.Get() == closedTrue {
		return errors.NotValidf("worker already closed")
	}

	st := w.findSubTask(req.Task)
	if st == nil {
		return errors.NotFoundf("sub task %v", req.Task)
	}

	if len(req.RemoveLockID) > 0 {
		info := st.DDLLockInfo()
		if info == nil || info.ID != req.RemoveLockID {
			return errors.NotFoundf("DDLLockInfo with ID %s", req.RemoveLockID)
		}
		st.ClearDDLLockInfo() // remove DDL lock info
		st.ClearDDLInfo()
		log.Infof("[worker] BreakDDLLock remove cacheDDLInfo")
	}

	if req.ExecDDL && req.SkipDDL {
		return errors.New("execDDL and skipDDL can not specify both at the same time")
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
		return errors.NotValidf("worker already closed")
	}

	return errors.Trace(w.relayHolder.SwitchMaster(ctx, req))
}

// OperateRelay operates relay unit
func (w *Worker) OperateRelay(ctx context.Context, req *pb.OperateRelayRequest) error {
	if w.closed.Get() == closedTrue {
		return errors.NotValidf("worker already closed")
	}

	return errors.Trace(w.relayHolder.Operate(ctx, req))
}

// PurgeRelay purges relay log files
func (w *Worker) PurgeRelay(ctx context.Context, req *pb.PurgeRelayRequest) error {
	if w.closed.Get() == closedTrue {
		return errors.NotValidf("worker already closed")
	}

	return errors.Trace(w.relayPurger.Do(ctx, req))
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
		return nil, errors.NotValidf("worker already closed")
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
		return errors.NotValidf("worker already closed")
	}

	stage := w.relayHolder.Stage()
	if stage == pb.Stage_Finished || stage == pb.Stage_Stopped {
		return errors.Errorf("Worker's relay log unit in invalid stage: %s", stage.String())
	}

	// Check whether subtask is running syncer unit
	for _, st := range w.subTasks {
		isRunning := st.CheckUnit()
		if !isRunning {
			return errors.New("there is a subtask does not run syncer")
		}
	}

	// Save configure to local file.
	newCfg := NewConfig()
	err := newCfg.UpdateConfigFile(content)
	if err != nil {
		return errors.Trace(err)
	}

	err = newCfg.Reload()
	if err != nil {
		return errors.Trace(err)
	}

	if newCfg.SourceID != w.cfg.SourceID {
		return errors.New("update source ID is not allowed")
	}

	log.Infof("[worker] update relay configure with config: %v", newCfg)
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
				return errors.Trace(err)
			}
		} else if stage == pb.Stage_Running {
			err = st.Pause()
			if err != nil {
				return errors.Trace(err)
			}

			err = st.UpdateFromConfig(cfg)
			if err != nil {
				return errors.Trace(err)
			}

			err = st.Resume()
			if err != nil {
				return errors.Trace(err)
			}
		}
	}

	log.Info("[worker] update relay configure in subtasks success.")

	// Update relay unit configure
	err = w.relayHolder.Update(ctx, cloneCfg)
	if err != nil {
		return errors.Trace(err)
	}

	w.cfg.From = newCfg.From
	w.cfg.AutoFixGTID = newCfg.AutoFixGTID
	w.cfg.Charset = newCfg.Charset

	if w.cfg.ConfigFile == "" {
		w.cfg.ConfigFile = "dm-worker.toml"
	}
	content, err = w.cfg.Toml()
	if err != nil {
		return errors.Trace(err)
	}
	w.cfg.UpdateConfigFile(content)

	log.Infof("[worker] save config to local file: %s", w.cfg.ConfigFile)
	log.Info("[worker] update relay configure in success.")

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
			return errors.Trace(err)
		}
	} else if stage == pb.Stage_Stopped {
		return errors.New("relay unit has stopped, can not be migrated")
	}
	err := w.relayHolder.Migrate(ctx, binlogName, binlogPos)
	if err != nil {
		return errors.Trace(err)
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

func (w *Worker) restoreSubTask() error {
	tasks := w.meta.LoadTaskMeta()

	w.Lock()
	defer w.Unlock()
	w.subTasks = make(map[string]*SubTask)
	for name, task := range tasks {
		taskCfg := new(config.SubTaskConfig)
		if err := taskCfg.Decode(string(task.Task)); err != nil {
			return errors.Annotatef(err, "decode subtask config %s error in restoreSubTask", task.Task)
		}

		cfgDecrypted, err := taskCfg.DecryptPassword()
		if err != nil {
			return errors.Trace(err)
		}

		log.Infof("[worker] start sub task with config: %v", cfgDecrypted)

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
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	retryCnt := 0

Loop:
	for {
		select {
		case <-w.ctx.Done():
			log.Infof("[worker] handle task process exits!")
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

			log.Infof("start to execute operation ID = %d detail %+v", opLog.Id, opLog)

			st, exist := w.subTasks[opLog.Task.Name]
			var err error
			switch opLog.Task.Op {
			case pb.TaskOp_Start:
				if exist {
					err = errors.AlreadyExistsf("sub task %s", opLog.Task.Name)
					break
				}

				if w.relayPurger.Purging() {
					if retryCnt < maxRetryCount {
						retryCnt++
						log.Warnf("relay log purger is purging, cannot start sub task %s, would try again later", opLog.Task.Name)
						w.Unlock()
						continue Loop
					}

					retryCnt = 0
					err = errors.Errorf("relay log purger is purging, cannot start sub task %s, please try again later", opLog.Task.Name)
					break
				}

				retryCnt = 0
				taskCfg := new(config.SubTaskConfig)
				if err1 := taskCfg.Decode(string(opLog.Task.Task)); err1 != nil {
					err = errors.Annotate(err1, "decode subtask config error in handleTask")
					break
				}

				var cfgDecrypted *config.SubTaskConfig
				cfgDecrypted, err = taskCfg.DecryptPassword()
				if err != nil {
					break
				}

				log.Infof("[worker] start sub task with config: %v", cfgDecrypted)
				st = NewSubTask(cfgDecrypted)
				w.subTasks[opLog.Task.Name] = st
				st.Run()

			case pb.TaskOp_Update:
				if !exist {
					err = errors.NotFoundf("sub task with name %s", opLog.Task.Name)
					break
				}

				taskCfg := new(config.SubTaskConfig)
				if err1 := taskCfg.Decode(string(opLog.Task.Task)); err1 != nil {
					err = errors.Annotate(err1, "decode subtask config error in handleTask")
					break
				}

				log.Infof("[worker] update sub task %s with config: %v", opLog.Task.Name, taskCfg)
				err = st.Update(taskCfg)
			case pb.TaskOp_Stop:
				if !exist {
					err = errors.NotFoundf("sub task with name %s", opLog.Task.Name)
					break
				}

				log.Infof("[worker] stop sub task %s", opLog.Task.Name)
				st.Close()
				delete(w.subTasks, opLog.Task.Name)
			case pb.TaskOp_Pause:
				if !exist {
					err = errors.NotFoundf("sub task with name %s", opLog.Task.Name)
					break
				}

				log.Infof("[worker] pause sub task %s", opLog.Task.Name)
				err = st.Pause()
			case pb.TaskOp_Resume:
				if !exist {
					err = errors.NotFoundf("sub task with name %s", opLog.Task.Name)
					break
				}

				log.Infof("[worker] resume sub task %s", opLog.Task.Name)
				err = st.Resume()
			}

			log.Infof("end to execute operation %d result %v", opLog.Id, err)

			if err != nil {
				opLog.Message = err.Error()
			} else {
				opLog.Task.Stage = st.Stage()
				opLog.Success = true
			}

			// fill current task config
			if len(opLog.Task.Task) == 0 {
				tm := w.meta.GetTask(opLog.Task.Name)
				opLog.Task.Task = append([]byte{}, tm.Task...)
			}

			err = w.meta.MarkOperation(opLog)
			w.Unlock()
			if err != nil {
				log.Errorf("fail to mark subtask %s operation %+v", opLog.Task.Name, opLog)
			}
		}
	}
}
