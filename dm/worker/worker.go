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

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/errors"
	"github.com/siddontang/go/sync2"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/streamer"
	"github.com/pingcap/dm/pkg/utils"
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

	cfg         *Config
	subTasks    map[string]*SubTask
	relayHolder *RelayHolder
	relayPurger *purger.Purger
}

// NewWorker creates a new Worker
func NewWorker(cfg *Config) *Worker {
	w := Worker{
		cfg:         cfg,
		subTasks:    make(map[string]*SubTask),
		relayHolder: NewRelayHolder(cfg),
	}

	operators := []purger.RelayOperator{
		w.relayHolder,
		streamer.GetReaderHub(),
	}
	interceptors := []purger.PurgeInterceptor{
		&w,
	}
	w.relayPurger = purger.NewPurger(cfg.Purge, cfg.RelayDir, operators, interceptors)

	w.closed.Set(closedTrue) // not start yet
	w.ctx, w.cancel = context.WithCancel(context.Background())
	return &w
}

// Init initializes the worker
func (w *Worker) Init() error {
	err := w.relayHolder.Init()
	if err != nil {
		return errors.Trace(err)
	}

	InitConditionHub(w)

	return nil
}

// Start starts working
func (w *Worker) Start() {
	w.closed.Set(closedFalse)
	w.wg.Add(1)
	defer w.wg.Done()

	log.Info("[worker] start running")

	// start relay
	w.relayHolder.Start()

	// start purger
	w.relayPurger.Start()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			log.Debugf("[worker] status \n%s", w.StatusJSON(""))
		}
	}
}

// Close stops working and releases resources
func (w *Worker) Close() {
	if !w.closed.CompareAndSwap(closedFalse, closedTrue) {
		return
	}

	w.Lock()
	// close all sub tasks
	for name, st := range w.subTasks {
		st.Close()
		delete(w.subTasks, name)
	}
	w.Unlock()

	// close relay
	w.relayHolder.Close()

	// close purger
	w.relayPurger.Close()

	// cancel status output ticker and wait for return
	w.cancel()
	w.wg.Wait()
}

// StartSubTask creates a sub task an run it
func (w *Worker) StartSubTask(cfg *config.SubTaskConfig) error {
	if w.closed.Get() == closedTrue {
		return errors.NotValidf("worker already closed")
	}

	w.Lock()
	defer w.Unlock()

	if w.relayPurger.Purging() {
		return errors.Errorf("relay log purger is purging, cannot start sub task %s, please try again later", cfg.Name)
	}

	_, ok := w.subTasks[cfg.Name]
	if ok {
		return errors.Errorf("sub task with name %v already started", cfg.Name)
	}

	// copy some config item from dm-worker's config
	w.copyConfigFromWorker(cfg)

	log.Infof("[worker] starting sub task with config: %v", cfg)

	// try decrypt password for To DB
	var (
		pswdTo string
		err    error
	)
	if len(cfg.To.Password) > 0 {
		pswdTo, err = utils.Decrypt(cfg.To.Password)
		if err != nil {
			return errors.Trace(err)
		}
	}
	cfg.To.Password = pswdTo

	st := NewSubTask(cfg)
	err = st.Init()
	if err != nil {
		return errors.Trace(err)
	}

	w.subTasks[cfg.Name] = st

	st.Run()
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

// StopSubTask stops a running sub task
func (w *Worker) StopSubTask(name string) error {
	if w.closed.Get() == closedTrue {
		return errors.NotValidf("worker already closed")
	}

	w.Lock()
	defer w.Unlock()
	st, ok := w.subTasks[name]
	if !ok {
		return errors.NotFoundf("sub task with name %s", name)
	}

	st.Close()
	delete(w.subTasks, name)
	return nil
}

// PauseSubTask pauses a running sub task
func (w *Worker) PauseSubTask(name string) error {
	if w.closed.Get() == closedTrue {
		return errors.NotValidf("worker already closed")
	}

	st := w.findSubTask(name)
	if st == nil {
		return errors.NotFoundf("sub task with name %s", name)
	}

	return st.Pause()
}

// ResumeSubTask resumes a paused sub task
func (w *Worker) ResumeSubTask(name string) error {
	if w.closed.Get() == closedTrue {
		return errors.NotValidf("worker already closed")
	}

	st := w.findSubTask(name)
	if st == nil {
		return errors.NotFoundf("sub task with name %s", name)
	}

	return st.Resume()
}

// UpdateSubTask update config for a sub task
func (w *Worker) UpdateSubTask(cfg *config.SubTaskConfig) error {
	if w.closed.Get() == closedTrue {
		return errors.NotValidf("worker already closed")
	}

	st := w.findSubTask(cfg.Name)
	if st == nil {
		return errors.NotFoundf("sub task with name %s", cfg.Name)
	}

	return st.Update(cfg)
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

// findSubTask finds sub task by name
func (w *Worker) findSubTask(name string) *SubTask {
	w.RLock()
	defer w.RUnlock()
	return w.subTasks[name]
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
	w.subTasks[v.Task].SaveDDLInfo(v)
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
	if w.closed.Get() == closedTrue {
		return errors.NotValidf("worker already closed")
	}
	w.Lock()
	defer w.Unlock()

	stage := w.relayHolder.Stage()
	if stage == pb.Stage_Finished || stage == pb.Stage_Stopped {
		return errors.Errorf("Worker's relay log unit has already stoped.")
	}

	// Check whether subtask is running syncer unit
	for _, st := range w.subTasks {
		isRunning := st.CheckUnit()
		if !isRunning {
			return errors.Errorf("There is a subtask does not run syncer.")
		}
	}

	// Save configure to local file.
	newCfg := NewConfig()
	err := newCfg.UpdateConfigFile(content)
	if err != nil {
		return errors.Trace(err)
	}

	if newCfg.SourceID != w.cfg.SourceID {
		return errors.Errorf("update source ID is not allowed")
	}

	err = newCfg.Reload()
	if err != nil {
		return errors.Trace(err)
	}

	log.Infof("[worker] update relay configure with config: %v", newCfg)

	// Update SubTask configure
	for _, st := range w.subTasks {
		cfg := config.NewSubTaskConfig()

		cfg.From = newCfg.From

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
	err = w.relayHolder.Update(ctx, newCfg)
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
