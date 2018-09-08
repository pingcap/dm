// Copyright 2018 PingCAP, Inc.
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
	"reflect"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-enterprise-tools/dm/config"
	"github.com/pingcap/tidb-enterprise-tools/dm/pb"
	"github.com/pingcap/tidb-enterprise-tools/pkg/utils"
	"github.com/pingcap/tidb-enterprise-tools/relay"
	"github.com/siddontang/go/sync2"
	"golang.org/x/net/context"
)

var (
	// sub tasks may changed, so we re-FetchDDLInfo at intervals
	reFetchInterval = 10 * time.Second
)

// Worker manages sub tasks and process units for data migration
type Worker struct {
	sync.RWMutex
	wg     sync.WaitGroup
	closed sync2.AtomicBool

	// context created when Worker created, and canceled when closing
	ctx    context.Context
	cancel context.CancelFunc

	cfg      *Config
	subTasks map[string]*SubTask
	relay    *relay.Relay
}

// NewWorker creates a new Worker
func NewWorker(cfg *Config) *Worker {
	w := Worker{
		cfg:      cfg,
		subTasks: make(map[string]*SubTask),
	}
	w.closed.Set(true) // not start yet
	w.ctx, w.cancel = context.WithCancel(context.Background())

	relayCfg := &relay.Config{
		EnableGTID:     cfg.EnableGTID,
		AutoFixGTID:    false,
		Flavor:         cfg.Flavor,
		MetaFile:       cfg.MetaFile,
		RelayDir:       cfg.RelayDir,
		ServerID:       cfg.ServerID, // TODO: use auto-generated and unique server id?
		Charset:        cfg.Charset,
		VerifyChecksum: cfg.VerifyChecksum,
		From: relay.DBConfig{
			Host:     cfg.From.Host,
			Port:     cfg.From.Port,
			User:     cfg.From.User,
			Password: cfg.From.Password,
		},
	}

	w.relay = relay.NewRelay(relayCfg)
	return &w
}

// Start starts working
func (w *Worker) Start() {
	w.closed.Set(false)
	w.wg.Add(1)
	defer w.wg.Done()

	log.Info("[worker] start running")

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		err := w.relay.Init()
		if err != nil {
			log.Errorf("relay init err %v", errors.ErrorStack(err))
			return
		}

		pr := make(chan pb.ProcessResult, 1)
		w.relay.Process(w.ctx, pr)
		w.relay.Close()

		var errOccurred bool
		for len(pr) > 0 {
			r := <-pr
			for _, err := range r.Errors {
				errOccurred = true
				log.Errorf("process error with type %v:\n %v", err.Type, err.Msg)
			}
		}
		if errOccurred {
			log.Errorf("relay exits with some errors")
		}
	}()

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
	w.Lock()
	defer w.Unlock()
	if w.closed.Get() {
		return
	}

	// close all sub tasks
	for name, st := range w.subTasks {
		st.Close()
		delete(w.subTasks, name)
	}

	if w.relay != nil {
		w.relay.Close()
	}

	// cancel status output ticker and wait for return
	w.cancel()
	w.wg.Wait()

	w.closed.Set(true)
}

// StartSubTask creates a sub task an run it
func (w *Worker) StartSubTask(cfg *config.SubTaskConfig) error {
	w.Lock()
	defer w.Unlock()

	_, ok := w.subTasks[cfg.Name]
	if ok {
		return errors.Errorf("sub task with name %v already started", cfg.Name)
	}

	// copy log configurations, mydumper unit use it
	cfg.LogLevel = w.cfg.LogLevel
	cfg.LogFile = w.cfg.LogFile
	cfg.LogRotate = w.cfg.LogRotate

	// NOTE: use worker's cfg.From, cfg.ServerID
	cfg.From = w.cfg.From
	cfg.ServerID = w.cfg.ServerID
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

// StopSubTask stops a running sub task
func (w *Worker) StopSubTask(name string) error {
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
	st := w.findSubTask(name)
	if st == nil {
		return errors.NotFoundf("sub task with name %s", name)
	}

	return st.Pause()
}

// ResumeSubTask resumes a paused sub task
func (w *Worker) ResumeSubTask(name string) error {
	st := w.findSubTask(name)
	if st == nil {
		return errors.NotFoundf("sub task with name %s", name)
	}

	return st.Resume()
}

// QueryStatus query worker's sub tasks' status
func (w *Worker) QueryStatus(name string) []*pb.SubTaskStatus {
	return w.Status(name)
}

// HandleSQLs implements Handler.HandleSQLs.
func (w *Worker) HandleSQLs(name string, op pb.SQLOp, pos string, args []string) error {
	w.Lock()
	defer w.Unlock()
	st, ok := w.subTasks[name]
	if !ok {
		return errors.NotFoundf("sub task with name %s", name)
	}

	err := st.SetSyncerOperator(op, pos, args)
	return errors.Trace(err)
}

// findSubTask finds sub task by name
func (w *Worker) findSubTask(name string) *SubTask {
	w.RLock()
	defer w.RUnlock()
	return w.subTasks[name]
}

// FetchDDLInfo fetches all sub tasks' DDL info which pending to sync
func (w *Worker) FetchDDLInfo(ctx context.Context) *pb.DDLInfo {
	if w.closed.Get() {
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
		return // canceled
	}

	v, ok := value.Interface().(*pb.DDLInfo)
	if !ok {
		return // should not go here
	}

	ch <- v
	return
}

// SendBackDDLInfo sends sub tasks' DDL info back to pending
func (w *Worker) SendBackDDLInfo(ctx context.Context, info *pb.DDLInfo) bool {
	if w.closed.Get() {
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
	st := w.findSubTask(info.Task)
	if st == nil {
		return errors.NotFoundf("sub task for DDLLockInfo %+v", info)
	}
	return st.SaveDDLLockInfo(info)
}

// ExecuteDDL executes (or ignores) DDL (in sharding DDL lock, requested by dm-master)
func (w *Worker) ExecuteDDL(ctx context.Context, req *pb.ExecDDLRequest) error {
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
	}
	return err
}

// BreakDDLLock breaks current blocking DDL lock and/or remove current DDLLockInfo
func (w *Worker) BreakDDLLock(ctx context.Context, req *pb.BreakDDLLockRequest) error {
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
