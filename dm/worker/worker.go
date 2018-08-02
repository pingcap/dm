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
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-enterprise-tools/dm/config"
	"github.com/pingcap/tidb-enterprise-tools/dm/pb"
	"github.com/siddontang/go/sync2"
	"golang.org/x/net/context"
)

// Worker manages sub tasks and process units for data migration
type Worker struct {
	sync.Mutex
	wg     sync.WaitGroup
	closed sync2.AtomicBool

	// context created when Worker created, and canceled when closing
	ctx    context.Context
	cancel context.CancelFunc

	cfg      *Config
	subTasks map[string]*SubTask
}

// NewWorker creates a new Worker
func NewWorker(cfg *Config) *Worker {
	w := Worker{
		cfg:      cfg,
		subTasks: make(map[string]*SubTask),
	}
	w.closed.Set(true) // not start yet
	w.ctx, w.cancel = context.WithCancel(context.Background())
	return &w
}

// Start starts working
func (w *Worker) Start() {
	w.closed.Set(false)
	w.wg.Add(1)
	defer w.wg.Done()

	log.Info("[worker] start running")

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			log.Debugf("[worker] status \n%s", w.StatusJson(""))
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

	// use worker's cfg.From
	cfg.From = w.cfg.From
	st := NewSubTask(cfg)
	err := st.Init()
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
	w.Lock()
	defer w.Unlock()
	st, ok := w.subTasks[name]
	if !ok {
		return errors.NotFoundf("sub task with name %s", name)
	}

	return st.Pause()
}

// ResumeSubTask resumes a paused sub task
func (w *Worker) ResumeSubTask(name string) error {
	w.Lock()
	defer w.Unlock()
	st, ok := w.subTasks[name]
	if !ok {
		return errors.NotFoundf("sub task with name %s", name)
	}

	return st.Resume()
}

// QueryStatus implements Handler.QueryStatus
func (w *Worker) QueryStatus(name string) []*pb.SubTaskStatus {
	return w.Status(name)
}
