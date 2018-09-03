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
	"github.com/pingcap/tidb-enterprise-tools/dm/unit"
	"github.com/pingcap/tidb-enterprise-tools/pkg/utils"
	"github.com/pingcap/tidb-enterprise-tools/relay"
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
	relay    unit.Unit
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

	// try decrypt password for To DB
	pswdTo, err := utils.Decrypt(cfg.To.Password)
	if err != nil {
		return errors.Trace(err)
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
