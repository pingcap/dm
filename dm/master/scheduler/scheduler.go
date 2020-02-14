// Copyright 2020 PingCAP, Inc.
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

package scheduler

import (
	"context"
	"sync"

	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/ha"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
)

// Scheduler schedules tasks for DM-worker instances, including:
// - register/unregister DM-worker instances.
// - observe the online/offline status of DM-worker instances.
// - observe add/remove operations for upstream sources' config.
// - schedule upstream sources to DM-worker instances.
// - schedule data migration subtask operations.
// - holds agents of DM-worker instances.
// NOTE: the DM-master server MUST wait for this scheduler become started before handling client requests.
type Scheduler struct {
	mu sync.RWMutex

	logger log.Logger

	started bool // whether the scheduler already started for work.
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	etcdCli *clientv3.Client

	// worker name -> worker.
	workers map[string]*Worker
}

// NewScheduler creates a new scheduler instance.
func NewScheduler(pLogger *log.Logger) *Scheduler {
	return &Scheduler{
		logger:  pLogger.WithFields(zap.String("component", "scheduler")),
		workers: make(map[string]*Worker),
	}
}

// Start starts the scheduler for work.
func (s *Scheduler) Start(pCtx context.Context, etcdCli *clientv3.Client) error {
	s.logger.Info("the scheduler is starting")

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return terror.ErrSchedulerStarted.Generate()
	}

	s.reset() // reset previous status.

	rev, err := s.recoverWorkers(etcdCli)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(pCtx)

	// starting to observe status of DM-worker instances.
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.observerWorkers(ctx, rev+1)
	}()

	s.started = true // started now
	s.cancel = cancel
	s.etcdCli = etcdCli
	s.logger.Info("the scheduler has started")
	return nil
}

// Close closes the scheduler.
func (s *Scheduler) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return
	}

	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}

	s.wg.Wait()
	s.started = false // closed now.
	s.logger.Info("the scheduler has closed")
}

// AddWorker adds the information of the DM-worker when registering a new instance.
// This only adds the information of the DM-worker,
// in order to know whether it's online (ready to handle works),
// we need to wait for its healthy status through keep-alive.
func (s *Scheduler) AddWorker(name, addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return terror.ErrSchedulerNotStarted.Generate()
	}

	// check whether exists.
	if w, ok := s.workers[name]; ok {
		return terror.ErrSchedulerWorkerExist.Generate(w.baseInfo)
	}

	// put the base info into etcd.
	// TODO: try to handle the return `err` of `PutWorkerInfo` for the case:
	//   puted in etcd, but the response to the etcd client interrupted.
	//   and handle that for other etcd operations too.
	info := ha.NewWorkerInfo(name, addr)
	_, err := ha.PutWorkerInfo(s.etcdCli, info)
	if err != nil {
		return err
	}

	// generate an agent of DM-worker (with Offline stage) and keep it in the scheduler.
	w, err := NewWorker(info)
	if err != nil {
		return err
	}
	s.workers[name] = w
	return nil
}

// RemoveWorker removes the information of the DM-worker when removing the instance manually.
// The user should shutdown the DM-worker instance before removing its information.
func (s *Scheduler) RemoveWorker(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return terror.ErrSchedulerNotStarted.Generate()
	}

	w, ok := s.workers[name]
	if !ok {
		return terror.ErrSchedulerWorkerNotExist.Generate(name)
	} else if w.Stage() != WorkerOffline {
		return terror.ErrSchedulerWorkerOnline.Generate()
	}

	// delete the info in etcd.
	_, err := ha.DeleteWorkerInfo(s.etcdCli, name)
	if err != nil {
		return err
	}
	delete(s.workers, name)
	return nil
}

// observerWorkers observe the online/offline status of DM-worker instances.
func (s *Scheduler) observerWorkers(ctx context.Context, startRev int64) {

}

// recoverWorkers recovers history DM-worker info and status from etcd.
func (s *Scheduler) recoverWorkers(cli *clientv3.Client) (int64, error) {
	// 1. get all history base info.
	// it should no new DM-worker registered between this call and the below `GetKeepAliveWorkers`,
	// because no DM-master leader are handling DM-worker register requests.
	wim, _, err := ha.GetAllWorkerInfo(cli)
	if err != nil {
		return 0, err
	}

	// 2. get all history bound relationships.
	// it should no new bound relationship added between this call and the below `GetKeepAliveWorkers`,
	// because no DM-master leader are doing the scheduler.
	// TODO(csuzhangxc): handle the case whether the bound relationship exists, but the base info not exists.
	sbm, _, err := ha.GetSourceBound(cli, "")
	if err != nil {
		return 0, err
	}

	// 3. get all history offline status.
	kam, rev, err := ha.GetKeepAliveWorkers(cli)
	if err != nil {
		return 0, err
	}

	for name, info := range wim {
		// create the agent of worker with offline stage.
		w, err2 := NewWorker(info)
		if err2 != nil {
			return 0, err2
		}
		// set the stage as Free if it's keep alive.
		if _, ok := kam[name]; ok {
			w.ToFree()
		}
		// set the stage as Bound and record the bound relationship if exists.
		if bound, ok := sbm[name]; ok {
			err2 = w.ToBound(bound)
			if err2 != nil {
				return 0, err2
			}
		}
		s.workers[name] = w
	}
	return rev, nil
}

// reset resets the internal status.
func (s *Scheduler) reset() {
	s.workers = make(map[string]*Worker)
}
