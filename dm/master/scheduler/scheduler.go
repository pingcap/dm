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

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
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

	// all source configs, source ID -> source config.
	sourceCfgs map[string]config.MysqlConfig

	// all subtask configs, task name -> source ID -> subtask config.
	subTaskCfgs map[string]map[string]config.SubTaskConfig

	// all DM-workers, worker name -> worker.
	workers map[string]*Worker

	// all bound relationship, source ID -> worker.
	bounds map[string]*Worker

	// unbound (pending to bound) sources.
	// NOTE: refactor to support scheduling by priority.
	unbounds []string

	// expectant relay stages for sources, source ID -> stage.
	expectRelayStages map[string]ha.Stage

	// expectant subtask stages for tasks & sources, task name -> source ID -> stage.
	expectSubTaskStages map[string]map[string]ha.Stage
}

// NewScheduler creates a new scheduler instance.
func NewScheduler(pLogger *log.Logger) *Scheduler {
	return &Scheduler{
		logger:              pLogger.WithFields(zap.String("component", "scheduler")),
		sourceCfgs:          make(map[string]config.MysqlConfig),
		subTaskCfgs:         make(map[string]map[string]config.SubTaskConfig),
		workers:             make(map[string]*Worker),
		bounds:              make(map[string]*Worker),
		unbounds:            make([]string, 0, 10),
		expectRelayStages:   make(map[string]ha.Stage),
		expectSubTaskStages: make(map[string]map[string]ha.Stage),
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
	workerEvCh := make(chan ha.WorkerEvent, 10)
	workerErrCh := make(chan error, 10)
	s.wg.Add(2)
	go func() {
		defer func() {
			s.wg.Done()
			close(workerEvCh)
			close(workerErrCh)
		}()
		ha.WatchWorkerEvent(ctx, etcdCli, rev+1, workerEvCh, workerErrCh)
	}()
	go func() {
		defer s.wg.Done()
		s.handleWorkerEv(ctx, workerEvCh, workerErrCh)
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

// AddSourceCfg adds the upstream source config to the cluster.
// NOTE: please verify the config before call this.
func (s *Scheduler) AddSourceCfg(cfg config.MysqlConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return terror.ErrSchedulerNotStarted.Generate()
	}

	// check whether exists.
	if cfg, ok := s.sourceCfgs[cfg.SourceID]; ok {
		return terror.ErrSchedulerSourceCfgExist.Generate(cfg.SourceID)
	}

	// put the config into etcd.
	_, err := ha.PutSourceCfg(s.etcdCli, cfg)
	if err != nil {
		return err
	}

	s.sourceCfgs[cfg.SourceID] = cfg
	s.pushToUnbound(cfg.SourceID)
	return nil
}

// RemoveSourceCfg removes the upstream source config in the cluster.
// when removing the upstream source config, it should also remove:
// - any existing relay stage.
// - any source-worker bound relationship.
func (s *Scheduler) RemoveSourceCfg(source string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return terror.ErrSchedulerNotStarted.Generate()
	}

	// check whether the config exists.
	_, ok := s.sourceCfgs[source]
	if !ok {
		return terror.ErrSchedulerSourceCfgNotExist.Generate(source)
	}

	// TODO(csuzhangxc): check whether subtask exists.

	// find worker name by source ID.
	var worker string // empty should be find below.
	if w, ok := s.bounds[source]; ok {
		worker = w.BaseInfo().Name
	}

	// delete the info in etcd.
	_, err := ha.DeleteSourceCfgRelayStageSourceBound(s.etcdCli, source, worker)
	if err != nil {
		return err
	}

	delete(s.sourceCfgs, source)
	s.unboundWorker(source)
	return nil
}

// GetSourceCfgByID gets source config by source ID.
func (s *Scheduler) GetSourceCfgByID(source string) *config.MysqlConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cfg, ok := s.sourceCfgs[source]
	if !ok {
		return nil
	}
	clone := cfg
	return &clone
}

// AddSubTasks adds the information of one or more subtasks for one task.
func (s *Scheduler) AddSubTasks(cfgs ...config.SubTaskConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return terror.ErrSchedulerNotStarted.Generate()
	}

	if len(cfgs) == 0 {
		return nil // no subtasks need to add, this should not happen.
	}

	// check whether exists.
	var (
		taskNamesM    = make(map[string]struct{}, 1)
		existSourcesM = make(map[string]struct{}, len(cfgs))
	)
	for _, cfg := range cfgs {
		taskNamesM[cfg.Name] = struct{}{}
		cfgM, ok := s.subTaskCfgs[cfg.Name]
		if !ok {
			continue
		}
		_, ok = cfgM[cfg.SourceID]
		if !ok {
			continue
		}
		existSourcesM[cfg.SourceID] = struct{}{}
	}
	taskNames := strMapToSlice(taskNamesM)
	existSources := strMapToSlice(existSourcesM)
	if len(taskNames) > 1 {
		// only subtasks from one task supported now.
		return terror.ErrSchedulerMultiTask.Generate(taskNames)
	} else if len(existSources) == len(cfgs) {
		// all subtasks already exist, return an error.
		return terror.ErrSchedulerSubTaskExist.Generate(taskNames[0], existSources)
	} else if len(existSources) > 0 {
		// some subtasks already exists, log a warn.
		s.logger.Warn("some subtasks already exist", zap.String("task", taskNames[0]), zap.Strings("sources", existSources))
	}

	// construct `Running` stages when adding.
	putStages := make([]ha.Stage, 0, len(cfgs)-len(existSources))
	for _, cfg := range cfgs {
		if _, ok := existSourcesM[cfg.SourceID]; ok {
			continue
		}
		putStages = append(putStages, ha.NewSubTaskStage(pb.Stage_Running, cfg.SourceID, cfg.Name))
	}

	// put the configs and stages into etcd.
	ha.PutSubTaskCfgStage(s.etcdCli, cfgs, putStages)
	return nil
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
	_, err = s.recordWorker(info)
	return err
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
	s.deleteWorker(name)
	return nil
}

// GetWorkerByName gets worker agent by worker name.
func (s *Scheduler) GetWorkerByName(name string) *Worker {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.workers[name]
}

// GetWorkerBySource gets the current bound worker agent by source ID,
// returns nil if the source not bound.
func (s *Scheduler) GetWorkerBySource(source string) *Worker {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.bounds[source]
}

// BoundSources returns all bound source IDs.
func (s *Scheduler) BoundSources() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	IDs := make([]string, 0, len(s.bounds))
	for ID := range s.bounds {
		IDs = append(IDs, ID)
	}
	return IDs
}

// UnboundSources returns all unbound source IDs.
func (s *Scheduler) UnboundSources() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	IDs := make([]string, 0, len(s.unbounds))
	for _, ID := range s.unbounds {
		IDs = append(IDs, ID)
	}
	return IDs
}

// UpdateExpectRelayStage updates the current expect relay stage.
// now, only support updates:
// - from `Running` to `Paused`.
// - from `Paused` to `Running`.
// NOTE: from `Running` to `Running` and `Paused` to `Paused` still update the data in etcd,
// because some user may want to update `{Running, Paused, ...}` to `{Running, Running, ...}`.
// so, this should be supported in DM-worker.
func (s *Scheduler) UpdateExpectRelayStage(newStage pb.Stage, sources ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return terror.ErrSchedulerNotStarted.Generate()
	}

	if len(sources) == 0 {
		return nil // no sources need to update the stage, this should not happen.
	}

	// check the new expectant stage.
	switch newStage {
	case pb.Stage_Running, pb.Stage_Paused:
	default:
		return terror.ErrSchedulerRelayStageInvalidUpdate.Generate(newStage)
	}

	var (
		notExistSourcesM = make(map[string]struct{})
		currStagesM      = make(map[string]struct{})
		stages           = make([]ha.Stage, 0, len(sources))
	)
	for _, source := range sources {
		if currStage, ok := s.expectRelayStages[source]; !ok {
			notExistSourcesM[source] = struct{}{}
		} else {
			currStagesM[currStage.Expect.String()] = struct{}{}
		}
		stages = append(stages, ha.NewRelayStage(newStage, source))
	}
	notExistSources := strMapToSlice(notExistSourcesM)
	currStages := strMapToSlice(currStagesM)
	if len(notExistSources) > 0 {
		// some sources not exist, reject the request.
		return terror.ErrSchedulerRelayStageNotExist.Generate(notExistSources)
	} else if len(currStages) > 1 {
		// more than one current relay stage exist, but need to update the same one, log a warn.
		s.logger.Warn("update more than one current expectant relay stage to the same one",
			zap.Strings("from", currStages), zap.Stringer("to", newStage))
	}

	// put the stages into etcd.
	_, err := ha.PutRelayStage(s.etcdCli, stages...)
	if err != nil {
		return err
	}

	// update the stages in the scheduler.
	for _, stage := range stages {
		s.expectRelayStages[stage.Source] = stage
	}

	return nil
}

// GetExpectRelayStage returns the current expect relay stage.
// If the stage not exists, an invalid stage is returned.
// This func is used for testing.
func (s *Scheduler) GetExpectRelayStage(source string) ha.Stage {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if stage, ok := s.expectRelayStages[source]; ok {
		return stage
	}
	return ha.NewRelayStage(pb.Stage_InvalidStage, source)
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
		// create and record the worker agent.
		w, err2 := s.recordWorker(info)
		if err2 != nil {
			return 0, err2
		}
		// set the stage as Free if it's keep alive.
		if _, ok := kam[name]; ok {
			w.ToFree()
		}
		// set the stage as Bound and record the bound relationship if exists.
		if bound, ok := sbm[name]; ok {
			err2 = s.boundWorker(w, bound)
			if err2 != nil {
				return 0, err2
			}
		}
	}
	return rev, nil
}

// handleWorkerEv handles the online/offline status change event of DM-worker instances.
func (s *Scheduler) handleWorkerEv(ctx context.Context, evCh <-chan ha.WorkerEvent, errCh <-chan error) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-evCh:
			if !ok {
				return
			}
			s.logger.Info("receive worker status change event", zap.Bool("delete", ev.IsDeleted), zap.Stringer("event", ev))
			var err error
			if ev.IsDeleted {
				err = s.handleWorkerOffline(ev)
			} else {
				err = s.handleWorkerOnline(ev)
			}
			if err != nil {
				s.logger.Error("fail to handle worker status change event", zap.Stringer("event", ev), zap.Error(err))
			}
		case err, ok := <-errCh:
			if !ok {
				return
			}
			// NOTE: we only log the `err` here, but we should update metrics and do more works for it later.
			s.logger.Error("receive error when watching worker status change event", zap.Error(err))
		}
	}
}

// handleWorkerOnline handles the scheduler when a DM-worker become online.
// This should try to bound an unbounded source to it.
// NOTE: this func need to hold the mutex.
func (s *Scheduler) handleWorkerOnline(ev ha.WorkerEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. find the worker.
	w, ok := s.workers[ev.WorkerName]
	if !ok {
		s.logger.Warn("worker for the event not exists", zap.Stringer("event", ev))
		return nil
	}

	// 2. check whether is bound.
	if w.Stage() == WorkerBound {
		s.logger.Warn("worker already bound", zap.Stringer("bound", w.Bound()))
	}

	// 3. change the stage (from Offline) to Free.
	w.ToFree()

	// 4. check whether any unbound source exists.
	if len(s.unbounds) == 0 {
		s.logger.Info("no unbound sources exist", zap.Stringer("event", ev))
		return nil
	}

	// 5. pop a source to bound, priority supported if needed later.
	// DO NOT forget to push it back if fail to bound.
	source := s.unbounds[0]
	s.unbounds = s.unbounds[1:]

	var err error
	defer func() {
		if err != nil {
			// push the source back.
			s.unbounds = append(s.unbounds, source)
		}
	}()

	// 6. put the bound relationship into etcd.
	bound := ha.NewSourceBound(source, ev.WorkerName)
	if _, ok := s.expectRelayStages[source]; ok {
		// the relay stage exists before, only put the bound relationship.
		_, err = ha.PutSourceBound(s.etcdCli, bound)
	} else {
		// no relay stage exists before, create a `Runnng` stage and put it with the bound relationship.
		stage := ha.NewRelayStage(pb.Stage_Running, source)
		_, err = ha.PutRelayStageSourceBound(s.etcdCli, stage, bound)
		defer func() {
			if err == nil {
				// 6.1 if not error exist when returning, record the stage.
				s.expectRelayStages[source] = stage
			}
		}()
	}
	if err != nil {
		return err
	}

	// 7. update the bound relationship in the scheduler.
	// this should be done without error because `w.ToFree` called before.
	err = s.boundWorker(w, bound)
	if err != nil {
		return err
	}

	s.logger.Info("bound the worker to source", zap.Stringer("bound", bound), zap.Stringer("event", ev))
	return nil
}

// handleWorkerOffline handles the scheduler when a DM-worker become offline.
// This should unbound any previous bounded source.
// NOTE: this func need to hold the mutex.
func (s *Scheduler) handleWorkerOffline(ev ha.WorkerEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. find the worker.
	w, ok := s.workers[ev.WorkerName]
	if !ok {
		s.logger.Warn("worker for the event not exists", zap.Stringer("event", ev))
		return nil
	}

	// 2. find the bound relationship.
	bound := w.Bound()

	// 3. check whether bound before.
	if bound.Source == "" {
		// 3.1. change the stage (from Free) to Offline.
		w.ToOffline()
		s.logger.Info("worker not bound, no need to unbound", zap.Stringer("event", ev))
		return nil
	}

	// 4. delete the bound relationship in etcd.
	_, err := ha.DeleteSourceBound(s.etcdCli, bound.Worker)
	if err != nil {
		return err
	}

	// 5. unbound for the source.
	s.unboundWorker(bound.Source)

	// 6. change the stage (from Free) to Offline.
	w.ToOffline()

	// 7. push the source to unbound.
	s.pushToUnbound(bound.Source)

	s.logger.Info("unbound the worker for source", zap.Stringer("bound", bound), zap.Stringer("event", ev))
	return nil
}

// pushToUnbound pushes the source to unbound (pending for bound).
// TODO(csuzhangxc): try to bound.
func (s *Scheduler) pushToUnbound(source string) {
	s.unbounds = append(s.unbounds, source)
}

// recordWorker creates the worker agent (with Offline stage) and records in the scheduler.
// this func is used when adding a new worker.
// NOTE: trigger scheduler when the worker become online, not when added.
func (s *Scheduler) recordWorker(info ha.WorkerInfo) (*Worker, error) {
	w, err := NewWorker(info)
	if err != nil {
		return nil, err
	}
	s.workers[info.Name] = w
	return w, nil
}

// deleteWorker deletes the recorded worker and bound.
// this func is used when removing the worker.
// NOTE: delete work should unbound the source.
func (s *Scheduler) deleteWorker(name string) {
	w, ok := s.workers[name]
	if !ok {
		return
	}
	w.Close()
	delete(s.workers, name)
	delete(s.bounds, w.Bound().Source)
}

// boundWorker bounds the worker with the source.
// this func is used when received keep-alive from the worker.
func (s *Scheduler) boundWorker(w *Worker, b ha.SourceBound) error {
	err := w.ToBound(b)
	if err != nil {
		return err
	}
	s.bounds[b.Source] = w
	return nil
}

// unboundWorker unbounds the worker with the source.
// this func is used:
// - when removing the upstream source.
// - when lost keep-alive from a worker.
func (s *Scheduler) unboundWorker(source string) {
	w, ok := s.bounds[source]
	if !ok {
		return
	}
	w.ToFree()
	delete(s.bounds, source)
}

// reset resets the internal status.
func (s *Scheduler) reset() {
	s.sourceCfgs = make(map[string]config.MysqlConfig)
	s.subTaskCfgs = make(map[string]map[string]config.SubTaskConfig)
	s.workers = make(map[string]*Worker)
	s.bounds = make(map[string]*Worker)
	s.unbounds = make([]string, 0, 10)
	s.expectRelayStages = make(map[string]ha.Stage)
	s.expectSubTaskStages = make(map[string]map[string]ha.Stage)
}

// strMapToSlice converts a `map[string]struct{}` to `[]string`.
func strMapToSlice(m map[string]struct{}) []string {
	ret := make([]string, 0, len(m))
	for s := range m {
		ret = append(ret, s)
	}
	return ret
}
