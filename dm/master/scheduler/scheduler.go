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
	"sort"
	"sync"

	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/master/workerrpc"
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
// Cases trigger a source-to-worker bound try:
// - a worker from Offline to Free:
//   - receive keep-alive.
// - a worker from Bound to Free:
//   - trigger by unbound: `a source removed`.
// - a new source added:
//   - add source request from user.
// - a source unbound from another worker:
//   - trigger by unbound: `a worker from Bound to Offline`.
//   - TODO(csuzhangxc): design a strategy to ensure the old worker already shutdown its work.
// Cases trigger a source-to-worker unbound try.
// - a worker from Bound to Offline:
//   - lost keep-alive.
// - a source removed:
//   - remove source request from user.
// TODO: try to handle the return `err` of etcd operations,
//   because may put into etcd, but the response to the etcd client interrupted.
type Scheduler struct {
	mu sync.RWMutex

	logger log.Logger

	started bool // whether the scheduler already started for work.
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	etcdCli *clientv3.Client

	// all source configs, source ID -> source config.
	// add:
	// - add source by user request (calling `AddSourceCfg`).
	// - recover from etcd (calling `recoverSources`).
	// delete:
	// - remove source by user request (calling `RemoveSourceCfg`).
	sourceCfgs map[string]config.SourceConfig

	// all subtask configs, task name -> source ID -> subtask config.
	// add:
	// - add/start subtask by user request (calling `AddSubTasks`).
	// - recover from etcd (calling `recoverSubTasks`).
	// delete:
	// - remove/stop subtask by user request (calling `RemoveSubTasks`).
	subTaskCfgs map[string]map[string]config.SubTaskConfig

	// all DM-workers, worker name -> worker.
	// add:
	// - add worker by user request (calling `AddWorker`).
	// - recover from etcd (calling `recoverWorkersBounds`).
	// delete:
	// - remove worker by user request (calling `RemoveWorker`).
	workers map[string]*Worker

	// all bound relationship, source ID -> worker.
	// add:
	// - when bounding a source to a worker.
	// delete:
	// - when unbounding a source from a worker.
	// see `Cases trigger a source-to-worker bound try` above.
	bounds map[string]*Worker

	// unbound (pending to bound) sources.
	// NOTE: refactor to support scheduling by priority.
	// add:
	// - add source by user request (calling `AddSourceCfg`).
	// - recover from etcd (calling `recoverWorkersBounds`).
	// - when the bounding worker become offline.
	// delete:
	// - remove source by user request (calling `RemoveSourceCfg`).
	// - when bounded the source to a worker.
	unbounds map[string]struct{}

	// expectant relay stages for sources, source ID -> stage.
	// add:
	// - bound the source to a worker (at first time).
	// - recover from etcd (calling `recoverSources`).
	// update:
	// - update stage by user request (calling `UpdateExpectRelayStage`).
	// delete:
	// - remove source by user request (calling `RemoveSourceCfg`).
	expectRelayStages map[string]ha.Stage

	// expectant subtask stages for tasks & sources, task name -> source ID -> stage.
	// add:
	// - add/start subtask by user request (calling `AddSubTasks`).
	// - recover from etcd (calling `recoverSubTasks`).
	// update:
	// - update stage by user request (calling `UpdateExpectSubTaskStage`).
	// delete:
	// - remove/stop subtask by user request (calling `RemoveSubTasks`).
	expectSubTaskStages map[string]map[string]ha.Stage
}

// NewScheduler creates a new scheduler instance.
func NewScheduler(pLogger *log.Logger) *Scheduler {
	return &Scheduler{
		logger:              pLogger.WithFields(zap.String("component", "scheduler")),
		sourceCfgs:          make(map[string]config.SourceConfig),
		subTaskCfgs:         make(map[string]map[string]config.SubTaskConfig),
		workers:             make(map[string]*Worker),
		bounds:              make(map[string]*Worker),
		unbounds:            make(map[string]struct{}),
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

	// recover previous status from etcd.
	err := s.recoverSources(etcdCli)
	if err != nil {
		return err
	}
	err = s.recoverSubTasks(etcdCli)
	if err != nil {
		return err
	}
	rev, err := s.recoverWorkersBounds(etcdCli)
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

	if !s.started {
		s.mu.Unlock()
		return
	}

	s.logger.Info("the scheduler is closing")
	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}
	s.mu.Unlock()

	// need to wait for goroutines to return which may hold the mutex.
	s.wg.Wait()

	s.mu.Lock()
	defer s.mu.Unlock()
	s.started = false // closed now.
	s.logger.Info("the scheduler has closed")
}

// AddSourceCfg adds the upstream source config to the cluster.
// NOTE: please verify the config before call this.
func (s *Scheduler) AddSourceCfg(cfg config.SourceConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return terror.ErrSchedulerNotStarted.Generate()
	}

	// 1. check whether exists.
	if _, ok := s.sourceCfgs[cfg.SourceID]; ok {
		return terror.ErrSchedulerSourceCfgExist.Generate(cfg.SourceID)
	}

	// 2. put the config into etcd.
	_, err := ha.PutSourceCfg(s.etcdCli, cfg)
	if err != nil {
		return err
	}

	// 3. record the config in the scheduler.
	s.sourceCfgs[cfg.SourceID] = cfg

	// 4. try to bound it to a Free worker.
	bounded, err := s.tryBoundForSource(cfg.SourceID)
	if err != nil {
		return err
	} else if !bounded {
		// 5. record the source as unbounded.
		s.unbounds[cfg.SourceID] = struct{}{}
	}
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

	// 1. check whether the config exists.
	_, ok := s.sourceCfgs[source]
	if !ok {
		return terror.ErrSchedulerSourceCfgNotExist.Generate(source)
	}

	// 2. check whether any subtask exists for the source.
	existingSubtasksM := make(map[string]struct{})
	for task, cfg := range s.subTaskCfgs {
		for source2 := range cfg {
			if source2 == source {
				existingSubtasksM[task] = struct{}{}
			}
		}
	}
	existingSubtasks := strMapToSlice(existingSubtasksM)
	if len(existingSubtasks) > 0 {
		return terror.ErrSchedulerSourceOpTaskExist.Generate(source, existingSubtasks)
	}

	// 3. find worker name by source ID.
	var (
		workerName string // empty should be fine below.
		worker     *Worker
	)
	if w, ok := s.bounds[source]; ok {
		worker = w
		workerName = w.BaseInfo().Name
	}

	// 4. delete the info in etcd.
	_, err := ha.DeleteSourceCfgRelayStageSourceBound(s.etcdCli, source, workerName)
	if err != nil {
		return err
	}

	// 5. delete the config and expectant stage in the scheduler
	delete(s.sourceCfgs, source)
	delete(s.expectRelayStages, source)

	// 6. unbound for the source.
	s.updateStatusForUnbound(source)

	// 7. remove it from unbounds.
	delete(s.unbounds, source)

	// 8. try to bound the worker for another source.
	if worker != nil {
		_, err = s.tryBoundForWorker(worker)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetSourceCfgByID gets source config by source ID.
func (s *Scheduler) GetSourceCfgByID(source string) *config.SourceConfig {
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

	// 1. check whether exists.
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

	// 2. construct `Running` stages when adding.
	newCfgs := make([]config.SubTaskConfig, 0, len(cfgs)-len(existSources))
	newStages := make([]ha.Stage, 0, cap(newCfgs))
	unbounds := make([]string, 0)
	for _, cfg := range cfgs {
		if _, ok := existSourcesM[cfg.SourceID]; ok {
			continue
		}
		newCfgs = append(newCfgs, cfg)
		newStages = append(newStages, ha.NewSubTaskStage(pb.Stage_Running, cfg.SourceID, cfg.Name))
		if _, ok := s.bounds[cfg.SourceID]; !ok {
			unbounds = append(unbounds, cfg.SourceID)
		}
	}

	// 3. check whether any sources unbound.
	if len(unbounds) > 0 {
		return terror.ErrSchedulerSourcesUnbound.Generate(unbounds)
	}

	// 4. put the configs and stages into etcd.
	_, err := ha.PutSubTaskCfgStage(s.etcdCli, newCfgs, newStages)
	if err != nil {
		return err
	}

	// 5. record the config and the expectant stage.
	for _, cfg := range newCfgs {
		if _, ok := s.subTaskCfgs[cfg.Name]; !ok {
			s.subTaskCfgs[cfg.Name] = make(map[string]config.SubTaskConfig)
		}
		s.subTaskCfgs[cfg.Name][cfg.SourceID] = cfg
	}
	for _, stage := range newStages {
		if _, ok := s.expectSubTaskStages[stage.Task]; !ok {
			s.expectSubTaskStages[stage.Task] = make(map[string]ha.Stage)
		}
		s.expectSubTaskStages[stage.Task][stage.Source] = stage
	}

	return nil
}

// RemoveSubTasks removes the information of one or more subtaks for one task.
func (s *Scheduler) RemoveSubTasks(task string, sources ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return terror.ErrSchedulerNotStarted.Generate()
	}

	if task == "" || len(sources) == 0 {
		return nil // no subtask need to stop, this should not happen.
	}

	// 1. check the task exists.
	stagesM, ok1 := s.expectSubTaskStages[task]
	cfgsM, ok2 := s.subTaskCfgs[task]
	if !ok1 || !ok2 {
		return terror.ErrSchedulerSubTaskOpTaskNotExist.Generate(task)
	}

	var (
		notExistSourcesM = make(map[string]struct{})
		stages           = make([]ha.Stage, 0, len(sources))
		cfgs             = make([]config.SubTaskConfig, 0, len(sources))
	)
	for _, source := range sources {
		if stage, ok := stagesM[source]; !ok {
			notExistSourcesM[source] = struct{}{}
		} else {
			stages = append(stages, stage)
		}
		if cfg, ok := cfgsM[source]; ok {
			cfgs = append(cfgs, cfg)
		}
	}
	notExistSources := strMapToSlice(notExistSourcesM)
	if len(notExistSources) > 0 {
		// some sources not exist, reject the request.
		return terror.ErrSchedulerSubTaskOpSourceNotExist.Generate(notExistSources)
	}

	// 2. delete the configs and the stages.
	_, err := ha.DeleteSubTaskCfgStage(s.etcdCli, cfgs, stages)
	if err != nil {
		return err
	}

	// 3. clear the config and the expectant stage.
	for _, cfg := range cfgs {
		delete(s.subTaskCfgs[task], cfg.SourceID)
	}
	if len(s.subTaskCfgs[task]) == 0 {
		delete(s.subTaskCfgs, task)
	}
	for _, stage := range stages {
		delete(s.expectSubTaskStages[task], stage.Source)
	}
	if len(s.expectSubTaskStages[task]) == 0 {
		delete(s.expectSubTaskStages, task)
	}

	return nil
}

// GetSubTaskCfgByTaskSource gets subtask config by task name and source ID.
func (s *Scheduler) GetSubTaskCfgByTaskSource(task, source string) *config.SubTaskConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cfgM, ok := s.subTaskCfgs[task]
	if !ok {
		return nil
	}
	cfg, ok := cfgM[source]
	if !ok {
		return nil
	}
	clone := cfg
	return &clone
}

// GetSubTaskCfgsByTask gets subtask configs' map by task name.
func (s *Scheduler) GetSubTaskCfgsByTask(task string) map[string]*config.SubTaskConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cfgM, ok := s.subTaskCfgs[task]
	if !ok {
		return nil
	}
	cloneM := make(map[string]*config.SubTaskConfig, len(cfgM))
	for source, cfg := range cfgM {
		clone := cfg
		cloneM[source] = &clone
	}
	return cloneM
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

	// 1. check whether exists.
	if w, ok := s.workers[name]; ok {
		// NOTE: we do not support add the worker with different address now, support if needed later.
		// but we support add the worker with all the same information multiple times, and only the first one take effect,
		// because this is needed when restarting the worker.
		if addr == w.BaseInfo().Addr {
			s.logger.Warn("add the same worker again", zap.Stringer("worker info", w.BaseInfo()))
			return nil
		}
		return terror.ErrSchedulerWorkerExist.Generate(w.BaseInfo())
	}

	// 2. put the base info into etcd.
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

// BoundSources returns all bound source IDs in increasing order.
func (s *Scheduler) BoundSources() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	IDs := make([]string, 0, len(s.bounds))
	for ID := range s.bounds {
		IDs = append(IDs, ID)
	}
	sort.Strings(IDs)
	return IDs
}

// UnboundSources returns all unbound source IDs in increasing order.
func (s *Scheduler) UnboundSources() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	IDs := make([]string, 0, len(s.unbounds))
	for ID := range s.unbounds {
		IDs = append(IDs, ID)
	}
	sort.Strings(IDs)
	return IDs
}

// UpdateExpectRelayStage updates the current expect relay stage.
// now, only support updates:
// - from `Running` to `Paused`.
// - from `Paused` to `Running`.
// NOTE: from `Running` to `Running` and `Paused` to `Paused` still update the data in etcd,
// because some user may want to update `{Running, Paused, ...}` to `{Running, Running, ...}`.
// so, this should be also supported in DM-worker.
func (s *Scheduler) UpdateExpectRelayStage(newStage pb.Stage, sources ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return terror.ErrSchedulerNotStarted.Generate()
	}

	if len(sources) == 0 {
		return nil // no sources need to update the stage, this should not happen.
	}

	// 1. check the new expectant stage.
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
		return terror.ErrSchedulerRelayStageSourceNotExist.Generate(notExistSources)
	} else if len(currStages) > 1 {
		// more than one current relay stage exist, but need to update to the same one, log a warn.
		s.logger.Warn("update more than one current expectant relay stage to the same one",
			zap.Strings("from", currStages), zap.Stringer("to", newStage))
	}

	// 2. put the stages into etcd.
	_, err := ha.PutRelayStage(s.etcdCli, stages...)
	if err != nil {
		return err
	}

	// 3. update the stages in the scheduler.
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

// UpdateExpectSubTaskStage updates the current expect subtask stage.
// now, only support updates:
// - from `Running` to `Paused`.
// - from `Paused` to `Running`.
// NOTE: from `Running` to `Running` and `Paused` to `Paused` still update the data in etcd,
// because some user may want to update `{Running, Paused, ...}` to `{Running, Running, ...}`.
// so, this should be also supported in DM-worker.
func (s *Scheduler) UpdateExpectSubTaskStage(newStage pb.Stage, task string, sources ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return terror.ErrSchedulerNotStarted.Generate()
	}

	if task == "" || len(sources) == 0 {
		return nil // no subtask need to update, this should not happen.
	}

	// 1. check the new expectant stage.
	switch newStage {
	case pb.Stage_Running, pb.Stage_Paused:
	default:
		return terror.ErrSchedulerSubTaskStageInvalidUpdate.Generate(newStage)
	}

	// 2. check the task exists.
	stagesM, ok := s.expectSubTaskStages[task]
	if !ok {
		return terror.ErrSchedulerSubTaskOpTaskNotExist.Generate(task)
	}

	var (
		notExistSourcesM = make(map[string]struct{})
		currStagesM      = make(map[string]struct{})
		stages           = make([]ha.Stage, 0, len(sources))
	)
	for _, source := range sources {
		if currStage, ok := stagesM[source]; !ok {
			notExistSourcesM[source] = struct{}{}
		} else {
			currStagesM[currStage.Expect.String()] = struct{}{}
		}
		stages = append(stages, ha.NewSubTaskStage(newStage, source, task))
	}
	notExistSources := strMapToSlice(notExistSourcesM)
	currStages := strMapToSlice(currStagesM)
	if len(notExistSources) > 0 {
		// some sources not exist, reject the request.
		return terror.ErrSchedulerSubTaskOpSourceNotExist.Generate(notExistSources)
	} else if len(currStages) > 1 {
		// more than one current subtask stage exist, but need to update to the same one, log a warn.
		s.logger.Warn("update more than one current expectant subtask stage to the same one",
			zap.Strings("from", currStages), zap.Stringer("to", newStage))
	}

	// 3. put the stages into etcd.
	_, err := ha.PutSubTaskStage(s.etcdCli, stages...)
	if err != nil {
		return err
	}

	// 4. update the stages in the scheduler.
	for _, stage := range stages {
		s.expectSubTaskStages[task][stage.Source] = stage
	}

	return nil
}

// GetExpectSubTaskStage returns the current expect subtask stage.
// If the stage not exists, an invalid stage is returned.
// This func is used for testing.
func (s *Scheduler) GetExpectSubTaskStage(task, source string) ha.Stage {
	s.mu.RLock()
	defer s.mu.RUnlock()
	invalidStage := ha.NewSubTaskStage(pb.Stage_InvalidStage, source, task)
	stageM, ok := s.expectSubTaskStages[task]
	if !ok {
		return invalidStage
	}
	stage, ok := stageM[source]
	if !ok {
		return invalidStage
	}
	return stage
}

// recoverSourceCfgs recovers history source configs and expectant relay stages from etcd.
func (s *Scheduler) recoverSources(cli *clientv3.Client) error {
	// get all source configs.
	cfgM, _, err := ha.GetAllSourceCfg(cli)
	if err != nil {
		return err
	}
	// get all relay stages.
	stageM, _, err := ha.GetAllRelayStage(cli)
	if err != nil {
		return err
	}

	// recover in-memory data.
	for source, cfg := range cfgM {
		s.sourceCfgs[source] = cfg
	}
	for source, stage := range stageM {
		s.expectRelayStages[source] = stage
	}

	return nil
}

// recoverSubTasks recovers history subtask configs and expectant subtask stages from etcd.
func (s *Scheduler) recoverSubTasks(cli *clientv3.Client) error {
	// get all subtask configs.
	cfgMM, _, err := ha.GetAllSubTaskCfg(cli)
	if err != nil {
		return err
	}
	// get all subtask stages.
	stageMM, _, err := ha.GetAllSubTaskStage(cli)
	if err != nil {
		return nil
	}

	// recover in-memory data.
	for source, cfgM := range cfgMM {
		for task, cfg := range cfgM {
			if _, ok := s.subTaskCfgs[task]; !ok {
				s.subTaskCfgs[task] = make(map[string]config.SubTaskConfig)
			}
			s.subTaskCfgs[task][source] = cfg
		}
	}
	for source, stageM := range stageMM {
		for task, stage := range stageM {
			if _, ok := s.expectSubTaskStages[task]; !ok {
				s.expectSubTaskStages[task] = make(map[string]ha.Stage)
			}
			s.expectSubTaskStages[task][source] = stage
		}
	}

	return nil
}

// recoverWorkersBounds recovers history DM-worker info and status from etcd.
// and it also recovers the bound/unbound relationship.
func (s *Scheduler) recoverWorkersBounds(cli *clientv3.Client) (int64, error) {
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
	sbm, _, err := ha.GetSourceBound(cli, "")
	if err != nil {
		return 0, err
	}

	// 3. get all history offline status.
	kam, rev, err := ha.GetKeepAliveWorkers(cli)
	if err != nil {
		return 0, err
	}

	boundsToTrigger := make([]ha.SourceBound, 0)
	// 4. recover DM-worker info and status.
	for name, info := range wim {
		// create and record the worker agent.
		w, err2 := s.recordWorker(info)
		if err2 != nil {
			return 0, err2
		}
		// set the stage as Free if it's keep alive.
		if _, ok := kam[name]; ok {
			w.ToFree()
			// set the stage as Bound and record the bound relationship if exists.
			if bound, ok := sbm[name]; ok {
				boundsToTrigger = append(boundsToTrigger, bound)
				err2 = s.updateStatusForBound(w, bound)
				if err2 != nil {
					return 0, err2
				}
				delete(sbm, name)
			}
		}
	}

	// 5. delete invalid source bound info in etcd
	if len(sbm) > 0 {
		invalidSourceBounds := make([]string, 0, len(sbm))
		for name := range sbm {
			invalidSourceBounds = append(invalidSourceBounds, name)
		}
		_, err = ha.DeleteSourceBound(cli, invalidSourceBounds...)
		if err != nil {
			return 0, err
		}
	}

	// 6. put trigger source bounds info to etcd to order dm-workers to start source
	if len(boundsToTrigger) > 0 {
		_, err = ha.PutSourceBound(cli, boundsToTrigger...)
		if err != nil {
			return 0, nil
		}
	}

	// 7. recover bounds/unbounds, all sources which not in bounds should be in unbounds.
	for source := range s.sourceCfgs {
		if _, ok := s.bounds[source]; !ok {
			s.unbounds[source] = struct{}{}
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
				// TODO(csuzhangxc): report the error through metrics or other methods.
				s.logger.Error("fail to handle worker status change event", zap.Bool("delete", ev.IsDeleted), zap.Stringer("event", ev), zap.Error(err))
			}
		case err, ok := <-errCh:
			if !ok {
				return
			}
			// TODO(csuzhangxc): we only log the `err` here, but we should update metrics and do more works for it later.
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
		// TODO: When dm-worker keepalive is broken, it will turn off its own running source
		// After keepalive is restored, this dm-worker should continue to run the previously bound source
		// So we PutSourceBound here to trigger dm-worker to get this event and start source again.
		// If this worker still start a source, it doesn't matter. dm-worker will omit same source and reject source with different name
		s.logger.Warn("worker already bound", zap.Stringer("bound", w.Bound()))
		_, err := ha.PutSourceBound(s.etcdCli, w.Bound())
		return err
	}

	// 3. change the stage (from Offline) to Free.
	w.ToFree()

	// 4. try to bound an unbounded source.
	_, err := s.tryBoundForWorker(w)
	return err
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
	s.updateStatusForUnbound(bound.Source)

	// 6. change the stage (from Free) to Offline.
	w.ToOffline()

	s.logger.Info("unbound the worker for source", zap.Stringer("bound", bound), zap.Stringer("event", ev))

	// 7. try to bound the source to a Free worker again.
	bounded, err := s.tryBoundForSource(bound.Source)
	if err != nil {
		return err
	} else if !bounded {
		// 8. record the source as unbounded.
		s.unbounds[bound.Source] = struct{}{}
	}

	return nil
}

// tryBoundForWorker tries to bound a random unbounded source to the worker.
// returns (true, nil) after bounded.
func (s *Scheduler) tryBoundForWorker(w *Worker) (bounded bool, err error) {
	// 1. check whether any unbound source exists.
	var source string
	for source = range s.unbounds {
		break // got a source.
	}
	if source == "" {
		s.logger.Info("no unbound sources need to bound", zap.Stringer("worker", w.BaseInfo()))
		return false, nil
	}

	// 2. pop a source to bound, priority supported if needed later.
	// DO NOT forget to push it back if fail to bound.
	delete(s.unbounds, source)
	defer func() {
		if err != nil {
			// push the source back.
			s.unbounds[source] = struct{}{}
		}
	}()

	// 3. try to bound them.
	err = s.boundSourceToWorker(source, w)
	if err != nil {
		return false, err
	}
	return true, nil
}

// tryBoundForSource tries to bound a source to a random Free worker.
// returns (true, nil) after bounded.
func (s *Scheduler) tryBoundForSource(source string) (bool, error) {
	// 1. try to find a random Free worker.
	var worker *Worker
	for _, w := range s.workers {
		if w.Stage() == WorkerFree {
			worker = w
			break
		}
	}
	if worker == nil {
		s.logger.Info("no free worker exists for bound", zap.String("source", source))
		return false, nil
	}

	// 2. try to bound them.
	err := s.boundSourceToWorker(source, worker)
	if err != nil {
		return false, err
	}
	return true, nil
}

// boundSourceToWorker bounds the source and worker together.
// we should check the bound relationship of the source and the stage of the worker in the caller.
func (s *Scheduler) boundSourceToWorker(source string, w *Worker) error {
	// 1. put the bound relationship into etcd.
	var err error
	bound := ha.NewSourceBound(source, w.BaseInfo().Name)
	if _, ok := s.expectRelayStages[source]; ok {
		// the relay stage exists before, only put the bound relationship.
		_, err = ha.PutSourceBound(s.etcdCli, bound)
	} else {
		// no relay stage exists before, create a `Runnng` stage and put it with the bound relationship.
		stage := ha.NewRelayStage(pb.Stage_Running, source)
		_, err = ha.PutRelayStageSourceBound(s.etcdCli, stage, bound)
		defer func() {
			if err == nil {
				// 1.1 if no error exist when returning, record the stage.
				s.expectRelayStages[source] = stage
			}
		}()
	}
	if err != nil {
		return err
	}

	// 2. update the bound relationship in the scheduler.
	err = s.updateStatusForBound(w, bound)
	if err != nil {
		return err
	}

	s.logger.Info("bound the source to worker", zap.Stringer("bound", bound))
	return nil
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
// NOTE: trigger scheduler when the worker become offline, not when deleted.
func (s *Scheduler) deleteWorker(name string) {
	w, ok := s.workers[name]
	if !ok {
		return
	}
	w.Close()
	delete(s.workers, name)
}

// updateStatusForBound updates the in-memory status for bound, including:
// - update the stage of worker to `Bound`.
// - record the bound relationship in the scheduler.
// this func is called after the bound relationship existed in etcd.
func (s *Scheduler) updateStatusForBound(w *Worker, b ha.SourceBound) error {
	err := w.ToBound(b)
	if err != nil {
		return err
	}
	s.bounds[b.Source] = w
	return nil
}

// updateStatusForUnbound updates the in-memory status for unbound, including:
// - update the stage of worker to `Free`.
// - remove the bound relationship in the scheduler.
// this func is called after the bound relationship removed from etcd.
func (s *Scheduler) updateStatusForUnbound(source string) {
	w, ok := s.bounds[source]
	if !ok {
		return
	}
	w.ToFree()
	delete(s.bounds, source)
}

// reset resets the internal status.
func (s *Scheduler) reset() {
	s.sourceCfgs = make(map[string]config.SourceConfig)
	s.subTaskCfgs = make(map[string]map[string]config.SubTaskConfig)
	s.workers = make(map[string]*Worker)
	s.bounds = make(map[string]*Worker)
	s.unbounds = make(map[string]struct{})
	s.expectRelayStages = make(map[string]ha.Stage)
	s.expectSubTaskStages = make(map[string]map[string]ha.Stage)
}

// strMapToSlice converts a `map[string]struct{}` to `[]string` in increasing order.
func strMapToSlice(m map[string]struct{}) []string {
	ret := make([]string, 0, len(m))
	for s := range m {
		ret = append(ret, s)
	}
	sort.Strings(ret)
	return ret
}

// SetWorkerClientForTest sets mockWorkerClient for specified worker, only used for test
func (s *Scheduler) SetWorkerClientForTest(name string, mockCli workerrpc.Client) {
	if _, ok := s.workers[name]; ok {
		s.workers[name].cli = mockCli
	}
}
