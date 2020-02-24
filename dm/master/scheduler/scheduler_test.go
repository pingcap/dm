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
	"testing"
	"time"

	. "github.com/pingcap/check"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/integration"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/ha"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

const (
	// do not forget to update this path if the file removed/renamed.
	sourceSampleFile = "../../worker/source.toml"
	// do not forget to update this path if the file removed/renamed.
	subTaskSampleFile = "../../worker/subtask.toml"
)

var (
	etcdTestCli *clientv3.Client
)

func TestScheduler(t *testing.T) {
	log.InitLogger(&log.Config{})

	mockCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer mockCluster.Terminate(t)

	etcdTestCli = mockCluster.RandClient()

	TestingT(t)
}

// clear keys in etcd test cluster.
func clearTestInfoOperation(c *C) {
	c.Assert(ha.ClearTestInfoOperation(etcdTestCli), IsNil)
}

type testScheduler struct{}

var _ = Suite(&testScheduler{})

var (
	sourceCfgEmpty config.SourceConfig
	stageEmpty     ha.Stage
)

func (t *testScheduler) TestScheduler(c *C) {
	defer clearTestInfoOperation(c)

	var (
		logger       = log.L()
		s            = NewScheduler(&logger)
		sourceID1    = "mysql-replica-1"
		sourceID2    = "mysql-replica-2"
		workerName1  = "dm-worker-1"
		workerName2  = "dm-worker-2"
		workerAddr1  = "127.0.0.1:8262"
		workerAddr2  = "127.0.0.1:18262"
		taskName1    = "task-1"
		taskName2    = "task-2"
		workerInfo1  = ha.NewWorkerInfo(workerName1, workerAddr1)
		workerInfo2  = ha.NewWorkerInfo(workerName2, workerAddr2)
		sourceCfg1   config.SourceConfig
		subtaskCfg1  config.SubTaskConfig
		keepAliveTTL = int64(1) // NOTE: this should be >= minLeaseTTL, in second.
	)
	c.Assert(sourceCfg1.LoadFromFile(sourceSampleFile), IsNil)
	sourceCfg1.SourceID = sourceID1
	sourceCfg2 := sourceCfg1
	sourceCfg2.SourceID = sourceID2

	c.Assert(subtaskCfg1.DecodeFile(subTaskSampleFile, true), IsNil)
	subtaskCfg1.SourceID = sourceID1
	subtaskCfg1.Name = taskName1
	c.Assert(subtaskCfg1.Adjust(true), IsNil)
	subtaskCfg21 := subtaskCfg1
	subtaskCfg21.Name = taskName2
	c.Assert(subtaskCfg21.Adjust(true), IsNil)
	subtaskCfg22 := subtaskCfg21
	subtaskCfg22.SourceID = sourceID2
	c.Assert(subtaskCfg22.Adjust(true), IsNil)

	// not started scheduler can't do anything.
	c.Assert(terror.ErrSchedulerNotStarted.Equal(s.AddSourceCfg(sourceCfg1)), IsTrue)
	c.Assert(terror.ErrSchedulerNotStarted.Equal(s.RemoveSourceCfg(sourceID1)), IsTrue)
	c.Assert(terror.ErrSchedulerNotStarted.Equal(s.AddSubTasks(subtaskCfg1)), IsTrue)
	c.Assert(terror.ErrSchedulerNotStarted.Equal(s.RemoveSubTasks(taskName1, sourceID1)), IsTrue)
	c.Assert(terror.ErrSchedulerNotStarted.Equal(s.AddWorker(workerName1, workerAddr1)), IsTrue)
	c.Assert(terror.ErrSchedulerNotStarted.Equal(s.RemoveWorker(workerName1)), IsTrue)
	c.Assert(terror.ErrSchedulerNotStarted.Equal(s.UpdateExpectRelayStage(pb.Stage_Running, sourceID1)), IsTrue)
	c.Assert(terror.ErrSchedulerNotStarted.Equal(s.UpdateExpectSubTaskStage(pb.Stage_Running, taskName1, sourceID1)), IsTrue)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// CASE 1: start without any previous info.
	c.Assert(s.Start(ctx, etcdTestCli), IsNil)
	c.Assert(terror.ErrSchedulerStarted.Equal(s.Start(ctx, etcdTestCli)), IsTrue) // start multiple times.
	s.Close()
	s.Close() // close multiple times.

	// CASE 2: start again without any previous info.
	c.Assert(s.Start(ctx, etcdTestCli), IsNil)

	// CASE 2.1: add the first source config.
	// no source config exist before added.
	t.sourceCfgNotExist(c, s, sourceID1)
	// add source config1.
	c.Assert(s.AddSourceCfg(sourceCfg1), IsNil)
	c.Assert(terror.ErrSchedulerSourceCfgExist.Equal(s.AddSourceCfg(sourceCfg1)), IsTrue) // can't add multiple times.
	// the source config added.
	t.sourceCfgExist(c, s, sourceCfg1)
	// one unbound source exist (because no free worker).
	t.sourceBounds(c, s, []string{}, []string{sourceID1})

	// CASE 2.2: add the first worker.
	// no worker exist before added.
	t.workerNotExist(c, s, workerName1)
	// add worker1.
	c.Assert(s.AddWorker(workerName1, workerAddr1), IsNil)
	c.Assert(terror.ErrSchedulerWorkerExist.Equal(s.AddWorker(workerName1, workerAddr2)), IsTrue) // can't add with different address now.
	c.Assert(s.AddWorker(workerName1, workerAddr1), IsNil)                                        // but can add the worker multiple times (like restart the worker).
	// the worker added.
	t.workerExist(c, s, workerInfo1)
	t.workerOffline(c, s, workerName1)
	// still no bounds (because the worker is offline).
	t.sourceBounds(c, s, []string{}, []string{sourceID1})
	// no expect relay stage exist (because the source has never been bounded).
	t.relayStageMatch(c, s, sourceID1, pb.Stage_InvalidStage)

	// CASE 2.3: the worker become online.
	// do keep-alive for worker1.
	ctx1, cancel1 := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Assert(ha.KeepAlive(ctx1, etcdTestCli, workerName1, keepAliveTTL), IsNil)
	}()
	// wait for source1 bound to worker1.
	utils.WaitSomething(30, 10*time.Millisecond, func() bool {
		bounds := s.BoundSources()
		return len(bounds) == 1 && bounds[0] == sourceID1
	})
	t.sourceBounds(c, s, []string{sourceID1}, []string{})
	t.workerBound(c, s, ha.NewSourceBound(sourceID1, workerName1))
	// expect relay stage become Running after the first bound.
	t.relayStageMatch(c, s, sourceID1, pb.Stage_Running)

	// CASE 2.4: pause the relay.
	c.Assert(s.UpdateExpectRelayStage(pb.Stage_Paused, sourceID1), IsNil)
	t.relayStageMatch(c, s, sourceID1, pb.Stage_Paused)
	// update relay stage without source take no effect now (and return without error).
	c.Assert(s.UpdateExpectRelayStage(pb.Stage_Running), IsNil)
	t.relayStageMatch(c, s, sourceID1, pb.Stage_Paused)
	// update to non-(Running, Paused) stage is invalid.
	c.Assert(terror.ErrSchedulerRelayStageInvalidUpdate.Equal(s.UpdateExpectRelayStage(pb.Stage_InvalidStage, sourceID1)), IsTrue)
	c.Assert(terror.ErrSchedulerRelayStageInvalidUpdate.Equal(s.UpdateExpectRelayStage(pb.Stage_New, sourceID1)), IsTrue)
	c.Assert(terror.ErrSchedulerRelayStageInvalidUpdate.Equal(s.UpdateExpectRelayStage(pb.Stage_Stopped, sourceID1)), IsTrue)
	c.Assert(terror.ErrSchedulerRelayStageInvalidUpdate.Equal(s.UpdateExpectRelayStage(pb.Stage_Finished, sourceID1)), IsTrue)
	// can't update stage with not existing sources now.
	c.Assert(terror.ErrSchedulerRelayStageSourceNotExist.Equal(s.UpdateExpectRelayStage(pb.Stage_Running, sourceID1, sourceID2)), IsTrue)
	t.relayStageMatch(c, s, sourceID1, pb.Stage_Paused)

	// CASE 2.5: resume the relay.
	c.Assert(s.UpdateExpectRelayStage(pb.Stage_Running, sourceID1), IsNil)
	t.relayStageMatch(c, s, sourceID1, pb.Stage_Running)

	// CASE 2.6: start a task with only one source.
	// no subtask config exists before start.
	c.Assert(s.AddSubTasks(), IsNil) // can call without configs, return without error, but take no effect.
	t.subTaskCfgNotExist(c, s, taskName1, sourceID1)
	t.subTaskStageMatch(c, s, taskName1, sourceID1, pb.Stage_InvalidStage)
	// start the task.
	c.Assert(s.AddSubTasks(subtaskCfg1), IsNil)
	c.Assert(terror.ErrSchedulerSubTaskExist.Equal(s.AddSubTasks(subtaskCfg1)), IsTrue) // add again.
	// subtask config and stage exist.
	t.subTaskCfgExist(c, s, subtaskCfg1)
	t.subTaskStageMatch(c, s, taskName1, sourceID1, pb.Stage_Running)

	// try start a task with two sources, some sources not bound.
	c.Assert(terror.ErrSchedulerSourcesUnbound.Equal(s.AddSubTasks(subtaskCfg21, subtaskCfg22)), IsTrue)
	t.subTaskCfgNotExist(c, s, taskName2, sourceID1)
	t.subTaskStageMatch(c, s, taskName2, sourceID1, pb.Stage_InvalidStage)
	t.subTaskCfgNotExist(c, s, taskName2, sourceID2)
	t.subTaskStageMatch(c, s, taskName2, sourceID2, pb.Stage_InvalidStage)

	// CASE 2.7: pause/resume task1.
	c.Assert(s.UpdateExpectSubTaskStage(pb.Stage_Paused, taskName1, sourceID1), IsNil)
	t.subTaskStageMatch(c, s, taskName1, sourceID1, pb.Stage_Paused)
	c.Assert(s.UpdateExpectSubTaskStage(pb.Stage_Running, taskName1, sourceID1), IsNil)
	t.subTaskStageMatch(c, s, taskName1, sourceID1, pb.Stage_Running)
	// update subtask stage without source or task take no effect now (and return without error).
	c.Assert(s.UpdateExpectSubTaskStage(pb.Stage_Paused, "", sourceID1), IsNil)
	c.Assert(s.UpdateExpectSubTaskStage(pb.Stage_Paused, taskName1), IsNil)
	t.relayStageMatch(c, s, sourceID1, pb.Stage_Running)
	// update to non-(Running, Paused) stage is invalid.
	c.Assert(terror.ErrSchedulerSubTaskStageInvalidUpdate.Equal(s.UpdateExpectSubTaskStage(pb.Stage_InvalidStage, taskName1, sourceID1)), IsTrue)
	c.Assert(terror.ErrSchedulerSubTaskStageInvalidUpdate.Equal(s.UpdateExpectSubTaskStage(pb.Stage_New, taskName1, sourceID1)), IsTrue)
	c.Assert(terror.ErrSchedulerSubTaskStageInvalidUpdate.Equal(s.UpdateExpectSubTaskStage(pb.Stage_Stopped, taskName1, sourceID1)), IsTrue)
	c.Assert(terror.ErrSchedulerSubTaskStageInvalidUpdate.Equal(s.UpdateExpectSubTaskStage(pb.Stage_Finished, taskName1, sourceID1)), IsTrue)
	// can't update stage with not existing sources now.
	c.Assert(terror.ErrSchedulerSubTaskOpSourceNotExist.Equal(s.UpdateExpectSubTaskStage(pb.Stage_Paused, taskName1, sourceID1, sourceID2)), IsTrue)
	t.relayStageMatch(c, s, sourceID1, pb.Stage_Running)

	// CASE 2.8: worker1 become offline.
	// cancel keep-alive.
	cancel1()
	wg.Wait()
	// wait for source1 unbound from worker1.
	utils.WaitSomething(int(3*keepAliveTTL), time.Second, func() bool {
		unbounds := s.UnboundSources()
		return len(unbounds) == 1 && unbounds[0] == sourceID1
	})
	t.sourceBounds(c, s, []string{}, []string{sourceID1})
	// static information are still there.
	t.sourceCfgExist(c, s, sourceCfg1)
	t.subTaskCfgExist(c, s, subtaskCfg1)
	t.workerExist(c, s, workerInfo1)
	// worker1 still exists, but it's offline.
	t.workerOffline(c, s, workerName1)
	// expect relay stage keep Running.
	t.relayStageMatch(c, s, sourceID1, pb.Stage_Running)
	t.subTaskStageMatch(c, s, taskName1, sourceID1, pb.Stage_Running)

	// shutdown the scheduler.
	s.Close()

	// CASE 3: start again with previous `Offline` worker, relay stage, subtask stage.
	c.Assert(s.Start(ctx, etcdTestCli), IsNil)

	// CASE 3.1: previous information should recover.
	// source1 is still unbound.
	t.sourceBounds(c, s, []string{}, []string{sourceID1})
	// worker1 still exists, but it's offline.
	t.workerOffline(c, s, workerName1)
	// static information are still there.
	t.sourceCfgExist(c, s, sourceCfg1)
	t.subTaskCfgExist(c, s, subtaskCfg1)
	t.workerExist(c, s, workerInfo1)
	// expect relay stage keep Running.
	t.relayStageMatch(c, s, sourceID1, pb.Stage_Running)
	t.subTaskStageMatch(c, s, taskName1, sourceID1, pb.Stage_Running)

	// CASE 3.2: start worker1 again.
	// do keep-alive for worker1 again.
	ctx1, cancel1 = context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Assert(ha.KeepAlive(ctx1, etcdTestCli, workerName1, keepAliveTTL), IsNil)
	}()
	// wait for source1 bound to worker1.
	utils.WaitSomething(30, 10*time.Millisecond, func() bool {
		bounds := s.BoundSources()
		return len(bounds) == 1 && bounds[0] == sourceID1
	})
	// source1 bound to worker1.
	t.sourceBounds(c, s, []string{sourceID1}, []string{})
	t.workerBound(c, s, ha.NewSourceBound(sourceID1, workerName1))
	// expect stages keep Running.
	t.relayStageMatch(c, s, sourceID1, pb.Stage_Running)
	t.subTaskStageMatch(c, s, taskName1, sourceID1, pb.Stage_Running)

	// shutdown the scheduler.
	s.Close()

	// CASE 4: start again with previous `Bound` worker, relay stage, subtask stage.
	c.Assert(s.Start(ctx, etcdTestCli), IsNil)

	// CASE 4.1: previous information should recover.
	// source1 is still bound.
	t.sourceBounds(c, s, []string{sourceID1}, []string{})
	// worker1 still exists, and it's bound.
	t.workerBound(c, s, ha.NewSourceBound(sourceID1, workerName1))
	// static information are still there.
	t.sourceCfgExist(c, s, sourceCfg1)
	t.subTaskCfgExist(c, s, subtaskCfg1)
	t.workerExist(c, s, workerInfo1)
	// expect stages keep Running.
	t.relayStageMatch(c, s, sourceID1, pb.Stage_Running)
	t.subTaskStageMatch(c, s, taskName1, sourceID1, pb.Stage_Running)

	// CASE 4.2: add another worker into the cluster.
	// worker2 not exists before added.
	t.workerNotExist(c, s, workerName2)
	// add worker2.
	c.Assert(s.AddWorker(workerName2, workerAddr2), IsNil)
	// the worker added, but is offline.
	t.workerExist(c, s, workerInfo2)
	t.workerOffline(c, s, workerName2)

	// CASE 4.3: the worker2 become online.
	// do keep-alive for worker2.
	ctx2, cancel2 := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Assert(ha.KeepAlive(ctx2, etcdTestCli, workerName2, keepAliveTTL), IsNil)
	}()
	// wait for worker2 become Free.
	utils.WaitSomething(30, 10*time.Millisecond, func() bool {
		w := s.GetWorkerByName(workerName2)
		return w.Stage() == WorkerFree
	})
	t.workerFree(c, s, workerName2)

	// CASE 4.4: add source config2.
	// source2 not exists before.
	t.sourceCfgNotExist(c, s, sourceID2)
	// add source2.
	c.Assert(s.AddSourceCfg(sourceCfg2), IsNil)
	// source2 added.
	t.sourceCfgExist(c, s, sourceCfg2)
	// source2 should bound to worker2.
	t.workerBound(c, s, ha.NewSourceBound(sourceID2, workerName2))
	t.sourceBounds(c, s, []string{sourceID1, sourceID2}, []string{})
	t.relayStageMatch(c, s, sourceID2, pb.Stage_Running)

	// CASE 4.4: start a task with two sources.
	// can't add more than one tasks at a time now.
	c.Assert(terror.ErrSchedulerMultiTask.Equal(s.AddSubTasks(subtaskCfg1, subtaskCfg21)), IsTrue)
	// task2' config and stage not exists before.
	t.subTaskCfgNotExist(c, s, taskName2, sourceID1)
	t.subTaskCfgNotExist(c, s, taskName2, sourceID2)
	t.subTaskStageMatch(c, s, taskName2, sourceID1, pb.Stage_InvalidStage)
	t.subTaskStageMatch(c, s, taskName2, sourceID2, pb.Stage_InvalidStage)
	// start task2.
	c.Assert(s.AddSubTasks(subtaskCfg21, subtaskCfg22), IsNil)
	// config added, stage become Running.
	t.subTaskCfgExist(c, s, subtaskCfg21)
	t.subTaskCfgExist(c, s, subtaskCfg22)
	t.subTaskStageMatch(c, s, taskName2, sourceID1, pb.Stage_Running)
	t.subTaskStageMatch(c, s, taskName2, sourceID2, pb.Stage_Running)

	// CASE 4.4.1 fail to stop any task.
	// can call without tasks or sources, return without error, but take no effect.
	c.Assert(s.RemoveSubTasks("", sourceID1), IsNil)
	c.Assert(s.RemoveSubTasks(taskName1), IsNil)
	// stop not exist task.
	c.Assert(terror.ErrSchedulerSubTaskOpTaskNotExist.Equal(s.RemoveSubTasks("not-exist", sourceID1)), IsTrue)
	// config and stage not changed.
	t.subTaskCfgExist(c, s, subtaskCfg21)
	t.subTaskCfgExist(c, s, subtaskCfg22)
	t.subTaskStageMatch(c, s, taskName2, sourceID1, pb.Stage_Running)
	t.subTaskStageMatch(c, s, taskName2, sourceID2, pb.Stage_Running)

	// CASE 4.5: update subtasks stage from different current stage.
	// pause <task2, source1>.
	c.Assert(s.UpdateExpectSubTaskStage(pb.Stage_Paused, taskName2, sourceID1), IsNil)
	t.subTaskStageMatch(c, s, taskName2, sourceID1, pb.Stage_Paused)
	t.subTaskStageMatch(c, s, taskName2, sourceID2, pb.Stage_Running)
	// resume <task2, source1 and source2>.
	c.Assert(s.UpdateExpectSubTaskStage(pb.Stage_Running, taskName2, sourceID1, sourceID2), IsNil)
	t.subTaskStageMatch(c, s, taskName2, sourceID1, pb.Stage_Running)
	t.subTaskStageMatch(c, s, taskName2, sourceID2, pb.Stage_Running)

	// CASE 4.6: try remove source when subtasks exist.
	c.Assert(terror.ErrSchedulerSourceOpTaskExist.Equal(s.RemoveSourceCfg(sourceID2)), IsTrue)
	// source2 keep there.
	t.sourceCfgExist(c, s, sourceCfg2)
	// source2 still bound to worker2.
	t.workerBound(c, s, ha.NewSourceBound(sourceID2, workerName2))
	t.sourceBounds(c, s, []string{sourceID1, sourceID2}, []string{})
	t.relayStageMatch(c, s, sourceID2, pb.Stage_Running)

	// CASE 4.7: stop task2.
	c.Assert(s.RemoveSubTasks(taskName2, sourceID1, sourceID2), IsNil)
	t.subTaskCfgNotExist(c, s, taskName2, sourceID1)
	t.subTaskCfgNotExist(c, s, taskName2, sourceID2)
	t.subTaskStageMatch(c, s, taskName2, sourceID1, pb.Stage_InvalidStage)
	t.subTaskStageMatch(c, s, taskName2, sourceID2, pb.Stage_InvalidStage)

	// CASE 4.7: remove source2.
	c.Assert(s.RemoveSourceCfg(sourceID2), IsNil)
	c.Assert(terror.ErrSchedulerSourceCfgNotExist.Equal(s.RemoveSourceCfg(sourceID2)), IsTrue) // already removed.
	// source2 removed.
	t.sourceCfgNotExist(c, s, sourceID2)
	// worker2 become Free now.
	t.workerFree(c, s, workerName2)
	t.sourceBounds(c, s, []string{sourceID1}, []string{})
	t.relayStageMatch(c, s, sourceID2, pb.Stage_InvalidStage)

	// CASE 4.8: worker1 become offline.
	// before shutdown, worker1 bound source
	t.workerBound(c, s, ha.NewSourceBound(sourceID1, workerName1))
	// cancel keep-alive.
	cancel1()
	// wait for worker1 become offline.
	utils.WaitSomething(int(3*keepAliveTTL), time.Second, func() bool {
		w := s.GetWorkerByName(workerName1)
		c.Assert(w, NotNil)
		return w.Stage() == WorkerOffline
	})
	t.workerOffline(c, s, workerName1)
	// source1 should bound to worker2.
	t.sourceBounds(c, s, []string{sourceID1}, []string{})
	t.workerBound(c, s, ha.NewSourceBound(sourceID1, workerName2))
	// expect stages keep Running.
	t.relayStageMatch(c, s, sourceID1, pb.Stage_Running)
	t.subTaskStageMatch(c, s, taskName1, sourceID1, pb.Stage_Running)

	// CASE 4.9: remove worker1.
	c.Assert(s.RemoveWorker(workerName1), IsNil)
	c.Assert(terror.ErrSchedulerWorkerNotExist.Equal(s.RemoveWorker(workerName1)), IsTrue) // can't remove multiple times.
	// worker1 not exists now.
	t.workerNotExist(c, s, workerName1)

	// CASE 4.10: stop task1.
	c.Assert(s.RemoveSubTasks(taskName1, sourceID1), IsNil)
	t.subTaskCfgNotExist(c, s, taskName1, sourceID1)
	t.subTaskStageMatch(c, s, taskName1, sourceID1, pb.Stage_InvalidStage)

	// CASE 4.11: remove worker not supported when the worker is online.
	c.Assert(terror.ErrSchedulerWorkerOnline.Equal(s.RemoveWorker(workerName2)), IsTrue)
	t.sourceBounds(c, s, []string{sourceID1}, []string{})
	t.workerBound(c, s, ha.NewSourceBound(sourceID1, workerName2))
	t.relayStageMatch(c, s, sourceID1, pb.Stage_Running)

	// CASE 4.12: worker2 become offline.
	cancel2()
	wg.Wait()
	// wait for worker2 become offline.
	utils.WaitSomething(int(3*keepAliveTTL), time.Second, func() bool {
		w := s.GetWorkerByName(workerName2)
		c.Assert(w, NotNil)
		return w.Stage() == WorkerOffline
	})
	t.workerOffline(c, s, workerName2)
	// source1 should unbound
	t.sourceBounds(c, s, []string{}, []string{sourceID1})
	// expect stages keep Running.
	t.relayStageMatch(c, s, sourceID1, pb.Stage_Running)

	// CASE 4.13: remove worker2.
	c.Assert(s.RemoveWorker(workerName2), IsNil)
	t.workerNotExist(c, s, workerName2)
	// relay stage still there.
	t.sourceBounds(c, s, []string{}, []string{sourceID1})
	t.relayStageMatch(c, s, sourceID1, pb.Stage_Running)

	// CASE 4.14: remove source1.
	c.Assert(s.RemoveSourceCfg(sourceID1), IsNil)
	t.sourceCfgNotExist(c, s, sourceID1)
	t.sourceBounds(c, s, []string{}, []string{})
	t.relayStageMatch(c, s, sourceID1, pb.Stage_InvalidStage)
}

func (t *testScheduler) sourceCfgNotExist(c *C, s *Scheduler, source string) {
	c.Assert(s.GetSourceCfgByID(source), IsNil)
	cfg, _, err := ha.GetSourceCfg(etcdTestCli, source, 0)
	c.Assert(err, IsNil)
	c.Assert(cfg, DeepEquals, sourceCfgEmpty)
}

func (t *testScheduler) sourceCfgExist(c *C, s *Scheduler, expectCfg config.SourceConfig) {
	cfgP := s.GetSourceCfgByID(expectCfg.SourceID)
	c.Assert(cfgP, DeepEquals, &expectCfg)
	cfgV, _, err := ha.GetSourceCfg(etcdTestCli, expectCfg.SourceID, 0)
	c.Assert(err, IsNil)
	c.Assert(cfgV, DeepEquals, expectCfg)
}

func (t *testScheduler) subTaskCfgNotExist(c *C, s *Scheduler, task, source string) {
	c.Assert(s.GetSubTaskCfgByTaskSource(task, source), IsNil)
	cfgM, _, err := ha.GetSubTaskCfg(etcdTestCli, source, task, 0)
	c.Assert(err, IsNil)
	c.Assert(cfgM, HasLen, 0)
}

func (t *testScheduler) subTaskCfgExist(c *C, s *Scheduler, expectCfg config.SubTaskConfig) {
	cfgP := s.GetSubTaskCfgByTaskSource(expectCfg.Name, expectCfg.SourceID)
	c.Assert(cfgP, DeepEquals, &expectCfg)
	cfgM, _, err := ha.GetSubTaskCfg(etcdTestCli, expectCfg.SourceID, expectCfg.Name, 0)
	c.Assert(err, IsNil)
	c.Assert(cfgM, HasLen, 1)
	c.Assert(cfgM[expectCfg.Name], DeepEquals, expectCfg)
}

func (t *testScheduler) workerNotExist(c *C, s *Scheduler, worker string) {
	c.Assert(s.GetWorkerByName(worker), IsNil)
	wm, _, err := ha.GetAllWorkerInfo(etcdTestCli)
	c.Assert(err, IsNil)
	_, ok := wm[worker]
	c.Assert(ok, IsFalse)
}

func (t *testScheduler) workerExist(c *C, s *Scheduler, info ha.WorkerInfo) {
	c.Assert(s.GetWorkerByName(info.Name), NotNil)
	c.Assert(s.GetWorkerByName(info.Name).BaseInfo(), DeepEquals, info)
	wm, _, err := ha.GetAllWorkerInfo(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(wm[info.Name], DeepEquals, info)
}

func (t *testScheduler) workerOffline(c *C, s *Scheduler, worker string) {
	w := s.GetWorkerByName(worker)
	c.Assert(w, NotNil)
	c.Assert(w.Bound(), DeepEquals, nullBound)
	c.Assert(w.Stage(), Equals, WorkerOffline)
	wm, _, err := ha.GetAllWorkerInfo(etcdTestCli)
	c.Assert(err, IsNil)
	_, ok := wm[worker]
	c.Assert(ok, IsTrue)
	sbm, _, err := ha.GetSourceBound(etcdTestCli, worker)
	c.Assert(err, IsNil)
	_, ok = sbm[worker]
	c.Assert(ok, IsFalse)
}

func (t *testScheduler) workerFree(c *C, s *Scheduler, worker string) {
	w := s.GetWorkerByName(worker)
	c.Assert(w, NotNil)
	c.Assert(w.Bound(), DeepEquals, nullBound)
	c.Assert(w.Stage(), Equals, WorkerFree)
	wm, _, err := ha.GetAllWorkerInfo(etcdTestCli)
	c.Assert(err, IsNil)
	_, ok := wm[worker]
	c.Assert(ok, IsTrue)
	sbm, _, err := ha.GetSourceBound(etcdTestCli, worker)
	c.Assert(err, IsNil)
	_, ok = sbm[worker]
	c.Assert(ok, IsFalse)
}

func (t *testScheduler) workerBound(c *C, s *Scheduler, bound ha.SourceBound) {
	w := s.GetWorkerByName(bound.Worker)
	c.Assert(w, NotNil)
	c.Assert(w.Bound(), DeepEquals, bound)
	c.Assert(w.Stage(), Equals, WorkerBound)
	wm, _, err := ha.GetAllWorkerInfo(etcdTestCli)
	c.Assert(err, IsNil)
	_, ok := wm[bound.Worker]
	c.Assert(ok, IsTrue)
	sbm, _, err := ha.GetSourceBound(etcdTestCli, bound.Worker)
	c.Assert(err, IsNil)
	c.Assert(sbm[bound.Worker], DeepEquals, bound)
}

func (t *testScheduler) sourceBounds(c *C, s *Scheduler, expectBounds, expectUnbounds []string) {
	c.Assert(s.BoundSources(), DeepEquals, expectBounds)
	c.Assert(s.UnboundSources(), DeepEquals, expectUnbounds)

	wToB, _, err := ha.GetSourceBound(etcdTestCli, "")
	c.Assert(err, IsNil)
	c.Assert(wToB, HasLen, len(expectBounds))

	sToB := make(map[string]ha.SourceBound, len(wToB))
	for _, b := range wToB {
		sToB[b.Source] = b
	}
	for _, source := range expectBounds {
		c.Assert(sToB[source], NotNil)
		c.Assert(s.GetWorkerBySource(source), NotNil)
		c.Assert(s.GetWorkerBySource(source).Stage(), Equals, WorkerBound)
		c.Assert(sToB[source], DeepEquals, s.GetWorkerBySource(source).Bound())
	}

	for _, source := range expectUnbounds {
		c.Assert(s.GetWorkerBySource(source), IsNil)
	}
}

func (t *testScheduler) relayStageMatch(c *C, s *Scheduler, source string, expectStage pb.Stage) {
	stage := ha.NewRelayStage(expectStage, source)
	c.Assert(s.GetExpectRelayStage(source), DeepEquals, stage)

	eStage, _, err := ha.GetRelayStage(etcdTestCli, source)
	c.Assert(err, IsNil)
	switch expectStage {
	case pb.Stage_Running, pb.Stage_Paused:
		c.Assert(eStage, DeepEquals, stage)
	default:
		c.Assert(eStage, DeepEquals, stageEmpty)
	}
}

func (t *testScheduler) subTaskStageMatch(c *C, s *Scheduler, task, source string, expectStage pb.Stage) {
	stage := ha.NewSubTaskStage(expectStage, source, task)
	c.Assert(s.GetExpectSubTaskStage(task, source), DeepEquals, stage)

	eStageM, _, err := ha.GetSubTaskStage(etcdTestCli, source, task)
	c.Assert(err, IsNil)
	switch expectStage {
	case pb.Stage_Running, pb.Stage_Paused:
		c.Assert(eStageM, HasLen, 1)
		c.Assert(eStageM[task], DeepEquals, stage)
	default:
		c.Assert(eStageM, HasLen, 0)
	}
}

func (t *testScheduler) TestRestartScheduler(c *C) {
	defer clearTestInfoOperation(c)

	var (
		logger       = log.L()
		sourceID1    = "mysql-replica-1"
		workerName1  = "dm-worker-1"
		workerAddr1  = "127.0.0.1:8262"
		workerInfo1  = ha.NewWorkerInfo(workerName1, workerAddr1)
		sourceBound1 = ha.NewSourceBound(sourceID1, workerName1)
		sourceCfg1   config.SourceConfig
		wg           sync.WaitGroup
		keepAliveTTL = int64(1) // NOTE: this should be >= minLeaseTTL, in second.
	)
	c.Assert(sourceCfg1.LoadFromFile(sourceSampleFile), IsNil)
	sourceCfg1.SourceID = sourceID1

	s := NewScheduler(&logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// step 1: start scheduler
	c.Assert(s.Start(ctx, etcdTestCli), IsNil)
	// step 1.1: add sourceCfg and worker
	c.Assert(s.AddSourceCfg(sourceCfg1), IsNil)
	t.sourceCfgExist(c, s, sourceCfg1)
	c.Assert(s.AddWorker(workerName1, workerAddr1), IsNil)
	t.workerExist(c, s, workerInfo1)
	// step 2: start a worker
	// step 2.1: worker start watching source bound
	bsm, revBound, err := ha.GetSourceBound(etcdTestCli, workerName1)
	c.Assert(err, IsNil)
	c.Assert(bsm, HasLen, 0)
	sourceBoundCh := make(chan ha.SourceBound, 10)
	sourceBoundErrCh := make(chan error, 10)
	go func() {
		ha.WatchSourceBound(ctx, etcdTestCli, workerName1, revBound+1, sourceBoundCh, sourceBoundErrCh)
	}()
	// step 2.2: worker start keepAlive
	ctx1, cancel1 := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Assert(ha.KeepAlive(ctx1, etcdTestCli, workerName1, keepAliveTTL), IsNil)
	}()
	// step 2.3: scheduler should bound source to worker
	// wait for source1 bound to worker1.
	utils.WaitSomething(30, 10*time.Millisecond, func() bool {
		bounds := s.BoundSources()
		return len(bounds) == 1 && bounds[0] == sourceID1
	})
	checkSourceBoundCh := func() {
		time.Sleep(300 * time.Millisecond)
		c.Assert(sourceBoundCh, HasLen, 1)
		sourceBound := <-sourceBoundCh
		sourceBound.Revision = 0
		c.Assert(sourceBound, DeepEquals, sourceBound1)
		c.Assert(sourceBoundErrCh, HasLen, 0)
	}
	// worker should receive a put sourceBound event
	checkSourceBoundCh()
	// case 1: scheduler restarted, and worker keepalive brock but re-setup before scheduler is started
	// step 3: restart scheduler, but don't stop worker keepalive, which can simulate two situations:
	//			a. worker keepalive breaks but re-setup again before scheduler is started
	//			b. worker is restarted but re-setup keepalive before scheduler is started
	// dm-worker will close its source when keepalive is broken, so scheduler will send an event
	// to trigger it to restart the source again
	s.Close()
	c.Assert(sourceBoundCh, HasLen, 0)
	c.Assert(s.Start(ctx, etcdTestCli), IsNil)
	// worker should receive the trigger event again
	checkSourceBoundCh()
	// case 2: worker is restarted, and worker keepalive broke but scheduler didn't catch the delete event
	// step 4: restart worker keepalive, which can simulator one situation:
	//			a. worker keepalive breaks but re-setup again before keepaliveTTL is timeout
	c.Assert(sourceBoundCh, HasLen, 0)
	ctx2, cancel2 := context.WithCancel(ctx)
	// trigger same keepalive event again, just for test
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Assert(ha.KeepAlive(ctx2, etcdTestCli, workerName1, keepAliveTTL), IsNil)
	}()
	checkSourceBoundCh()
	// case 3: scheduler is restarted, but worker also broke after scheduler is down
	// step 5: stop scheduler -> stop worker keepalive -> restart scheduler
	//		   scheduler should unbound the source and update the bound info in etcd
	s.Close() // stop scheduler
	cancel1()
	cancel2() // stop worker keepalive
	wg.Wait()
	// check whether keepalive lease is out of date
	time.Sleep(time.Duration(keepAliveTTL) * time.Second)
	c.Assert(utils.WaitSomething(30, 10*time.Millisecond, func() bool {
		kam, _, err := ha.GetKeepAliveWorkers(etcdTestCli)
		return err == nil && len(kam) == 0
	}), IsTrue)
	c.Assert(sourceBoundCh, HasLen, 0)
	c.Assert(s.Start(ctx, etcdTestCli), IsNil) // restart scheduler
	c.Assert(s.BoundSources(), HasLen, 0)
	unbounds := s.UnboundSources()
	c.Assert(unbounds, HasLen, 1)
	c.Assert(unbounds[0], Equals, sourceID1)
	sourceBound1.Source = ""
	sourceBound1.IsDeleted = true
	checkSourceBoundCh()
}
