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

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/ha"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

const (
	// do not forget to update this path if the file removed/renamed.
	sourceSampleFile = "../../worker/dm-mysql.toml"
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
	clearSource := clientv3.OpDelete(common.UpstreamConfigKeyAdapter.Path(), clientv3.WithPrefix())
	clearSubTask := clientv3.OpDelete(common.UpstreamSubTaskKeyAdapter.Path(), clientv3.WithPrefix())
	clearWorkerInfo := clientv3.OpDelete(common.WorkerRegisterKeyAdapter.Path(), clientv3.WithPrefix())
	clearWorkerKeepAlive := clientv3.OpDelete(common.WorkerKeepAliveKeyAdapter.Path(), clientv3.WithPrefix())
	clearBound := clientv3.OpDelete(common.UpstreamBoundWorkerKeyAdapter.Path(), clientv3.WithPrefix())
	clearRelayStage := clientv3.OpDelete(common.StageRelayKeyAdapter.Path(), clientv3.WithPrefix())
	clearSubTaskStage := clientv3.OpDelete(common.StageSubTaskKeyAdapter.Path(), clientv3.WithPrefix())
	_, err := etcdTestCli.Txn(context.Background()).Then(
		clearSource, clearSubTask, clearWorkerInfo, clearBound, clearWorkerKeepAlive, clearRelayStage, clearSubTaskStage,
	).Commit()
	c.Assert(err, IsNil)
}

type testScheduler struct{}

var _ = Suite(&testScheduler{})

var (
	sourceCfgEmpty config.MysqlConfig
	stageEmpty     ha.Stage
)

func (t *testScheduler) TestScheduler(c *C) {
	defer clearTestInfoOperation(c)

	var (
		logger       = log.L()
		s            = NewScheduler(&logger)
		sourceID1    = "mysql-replica-1"
		workerName1  = "dm-worker-1"
		workerAddr1  = "127.0.0.1:8262"
		workerInfo1  = ha.NewWorkerInfo(workerName1, workerAddr1)
		sourceCfg1   config.MysqlConfig
		keepAliveTTL = int64(1) // NOTE: this should be >= minLeaseTTL, in second.
	)
	c.Assert(sourceCfg1.LoadFromFile(sourceSampleFile), IsNil)
	sourceCfg1.SourceID = sourceID1

	// not started scheduler can't do anything.
	c.Assert(terror.ErrSchedulerNotStarted.Equal(s.AddSourceCfg(sourceCfg1)), IsTrue)
	c.Assert(terror.ErrSchedulerNotStarted.Equal(s.RemoveSourceCfg(sourceID1)), IsTrue)
	c.Assert(terror.ErrSchedulerNotStarted.Equal(s.AddWorker(workerName1, workerAddr1)), IsTrue)
	c.Assert(terror.ErrSchedulerNotStarted.Equal(s.RemoveWorker(workerName1)), IsTrue)

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
	c.Assert(terror.ErrSchedulerWorkerExist.Equal(s.AddWorker(workerName1, workerAddr1)), IsTrue) // can't add multiple times.
	// the worker added.
	t.workerExist(c, s, workerInfo1)
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

	// start a task with only one source.

	// try start a task with two sources.

	// pause/resume task1.

	// shutdown worker1.

	// start worker1 again.

	// add worker2.

	// add source config2.

	// start a task with two sources.

	// stop task1.

	// CASE 2.x: remove worker not supported when the worker is online.
	c.Assert(terror.ErrSchedulerWorkerOnline.Equal(s.RemoveWorker(workerName1)), IsTrue)

	// CASE 2.x: the worker become offline.
	// cancel keep-alive.
	cancel1()
	wg.Wait()
	// wait for source1 unbound from worker1.
	utils.WaitSomething(int(3*keepAliveTTL), time.Second, func() bool {
		unbounds := s.UnboundSources()
		return len(unbounds) == 1 && unbounds[0] == sourceID1
	})
	t.sourceBounds(c, s, []string{}, []string{sourceID1})
	// worker1 still exists, but it's offline.
	t.workerOffline(c, s, workerName1)
	// expect relay stage keep Running.
	t.relayStageMatch(c, s, sourceID1, pb.Stage_Running)

	// shutdown and offline worker1.

	// stop task2.

	// shutdown and offline worker2.

	// remove/unregister the worker.
	//c.Assert(s.RemoveWorker(workerName1), IsNil)
	//c.Assert(terror.ErrSchedulerWorkerNotExist.Equal(s.RemoveWorker(workerName1)), IsTrue) // not exists.

	// bound source1 from worker1 to worker2.
}

func (t *testScheduler) sourceCfgNotExist(c *C, s *Scheduler, source string) {
	c.Assert(s.GetSourceCfgByID(source), IsNil)
	cfg, _, err := ha.GetSourceCfg(etcdTestCli, source, 0)
	c.Assert(err, IsNil)
	c.Assert(cfg, DeepEquals, sourceCfgEmpty)
}

func (t *testScheduler) sourceCfgExist(c *C, s *Scheduler, expectCfg config.MysqlConfig) {
	cfgP := s.GetSourceCfgByID(expectCfg.SourceID)
	c.Assert(cfgP, NotNil)
	c.Assert(cfgP, DeepEquals, &expectCfg)
	cfgV, _, err := ha.GetSourceCfg(etcdTestCli, expectCfg.SourceID, 0)
	c.Assert(err, IsNil)
	c.Assert(cfgV, DeepEquals, expectCfg)
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
}

func (t *testScheduler) workerBound(c *C, s *Scheduler, bound ha.SourceBound) {
	w := s.GetWorkerByName(bound.Worker)
	c.Assert(w, NotNil)
	c.Assert(w.Bound(), DeepEquals, bound)
	c.Assert(w.Stage(), Equals, WorkerBound)
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
