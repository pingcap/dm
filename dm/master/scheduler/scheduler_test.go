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
	"testing"

	. "github.com/pingcap/check"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/integration"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/ha"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
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

func (t *testScheduler) TestScheduler(c *C) {
	defer clearTestInfoOperation(c)

	var (
		logger         = log.L()
		s              = NewScheduler(&logger)
		sourceID1      = "mysql-replica-1"
		workerName1    = "dm-worker-1"
		workerAddr1    = "127.0.0.1:8262"
		workerInfo1    = ha.NewWorkerInfo(workerName1, workerAddr1)
		sourceCfg1     config.MysqlConfig
		sourceCfgEmpty config.MysqlConfig
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
	c.Assert(s.GetSourceCfgByID(sourceID1), IsNil)
	sourceCfgV, _, err := ha.GetSourceCfg(etcdTestCli, sourceID1, 0)
	c.Assert(err, IsNil)
	c.Assert(sourceCfgV, DeepEquals, sourceCfgEmpty)
	// add source config1.
	c.Assert(s.AddSourceCfg(sourceCfg1), IsNil)
	c.Assert(terror.ErrSchedulerSourceCfgExist.Equal(s.AddSourceCfg(sourceCfg1)), IsTrue) // can't add multiple times.
	// the source config added.
	sourceCfgP := s.GetSourceCfgByID(sourceID1)
	c.Assert(sourceCfgP, DeepEquals, &sourceCfg1)
	sourceCfgV, _, err = ha.GetSourceCfg(etcdTestCli, sourceID1, 0)
	c.Assert(err, IsNil)
	c.Assert(sourceCfgV, DeepEquals, sourceCfg1)
	// one unbound source exist (because no free worker).
	c.Assert(s.BoundSources(), HasLen, 0)
	unbounds := s.UnboundSources()
	c.Assert(unbounds, HasLen, 1)
	c.Assert(unbounds[0], Equals, sourceID1)
	sourceBoundM, _, err := ha.GetSourceBound(etcdTestCli, "")
	c.Assert(err, IsNil)
	c.Assert(sourceBoundM, HasLen, 0)

	// CASE 2.2: add the first worker.
	// no worker exist before added.
	c.Assert(s.GetWorkerByName(workerName1), IsNil)
	workerM, _, err := ha.GetAllWorkerInfo(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(workerM, HasLen, 0)
	// add worker1.
	c.Assert(s.AddWorker(workerName1, workerAddr1), IsNil)
	c.Assert(terror.ErrSchedulerWorkerExist.Equal(s.AddWorker(workerName1, workerAddr1)), IsTrue) // can't add multiple times.
	// the worker added.
	c.Assert(s.GetWorkerByName(workerName1), NotNil)
	c.Assert(s.GetWorkerByName(workerName1).BaseInfo(), DeepEquals, workerInfo1)
	workerM, _, err = ha.GetAllWorkerInfo(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(workerM, HasLen, 1)
	c.Assert(workerM[workerName1], DeepEquals, workerInfo1)
	// still no bounds (because the worker is offline).
	c.Assert(s.BoundSources(), HasLen, 0)
	unbounds = s.UnboundSources()
	c.Assert(unbounds, HasLen, 1)
	c.Assert(unbounds[0], Equals, sourceID1)

	// start a task with only one source.

	// try start a task with two sources.

	// pause/resume task1.

	// shutdown worker1.

	// start worker1 again.

	// add worker2.

	// add source config2.

	// start a task with two sources.

	// stop task1.

	// shutdown and offline worker1.

	// stop task2.

	// shutdown and offline worker2.

	// remove/unregister the worker.
	c.Assert(s.RemoveWorker(workerName1), IsNil)
	c.Assert(terror.ErrSchedulerWorkerNotExist.Equal(s.RemoveWorker(workerName1)), IsTrue) // not exists.
}
