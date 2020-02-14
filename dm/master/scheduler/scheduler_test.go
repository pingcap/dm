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
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
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
		logger = log.L()
		s      = NewScheduler(&logger)
		wn1    = "dm-worker-1"
		wa1    = "127.0.0.1:8262"
	)

	// not started scheduler can't do anything.
	c.Assert(terror.ErrSchedulerNotStarted.Equal(s.AddWorker(wn1, wa1)), IsTrue)
	c.Assert(terror.ErrSchedulerNotStarted.Equal(s.RemoveWorker(wn1)), IsTrue)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// CASE 1: start without any previous info.
	c.Assert(s.Start(ctx, etcdTestCli), IsNil)
	c.Assert(terror.ErrSchedulerStarted.Equal(s.Start(ctx, etcdTestCli)), IsTrue) // start multiple times.
	s.Close()
	s.Close() // close multiple times.

	// CASE 2: start again without any previous info.
	c.Assert(s.Start(ctx, etcdTestCli), IsNil)

	// add/register a worker.
	c.Assert(s.AddWorker(wn1, wa1), IsNil)
	c.Assert(terror.ErrSchedulerWorkerExist.Equal(s.AddWorker(wn1, wa1)), IsTrue) // can't add multiple times.

	// remove/unregister the worker.
	c.Assert(s.RemoveWorker(wn1), IsNil)
	c.Assert(terror.ErrSchedulerWorkerNotExist.Equal(s.RemoveWorker(wn1)), IsTrue) // not exists.
}
