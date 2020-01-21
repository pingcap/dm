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

package shardddl

import (
	"context"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/integration"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/shardddl/pessimism"
	"github.com/pingcap/dm/pkg/utils"
)

var (
	etcdTestCli *clientv3.Client
)

type testPessimist struct{}

var _ = Suite(&testPessimist{})

func TestPessimist(t *testing.T) {
	log.InitLogger(&log.Config{})

	mockCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer mockCluster.Terminate(t)

	etcdTestCli = mockCluster.RandClient()

	TestingT(t)
}

// clear keys in etcd test cluster.
func clearTestInfoOperation(c *C) {
	clearInfo := clientv3.OpDelete(common.ShardDDLPessimismInfoKeyAdapter.Path(), clientv3.WithPrefix())
	clearOp := clientv3.OpDelete(common.ShardDDLPessimismOperationKeyAdapter.Path(), clientv3.WithPrefix())
	_, err := etcdTestCli.Txn(context.Background()).Then(clearInfo, clearOp).Commit()
	c.Assert(err, IsNil)
}

func (t *testPessimist) TestPessimist(c *C) {
	defer clearTestInfoOperation(c)

	var (
		task1         = "task-1"
		task2         = "task-2"
		source1       = "mysql-replica-1"
		source2       = "mysql-replica-2"
		source3       = "mysql-replica-3"
		schema, table = "foo", "bar"
		DDLs          = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		ID1           = "task-1-`foo`.`bar`"
		i11           = pessimism.NewInfo(task1, source1, schema, table, DDLs)
		i12           = pessimism.NewInfo(task1, source2, schema, table, DDLs)

		sources = func(task string) []string {
			switch task {
			case task1:
				return []string{source1, source2}
			case task2:
				return []string{source1, source2, source3}
			default:
				c.Fatalf("unsupported task %s", task)
			}
			return []string{}
		}
		logger = log.L()
		p      = NewPessimist(&logger, sources)
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// CASE 1: start without any previous kv and no etcd operation.
	c.Assert(p.Start(ctx, etcdTestCli), IsNil)
	c.Assert(p.Locks(), HasLen, 0)
	p.Close()
	p.Close() // close multiple times.

	// CASE 2: start again without any previous kv.
	c.Assert(p.Start(ctx, etcdTestCli), IsNil)
	c.Assert(p.Locks(), HasLen, 0)

	// PUT i11, will create a lock but not synced.
	_, err := pessimism.PutInfo(etcdTestCli, i11)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 10*time.Millisecond, func() bool {
		return len(p.Locks()) == 1
	}), IsTrue)
	c.Assert(p.Locks(), HasKey, ID1)
	synced, remain := p.Locks()[ID1].IsSynced()
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 1)

	// PUT i12, the lock will be synced, then an operation PUT for the owner will be triggered.
	rev1, err := pessimism.PutInfo(etcdTestCli, i12)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 10*time.Millisecond, func() bool {
		synced, _ = p.Locks()[ID1].IsSynced()
		return synced
	}), IsTrue)

	// wait exec operation for the owner become available.
	opCh := make(chan pessimism.Operation, 10)
	ctx2, cancel2 := context.WithTimeout(ctx, 100*time.Millisecond)
	pessimism.WatchOperationPut(ctx2, etcdTestCli, task1, source1, rev1, opCh)
	cancel2()
	close(opCh)
	c.Assert(len(opCh), Equals, 1)
	op11 := <-opCh
	c.Assert(op11.Exec, IsTrue)
	c.Assert(op11.Done, IsFalse)
}
