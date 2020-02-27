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
		watchTimeout  = 500 * time.Millisecond
		task1         = "task-1"
		task2         = "task-2"
		source1       = "mysql-replica-1"
		source2       = "mysql-replica-2"
		source3       = "mysql-replica-3"
		schema, table = "foo", "bar"
		DDLs          = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		ID1           = "task-1-`foo`.`bar`"
		ID2           = "task-2-`foo`.`bar`"
		i11           = pessimism.NewInfo(task1, source1, schema, table, DDLs)
		i12           = pessimism.NewInfo(task1, source2, schema, table, DDLs)
		i21           = pessimism.NewInfo(task2, source1, schema, table, DDLs)
		i22           = pessimism.NewInfo(task2, source2, schema, table, DDLs)
		i23           = pessimism.NewInfo(task2, source3, schema, table, DDLs)

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
	ctx2, cancel2 := context.WithTimeout(ctx, watchTimeout)
	pessimism.WatchOperationPut(ctx2, etcdTestCli, task1, source1, rev1, opCh)
	cancel2()
	close(opCh)
	c.Assert(len(opCh), Equals, 1)
	op11 := <-opCh
	c.Assert(op11.Exec, IsTrue)
	c.Assert(op11.Done, IsFalse)

	// mark exec operation for the owner as `done` (and delete the info).
	op11c := op11
	op11c.Done = true
	rev2, err := pessimism.PutOperationDeleteInfo(etcdTestCli, op11c, i11)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 10*time.Millisecond, func() bool {
		return p.Locks()[ID1].IsDone(source1)
	}), IsTrue)

	// wait skip operation for the non-owner become available.
	opCh = make(chan pessimism.Operation, 10)
	ctx2, cancel2 = context.WithTimeout(ctx, watchTimeout)
	pessimism.WatchOperationPut(ctx2, etcdTestCli, task1, source2, rev2, opCh)
	cancel2()
	close(opCh)
	c.Assert(len(opCh), Equals, 1)
	op12 := <-opCh
	c.Assert(op12.Exec, IsFalse)
	c.Assert(op12.Done, IsFalse)

	// mark skip operation for the non-owner as `done` (and delete the info).
	// the lock should become resolved and deleted.
	op12c := op12
	op12c.Done = true
	_, err = pessimism.PutOperationDeleteInfo(etcdTestCli, op12c, i12)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 10*time.Millisecond, func() bool {
		_, ok := p.Locks()[ID1]
		return !ok
	}), IsTrue)
	c.Assert(p.Locks(), HasLen, 0)

	// PUT i21, i22, this will create a lock.
	_, err = pessimism.PutInfo(etcdTestCli, i21)
	c.Assert(err, IsNil)
	_, err = pessimism.PutInfo(etcdTestCli, i22)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 10*time.Millisecond, func() bool {
		lock := p.Locks()[ID2]
		if lock == nil {
			return false
		}
		_, remain = lock.IsSynced()
		return remain == 1
	}), IsTrue)

	p.Close() // close the Pessimist.

	// CASE 3: start again with some previous shard DDL info and the lock is un-synced.
	c.Assert(p.Start(ctx, etcdTestCli), IsNil)
	c.Assert(p.Locks(), HasLen, 1)
	c.Assert(p.Locks(), HasKey, ID2)
	synced, remain = p.Locks()[ID2].IsSynced()
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 1)

	// PUT i23, then the lock will become synced.
	rev3, err := pessimism.PutInfo(etcdTestCli, i23)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 10*time.Millisecond, func() bool {
		synced, _ = p.Locks()[ID2].IsSynced()
		return synced
	}), IsTrue)

	// wait exec operation for the owner become available.
	opCh = make(chan pessimism.Operation, 10)
	ctx2, cancel2 = context.WithTimeout(ctx, watchTimeout)
	// both source1 and source2 have shard DDL info exist, and neither of them have operation exist.
	// we must ensure source1 always become the owner of the lock.
	pessimism.WatchOperationPut(ctx2, etcdTestCli, task2, source1, rev3, opCh)
	cancel2()
	close(opCh)
	c.Logf("watch operation PUT with revision %d", rev3)
	c.Assert(len(opCh), Equals, 1)
	op21 := <-opCh
	c.Assert(op21.Exec, IsTrue)
	c.Assert(op21.Done, IsFalse)

	p.Close() // close the Pessimist.

	// CASE 4: start again with some previous shard DDL info and non-`done` operation.
	c.Assert(p.Start(ctx, etcdTestCli), IsNil)
	c.Assert(p.Locks(), HasLen, 1)
	c.Assert(p.Locks(), HasKey, ID2)
	synced, _ = p.Locks()[ID2].IsSynced()
	c.Assert(synced, IsTrue)
	c.Assert(p.Locks()[ID2].IsDone(source1), IsFalse)

	// mark exec operation for the owner as `done` (and delete the info).
	op21c := op21
	op21c.Done = true
	_, err = pessimism.PutOperationDeleteInfo(etcdTestCli, op21c, i21)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return p.Locks()[ID2].IsDone(source1)
	}), IsTrue)

	p.Close() // close the Pessimist.

	// CASE 5: start again with some previous shard DDL info and `done` operation for the owner.
	c.Assert(p.Start(ctx, etcdTestCli), IsNil)
	c.Assert(p.Locks(), HasLen, 1)
	c.Assert(p.Locks(), HasKey, ID2)
	synced, _ = p.Locks()[ID2].IsSynced()
	c.Assert(synced, IsTrue)
	c.Assert(p.Locks()[ID2].IsDone(source1), IsTrue)
	c.Assert(p.Locks()[ID2].IsDone(source2), IsFalse)

	// mark exec operation for one non-owner as `done` (and delete the info).
	op22c := pessimism.NewOperation(ID2, task2, source2, DDLs, false, true)
	_, err = pessimism.PutOperationDeleteInfo(etcdTestCli, op22c, i22)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 10*time.Millisecond, func() bool {
		return p.Locks()[ID2].IsDone(source2)
	}), IsTrue)

	p.Close() // close the Pessimist.

	// CASE 6: start again with some previous shard DDL info and `done` operation for the owner and non-owner.
	c.Assert(p.Start(ctx, etcdTestCli), IsNil)
	c.Assert(p.Locks(), HasLen, 1)
	c.Assert(p.Locks(), HasKey, ID2)
	synced, _ = p.Locks()[ID2].IsSynced()
	c.Assert(synced, IsTrue)
	c.Assert(p.Locks()[ID2].IsDone(source1), IsTrue)
	c.Assert(p.Locks()[ID2].IsDone(source2), IsTrue)
	c.Assert(p.Locks()[ID2].IsDone(source3), IsFalse)

	// mark skip operation for the non-owner as `done` (and delete the info).
	// the lock should become resolved and deleted.
	op23c := pessimism.NewOperation(ID2, task2, source3, DDLs, false, true)
	_, err = pessimism.PutOperationDeleteInfo(etcdTestCli, op23c, i23)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 10*time.Millisecond, func() bool {
		_, ok := p.Locks()[ID2]
		return !ok
	}), IsTrue)
	c.Assert(p.Locks(), HasLen, 0)

	p.Close() // close the Pessimist.

	// CASE 7: start again after all shard DDL locks have been resolved.
	c.Assert(p.Start(ctx, etcdTestCli), IsNil)
	c.Assert(p.Locks(), HasLen, 0)
	p.Close() // close the Pessimist.
}
