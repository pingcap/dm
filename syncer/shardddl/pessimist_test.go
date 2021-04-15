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
	"github.com/pingcap/dm/pkg/terror"
)

var etcdTestCli *clientv3.Client

type testPessimist struct{}

var _ = Suite(&testPessimist{})

func TestShardDDL(t *testing.T) {
	err := log.InitLogger(&log.Config{})
	if err != nil {
		t.Fatal(err)
	}

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
		task          = "task"
		source        = "mysql-replicate-1"
		schema, table = "foo", "bar"
		DDLs          = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		ID            = "task-`foo`.`bar`"
		op            = pessimism.NewOperation(ID, task, source, DDLs, true, false)

		logger = log.L()
		p      = NewPessimist(&logger, etcdTestCli, task, source)
		info   = p.ConstructInfo(schema, table, DDLs)
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// no info and operation in pending
	c.Assert(p.PendingInfo(), IsNil)
	c.Assert(p.PendingOperation(), IsNil)

	// put shard DDL info.
	rev1, err := p.PutInfo(ctx, info)
	c.Assert(err, IsNil)
	c.Assert(rev1, Greater, int64(0))

	// have info in pending
	info2 := p.PendingInfo()
	c.Assert(info2, NotNil)
	c.Assert(*info2, DeepEquals, info)

	// put the lock operation.
	rev2, putted, err := pessimism.PutOperations(etcdTestCli, false, op)
	c.Assert(err, IsNil)
	c.Assert(rev2, Greater, rev1)
	c.Assert(putted, IsTrue)

	// wait for the lock operation.
	op2, err := p.GetOperation(ctx, info, rev1)
	c.Assert(err, IsNil)
	c.Assert(op2, DeepEquals, op)

	// have operation in pending.
	op3 := p.PendingOperation()
	c.Assert(op3, NotNil)
	c.Assert(*op3, DeepEquals, op)

	// mark the operation as done and delete the info.
	c.Assert(p.DoneOperationDeleteInfo(op, info), IsNil)
	c.Assert(terror.ErrWorkerDDLLockInfoNotFound.Equal(p.DoneOperationDeleteInfo(op, info)), IsTrue)

	// verify the operation and info.
	opc := op2
	opc.Done = true
	opm, _, err := pessimism.GetAllOperations(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(opm, HasLen, 1)
	c.Assert(opm[task], HasLen, 1)
	c.Assert(opm[task][source], DeepEquals, opc)
	ifm, _, err := pessimism.GetAllInfo(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(ifm, HasLen, 0)

	// no info and operation in pending now.
	c.Assert(p.PendingInfo(), IsNil)
	c.Assert(p.PendingOperation(), IsNil)

	// try to put info again, but timeout because a `done` operation exist in etcd.
	ctx2, cancel2 := context.WithTimeout(ctx, time.Second)
	defer cancel2()
	_, err = p.PutInfo(ctx2, info)
	c.Assert(err, Equals, context.DeadlineExceeded)

	// start a goroutine to delete the `done` operation in background, then we can put info again.
	go func() {
		time.Sleep(500 * time.Millisecond) // wait `PutInfo` to start watch the deletion of the operation.
		_, err2 := pessimism.DeleteOperations(etcdTestCli, op)
		c.Assert(err2, IsNil)
	}()

	// put info again, but do not complete the flow.
	_, err = p.PutInfo(ctx, info)
	c.Assert(err, IsNil)
	c.Assert(p.PendingInfo(), NotNil)

	// put the lock operation again.
	rev3, _, err := pessimism.PutOperations(etcdTestCli, false, op)
	c.Assert(err, IsNil)
	// wait for the lock operation.
	_, err = p.GetOperation(ctx, info, rev3)
	c.Assert(err, IsNil)
	c.Assert(p.PendingOperation(), NotNil)

	// reset the pessimist.
	p.Reset()
	c.Assert(p.PendingInfo(), IsNil)
	c.Assert(p.PendingOperation(), IsNil)
}
