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

package pessimism

import (
	"context"
	"time"

	. "github.com/pingcap/check"
)

func (t *testForEtcd) TestOperationJSON(c *C) {
	o1 := NewOperation("test-ID", "test", "mysql-replica-1", []string{
		"ALTER TABLE bar ADD COLUMN c1 INT",
	}, true, false)

	j, err := o1.toJSON()
	c.Assert(err, IsNil)
	c.Assert(j, Equals, `{"id":"test-ID","task":"test","source":"mysql-replica-1","ddls":["ALTER TABLE bar ADD COLUMN c1 INT"],"exec":true,"done":false}`)

	o2, err := operationFromJSON(j)
	c.Assert(err, IsNil)
	c.Assert(o2, DeepEquals, o1)
}

func (t *testForEtcd) TestOperationEtcd(c *C) {
	defer clearTestInfoOperation(c)

	var (
		task1   = "test1"
		task2   = "test2"
		ID1     = "test1-`foo`.`bar`"
		ID2     = "test2-`foo`.`bar`"
		source1 = "mysql-replica-1"
		source2 = "mysql-replica-2"
		source3 = "mysql-replica-3"
		DDLs    = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		op11    = NewOperation(ID1, task1, source1, DDLs, true, false)
		op12    = NewOperation(ID1, task1, source2, DDLs, true, false)
		op13    = NewOperation(ID1, task1, source3, DDLs, true, false)
		op21    = NewOperation(ID2, task2, source1, DDLs, false, true)
	)

	// put the same keys twice.
	rev1, err := PutOperations(etcdTestCli, false, op11, op12)
	c.Assert(err, IsNil)
	rev2, err := PutOperations(etcdTestCli, false, op11, op12)
	c.Assert(err, IsNil)
	c.Assert(rev2, Greater, rev1)

	// start the watcher with the same revision as the last PUT for the specified task and source.
	wch := make(chan Operation, 10)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	WatchOperationPut(ctx, etcdTestCli, task1, source1, rev2, wch)
	cancel()
	close(wch)

	// watch should only get op11.
	c.Assert(len(wch), Equals, 1)
	c.Assert(<-wch, DeepEquals, op11)

	// put for another task.
	rev3, err := PutOperations(etcdTestCli, false, op21)
	c.Assert(err, IsNil)

	// start the watch with an older revision for all tasks and sources.
	wch = make(chan Operation, 10)
	ctx, cancel = context.WithTimeout(context.Background(), 500*time.Millisecond)
	WatchOperationPut(ctx, etcdTestCli, "", "", rev2, wch)
	cancel()
	close(wch)

	// watch should get 3 operations.
	c.Assert(len(wch), Equals, 3)
	c.Assert(<-wch, DeepEquals, op11)
	c.Assert(<-wch, DeepEquals, op12)
	c.Assert(<-wch, DeepEquals, op21)

	// get all operations.
	opm, rev4, err := GetAllOperations(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev4, Equals, rev3)
	c.Assert(opm, HasLen, 2)
	c.Assert(opm, HasKey, task1)
	c.Assert(opm, HasKey, task2)
	c.Assert(opm[task1], HasLen, 2)
	c.Assert(opm[task1][source1], DeepEquals, op11)
	c.Assert(opm[task1][source2], DeepEquals, op12)
	c.Assert(opm[task2], HasLen, 1)
	c.Assert(opm[task2][source1], DeepEquals, op21)

	// delete op11.
	_, err = DeleteOperations(etcdTestCli, op11)
	c.Assert(err, IsNil)

	// update op12 to `done`.
	op12c := op12
	op12c.Done = true
	putOp, err := putOperationOp(op12c)
	c.Assert(err, IsNil)
	txnResp, err := etcdTestCli.Txn(context.Background()).Then(putOp).Commit()
	c.Assert(err, IsNil)

	// put for `skipDone` with `done` in etcd, the operations should be skipped.
	rev5, err := PutOperations(etcdTestCli, true, op12, op13)
	c.Assert(err, IsNil)
	c.Assert(rev5, Equals, txnResp.Header.Revision) // same revision with the previous etcd operation.

	// get again, op11 deleted, op13 not putted.
	opm, _, err = GetAllOperations(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(opm[task1], HasLen, 1)
	c.Assert(opm[task1][source2], DeepEquals, op12c)
}
