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

type testOperation struct{}

var _ = Suite(&testOperation{})

func (t *testOperation) TestJSON(c *C) {
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

func (t *testOperation) TestEtcd(c *C) {
	var (
		task1   = "test1"
		task2   = "test2"
		ID1     = "test1-`foo`.`bar`"
		ID2     = "test2-`foo`.`bar`"
		source1 = "mysql-replica-1"
		source2 = "mysql-replica-2"
		DDLs    = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		op11    = NewOperation(ID1, task1, source1, DDLs, true, false)
		op12    = NewOperation(ID1, task1, source2, DDLs, true, false)
		op21    = NewOperation(ID2, task2, source1, DDLs, false, true)
	)

	// put the same keys twice.
	rev1, err := PutOperations(etcdTestCli, op11, op12)
	c.Assert(err, IsNil)
	rev2, err := PutOperations(etcdTestCli, op11, op12)
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
	_, err = PutOperations(etcdTestCli, op21)
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
}
