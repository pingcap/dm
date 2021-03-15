// Copyright 2021 PingCAP, Inc.
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

package optimism

import . "github.com/pingcap/check"

type testColumn struct{}

var _ = Suite(&testColumn{})

func (t *testColumn) TestColumnETCD(c *C) {
	defer clearTestInfoOperation(c)

	lockID := "test-`shardddl`.`tb`"
	rev1, putted, err := PutDroppedColumn(etcdTestCli, lockID, "a")
	c.Assert(err, IsNil)
	c.Assert(putted, IsTrue)
	rev2, putted, err := PutDroppedColumn(etcdTestCli, lockID, "b")
	c.Assert(err, IsNil)
	c.Assert(putted, IsTrue)
	c.Assert(rev2, Greater, rev1)

	expectedColm := map[string]map[string]interface{}{
		lockID: {
			"a": struct{}{},
			"b": struct{}{}},
	}
	colm, rev3, err := GetAllDroppedColumns(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(colm, DeepEquals, expectedColm)
	c.Assert(rev3, Equals, rev2)

	rev4, deleted, err := DeleteDroppedColumns(etcdTestCli, lockID, "b")
	c.Assert(err, IsNil)
	c.Assert(deleted, IsTrue)
	c.Assert(rev4, Greater, rev3)

	delete(expectedColm[lockID], "b")
	colm, rev5, err := GetAllDroppedColumns(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(colm, DeepEquals, expectedColm)
	c.Assert(rev5, Equals, rev4)
}
