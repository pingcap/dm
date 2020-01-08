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
	. "github.com/pingcap/check"

	"github.com/pingcap/dm/pkg/terror"
)

type testLock struct{}

var _ = Suite(&testLock{})

func (t *testLock) TestLock(c *C) {
	var (
		ID      = "test-`foo`.`bar`"
		task    = "test"
		source1 = "mysql-replica-1"
		source2 = "mysql-replica-2"
		source3 = "mysql-replica-3"
		DDLs    = []string{
			"ALTER TABLE bar ADD COLUMN c1 INT",
			"ALTER TABLE bar ADD COLUMN c2 INT",
		}
	)

	// create the lock with only 1 source.
	l1 := NewLock(ID, task, source1, DDLs, []string{source1})

	// DDLs mismatch.
	synced, remain, err := l1.TrySync(source1, DDLs[1:], []string{source1})
	c.Assert(terror.ErrMasterShardingDDLDiff.Equal(err), IsTrue)
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 1)
	c.Assert(l1.Ready(), DeepEquals, map[string]bool{source1: false})

	// synced
	synced, remain, err = l1.TrySync(source1, DDLs, []string{source1})
	c.Assert(err, IsNil)
	c.Assert(synced, IsTrue)
	c.Assert(remain, Equals, 0)
	c.Assert(l1.Ready(), DeepEquals, map[string]bool{source1: true})

	// create the lock with 2 sources.
	l2 := NewLock(ID, task, source1, DDLs, []string{source1, source2})

	// join a new source.
	synced, remain, err = l2.TrySync(source1, DDLs, []string{source2, source3})
	c.Assert(err, IsNil)
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 2)
	c.Assert(l2.Ready(), DeepEquals, map[string]bool{
		source1: true,
		source2: false,
		source3: false,
	})

	// sync other sources.
	synced, remain, err = l2.TrySync(source2, DDLs, []string{})
	c.Assert(err, IsNil)
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 1)
	c.Assert(l2.Ready(), DeepEquals, map[string]bool{
		source1: true,
		source2: true,
		source3: false,
	})
	synced, remain, err = l2.TrySync(source3, DDLs, nil)
	c.Assert(err, IsNil)
	c.Assert(synced, IsTrue)
	c.Assert(remain, Equals, 0)
	c.Assert(l2.Ready(), DeepEquals, map[string]bool{
		source1: true,
		source2: true,
		source3: true,
	})
}
