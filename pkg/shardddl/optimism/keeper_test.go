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

package optimism

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/tidb/util/mock"
)

type testLockKeeper struct{}

var _ = Suite(&testLockKeeper{})

func (t *testLockKeeper) TestLockKeeper(c *C) {
	var (
		lk         = NewLockKeeper()
		upSchema   = "foo_1"
		upTable    = "bar_1"
		downSchema = "foo"
		downTable  = "bar"
		DDLs       = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		task1      = "task1"
		task2      = "task2"
		source1    = "mysql-replica-1"
		source2    = "mysql-replica-2"

		p              = parser.New()
		se             = mock.NewContext()
		tblID    int64 = 111
		tiBefore       = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		tiAfter        = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)

		i11 = NewInfo(task1, source1, upSchema, upTable, downSchema, downTable, DDLs, tiBefore, tiAfter)
		i12 = NewInfo(task1, source2, upSchema, upTable, downSchema, downTable, DDLs, tiBefore, tiAfter)
		i21 = NewInfo(task2, source1, upSchema, upTable, downSchema, downTable, DDLs, tiBefore, tiAfter)

		sts1 = []SourceTables{
			NewSourceTables(task1, source1, map[string]map[string]struct{}{upSchema: {upTable: struct{}{}}}),
			NewSourceTables(task1, source2, map[string]map[string]struct{}{upSchema: {upTable: struct{}{}}}),
		}
		sts2 = []SourceTables{
			NewSourceTables(task1, source1, map[string]map[string]struct{}{upSchema: {upTable: struct{}{}}}),
		}
	)

	// lock with 2 sources.
	lockID1, newDDLs, err := lk.TrySync(i11, sts1)
	c.Assert(err, IsNil)
	c.Assert(lockID1, Equals, "task1-`foo`.`bar`")
	c.Assert(newDDLs, DeepEquals, DDLs)
	lock1 := lk.FindLock(lockID1)
	c.Assert(lock1, NotNil)
	c.Assert(lock1.ID, Equals, lockID1)
	synced, remain := lock1.IsSynced()
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 1)

	lockID1, newDDLs, err = lk.TrySync(i12, sts1)
	c.Assert(err, IsNil)
	c.Assert(lockID1, Equals, "task1-`foo`.`bar`")
	c.Assert(newDDLs, DeepEquals, DDLs)
	lock1 = lk.FindLock(lockID1)
	c.Assert(lock1, NotNil)
	c.Assert(lock1.ID, Equals, lockID1)
	synced, remain = lock1.IsSynced()
	c.Assert(synced, IsTrue)
	c.Assert(remain, Equals, 0)

	// lock with only 1 source.
	lockID2, newDDLs, err := lk.TrySync(i21, sts2)
	c.Assert(err, IsNil)
	c.Assert(lockID2, Equals, "task2-`foo`.`bar`")
	c.Assert(newDDLs, DeepEquals, DDLs)
	lock2 := lk.FindLock(lockID2)
	c.Assert(lock2, NotNil)
	c.Assert(lock2.ID, Equals, lockID2)
	synced, remain = lock2.IsSynced()
	c.Assert(synced, IsTrue)
	c.Assert(remain, Equals, 0)

	// try to find not-exists lock.
	lockIDNotExists := "lock-not-exists"
	c.Assert(lk.FindLock(lockIDNotExists), IsNil)

	// all locks.
	locks := lk.Locks()
	c.Assert(locks, HasLen, 2)
	c.Assert(locks[lockID1], Equals, lock1) // compare pointer
	c.Assert(locks[lockID2], Equals, lock2)

	// remove lock.
	c.Assert(lk.RemoveLock(lockID1), IsTrue)
	c.Assert(lk.RemoveLock(lockIDNotExists), IsFalse)
	c.Assert(lk.Locks(), HasLen, 1)

	// clear locks.
	lk.Clear()

	// no locks exist.
	c.Assert(lk.Locks(), HasLen, 0)
}
