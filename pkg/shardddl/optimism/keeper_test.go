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

type testKeeper struct{}

var _ = Suite(&testKeeper{})

func (t *testKeeper) TestLockKeeper(c *C) {
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

func (t *testKeeper) TestTableKeeper(c *C) {
	var (
		tk      = NewTableKeeper()
		task1   = "task-1"
		task2   = "task-2"
		source1 = "mysql-replica-1"
		source2 = "mysql-replica-2"
		st11    = NewSourceTables(task1, source1, map[string]map[string]struct{}{
			"db": {"tbl-1": struct{}{}, "tbl-2": struct{}{}},
		})
		st12 = NewSourceTables(task1, source2, map[string]map[string]struct{}{
			"db": {"tbl-1": struct{}{}, "tbl-2": struct{}{}},
		})
		st21 = NewSourceTables(task2, source2, map[string]map[string]struct{}{
			"db": {"tbl-3": struct{}{}},
		})
		st22 = NewSourceTables(task2, source2, map[string]map[string]struct{}{
			"db": {"tbl-3": struct{}{}, "tbl-4": struct{}{}},
		})
		stm = map[string]map[string]SourceTables{
			task1: {source2: st12, source1: st11},
		}
	)

	// no tables exist before Init/Update.
	c.Assert(tk.FindTables(task1), IsNil)

	// Init with `nil` is fine.
	tk.Init(nil)
	c.Assert(tk.FindTables(task1), IsNil)

	// tables for task1 exit after Init.
	tk.Init(stm)
	sts := tk.FindTables(task1)
	c.Assert(sts, HasLen, 2)
	c.Assert(sts[0], DeepEquals, st11)
	c.Assert(sts[1], DeepEquals, st12)

	// adds new tables.
	c.Assert(tk.Update(st21), IsTrue)
	sts = tk.FindTables(task2)
	c.Assert(sts, HasLen, 1)
	c.Assert(sts[0], DeepEquals, st21)

	// updates/appends new tables.
	c.Assert(tk.Update(st22), IsTrue)
	sts = tk.FindTables(task2)
	c.Assert(sts, HasLen, 1)
	c.Assert(sts[0], DeepEquals, st22)

	// deletes tables.
	st22.IsDeleted = true
	c.Assert(tk.Update(st22), IsTrue)
	c.Assert(tk.FindTables(task2), IsNil)

	// try to delete, but not exist.
	c.Assert(tk.Update(st22), IsFalse)
	st22.Task = "not-exist"
	c.Assert(tk.Update(st22), IsFalse)

	// tables for task1 not affected.
	sts = tk.FindTables(task1)
	c.Assert(sts, HasLen, 2)
	c.Assert(sts[0], DeepEquals, st11)
	c.Assert(sts[1], DeepEquals, st12)

	// add a table for st11.
	c.Assert(tk.AddTable(task1, st11.Source, "db-2", "tbl-3"), IsTrue)
	c.Assert(tk.AddTable(task1, st11.Source, "db-2", "tbl-3"), IsFalse)
	sts = tk.FindTables(task1)
	st11n := sts[0]
	c.Assert(st11n.Tables, HasKey, "db-2")
	c.Assert(st11n.Tables["db-2"], HasKey, "tbl-3")

	// removed the added table in st11.
	c.Assert(tk.RemoveTable(task1, st11.Source, "db-2", "tbl-3"), IsTrue)
	c.Assert(tk.RemoveTable(task1, st11.Source, "db-2", "tbl-3"), IsFalse)
	sts = tk.FindTables(task1)
	st11n = sts[0]
	c.Assert(st11n.Tables["db-2"], IsNil)

	// adds for not existing task/source takes no effect.
	c.Assert(tk.AddTable("not-exist", st11.Source, "db-2", "tbl-3"), IsFalse)
	c.Assert(tk.AddTable(task1, "not-exist", "db-2", "tbl-3"), IsFalse)
	sts = tk.FindTables(task1)
	st11n = sts[0]
	c.Assert(st11n.Tables["db-2"], IsNil)

	// removes for not existing task/source takes no effect.
	c.Assert(tk.RemoveTable("not-exit", st12.Source, "db", "tbl-1"), IsFalse)
	c.Assert(tk.RemoveTable(task1, "not-exit", "db", "tbl-1"), IsFalse)
	sts = tk.FindTables(task1)
	c.Assert(sts[1], DeepEquals, st12)
}
