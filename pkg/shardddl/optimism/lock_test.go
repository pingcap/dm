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
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/util/mock"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
)

type testLock struct{}

var _ = Suite(&testLock{})

func (t *testLock) SetUpSuite(c *C) {
	log.InitLogger(&log.Config{})
}

func (t *testLock) TestLockTrySyncNormal(c *C) {
	var (
		ID               = "test_lock_try_sync_normal-`foo`.`bar`"
		task             = "test_lock_try_sync_normal"
		sources          = []string{"mysql-replica-1", "mysql-replica-2"}
		downSchema       = "db"
		downTable        = "bar"
		dbs              = []string{"db1", "db2"}
		tbls             = []string{"bar1", "bar2"}
		tableCount       = len(sources) * len(dbs) * len(tbls)
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111
		DDLs1            = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		DDLs2            = []string{"ALTER TABLE bar ADD COLUMN c2 BIGINT", "ALTER TABLE bar ADD COLUMN c3 TEXT"}
		DDLs3            = []string{"ALTER TABLE bar DROP COLUMN c3"}
		DDLs4            = []string{"ALTER TABLE bar DROP COLUMN c2", "ALTER TABLE bar DROP COLUMN c1"}
		ti0              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)
		ti2              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 BIGINT, c3 TEXT)`)
		ti2_1            = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 BIGINT)`)
		ti3              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 BIGINT)`)
		ti4              = ti0
		ti4_1            = ti1
		tables           = map[string]map[string]struct{}{
			dbs[0]: {tbls[0]: struct{}{}, tbls[1]: struct{}{}},
			dbs[1]: {tbls[0]: struct{}{}, tbls[1]: struct{}{}},
		}
		tts = []TargetTable{
			newTargetTable(task, sources[0], downSchema, downTable, tables),
			newTargetTable(task, sources[1], downSchema, downTable, tables),
		}

		l = NewLock(ID, task, downSchema, downTable, ti0, tts)
	)

	// the initial status is synced.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: all tables execute a single & same DDL (schema become larger).
	syncedCount := 0
	for _, source := range sources {
		if source == sources[len(sources)-1] {
			ready := l.Ready()
			for _, source2 := range sources {
				synced := source != source2 // tables before the last source have synced.
				for _, db2 := range dbs {
					for _, tbl2 := range tbls {
						c.Assert(ready[source2][db2][tbl2], Equals, synced)
					}
				}
			}
		}

		for _, db := range dbs {
			for _, tbl := range tbls {
				DDLs, err := l.TrySync(source, db, tbl, DDLs1, ti1, tts)
				c.Assert(err, IsNil)
				c.Assert(DDLs, DeepEquals, DDLs1)

				syncedCount++
				synced, remain := l.IsSynced()
				c.Assert(synced, Equals, syncedCount == tableCount)
				c.Assert(remain, Equals, tableCount-syncedCount)
				c.Assert(synced, Equals, l.synced)
			}
		}
	}
	// synced again after all tables applied the DDL.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: TrySync again after synced is idempotent.
	DDLs, err := l.TrySync(sources[0], dbs[0], tbls[0], DDLs1, ti1, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: need to add more than one DDL to reach the desired schema (schema become larger).
	// add two columns for one table.
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[0], DDLs2, ti2, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2)
	ready := l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[0]], IsTrue)
	c.Assert(ready[sources[0]][dbs[0]][tbls[1]], IsFalse)

	// TrySync again is idempotent (more than one DDL).
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[0], DDLs2, ti2, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2)
	ready = l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[0]], IsTrue)
	c.Assert(ready[sources[0]][dbs[0]][tbls[1]], IsFalse)

	// add only the first column for another table.
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[1], DDLs2[0:1], ti2_1, tts) // use ti2_1 info
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2[0:1])
	ready = l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[0]], IsTrue)
	c.Assert(ready[sources[0]][dbs[0]][tbls[1]], IsFalse)
	synced, remain := l.IsSynced()
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, tableCount-1)
	c.Assert(synced, Equals, l.synced)
	cmp, err := l.tables[sources[0]][dbs[0]][tbls[0]].Compare(l.tables[sources[0]][dbs[0]][tbls[1]])
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 1)

	// TrySync again (only the first DDL).
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[1], DDLs2[0:1], ti2_1, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{}) // NOTE: special case, joined has larger schema.
	ready = l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[1]], IsFalse)

	// add the second column for another table.
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[1], DDLs2[1:2], ti2, tts) // use ti2 info.
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2[1:2])
	ready = l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[1]], IsTrue) // ready now.
	synced, remain = l.IsSynced()
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, tableCount-2)
	c.Assert(synced, Equals, l.synced)
	cmp, err = l.tables[sources[0]][dbs[0]][tbls[0]].Compare(l.tables[sources[0]][dbs[0]][tbls[1]])
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	// Try again (for the second DDL).
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[1], DDLs2[1:2], ti2, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2[1:2])

	// try add columns for all tables to reach the same schema.
	t.trySyncForAllTablesLarger(c, l, DDLs2, ti2, tts)
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: all tables execute a single & same DDL (schema become smaller).
	syncedCount = 0
	for _, source := range sources {
		if source == sources[len(sources)-1] {
			ready = l.Ready()
			for _, source2 := range sources {
				synced = source == source2 // tables before the last source have not synced.
				for _, db2 := range dbs {
					for _, tbl2 := range tbls {
						c.Assert(ready[source2][db2][tbl2], Equals, synced)
					}
				}
			}
		}

		for _, db := range dbs {
			for _, tbl := range tbls {
				syncedCount++
				DDLs, err = l.TrySync(source, db, tbl, DDLs3, ti3, tts)
				c.Assert(err, IsNil)
				synced, remain = l.IsSynced()
				c.Assert(synced, Equals, l.synced)
				if syncedCount == tableCount {
					c.Assert(DDLs, DeepEquals, DDLs3)
					c.Assert(synced, IsTrue)
					c.Assert(remain, Equals, 0)
				} else {
					c.Assert(DDLs, DeepEquals, []string{})
					c.Assert(synced, IsFalse)
					c.Assert(remain, Equals, syncedCount)
				}
			}
		}
	}
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: need to drop more than one DDL to reach the desired schema (schema become smaller).
	// drop two columns for one table.
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[0], DDLs4, ti4, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{})
	ready = l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[0]], IsFalse)
	c.Assert(ready[sources[0]][dbs[0]][tbls[1]], IsTrue)

	// TrySync again is idempotent.
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[0], DDLs4, ti4, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{})
	ready = l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[0]], IsFalse)
	c.Assert(ready[sources[0]][dbs[0]][tbls[1]], IsTrue)

	// drop only the first column for another table.
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[1], DDLs4[0:1], ti4_1, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{})
	ready = l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[0]], IsFalse)
	c.Assert(ready[sources[0]][dbs[0]][tbls[1]], IsFalse)
	cmp, err = l.tables[sources[0]][dbs[0]][tbls[0]].Compare(l.tables[sources[0]][dbs[0]][tbls[1]])
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// TrySync again (only the first DDL).
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[1], DDLs4[0:1], ti4_1, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{})

	// drop the second column for another table.
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[1], DDLs4[1:2], ti4, tts) // use ti4 info.
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{})
	ready = l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[0]], IsFalse)
	c.Assert(ready[sources[0]][dbs[0]][tbls[1]], IsFalse)
	cmp, err = l.tables[sources[0]][dbs[0]][tbls[0]].Compare(l.tables[sources[0]][dbs[0]][tbls[1]])
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	// TrySync again (for the second DDL).
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[1], DDLs4[1:2], ti4, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{})

	// try drop columns for other tables to reach the same schema.
	remain = tableCount - 2
	for source, schemaTables := range l.Ready() {
		for schema, tables := range schemaTables {
			for table, synced2 := range tables {
				if synced2 { // do not `TrySync` again for previous two (un-synced now).
					DDLs, err = l.TrySync(source, schema, table, DDLs4, ti4, tts)
					c.Assert(err, IsNil)
					remain--
					if remain == 0 {
						c.Assert(DDLs, DeepEquals, DDLs4)
					} else {
						c.Assert(DDLs, DeepEquals, []string{})
					}
				}
			}
		}
	}
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)
}

func (t *testLock) TestLockTrySyncIndex(c *C) {
	var (
		ID               = "test_lock_try_sync_index-`foo`.`bar`"
		task             = "test_lock_try_sync_index"
		source           = "mysql-replica-1"
		downSchema       = "db"
		downTable        = "bar"
		db               = "db"
		tbls             = []string{"bar1", "bar2"}
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111
		DDLs1            = []string{"ALTER TABLE bar DROP INDEX idx_c1"}
		DDLs2            = []string{"ALTER TABLE bar ADD UNIQUE INDEX idx_c1(c1)"}
		ti0              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, UNIQUE INDEX idx_c1(c1))`)
		ti1              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)
		ti2              = ti0
		tables           = map[string]map[string]struct{}{
			db: {tbls[0]: struct{}{}, tbls[1]: struct{}{}},
		}
		tts = []TargetTable{
			newTargetTable(task, source, downSchema, downTable, tables),
		}

		l = NewLock(ID, task, downSchema, downTable, ti0, tts)
	)

	// the initial status is synced.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// try sync for one table, `DROP INDEX` returned directly (to make schema become more compatible).
	// `DROP INDEX` is handled like `ADD COLUMN`.
	DDLs, err := l.TrySync(source, db, tbls[0], DDLs1, ti1, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	synced, remain := l.IsSynced()
	c.Assert(synced, Equals, l.synced)
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 1)

	// try sync for another table, also got `DROP INDEX` now.
	DDLs, err = l.TrySync(source, db, tbls[1], DDLs1, ti1, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	t.checkLockSynced(c, l)

	// try sync for one table, `ADD INDEX` not returned directly (to keep the schema more compatible).
	// `ADD INDEX` is handled like `DROP COLUMN`.
	DDLs, err = l.TrySync(source, db, tbls[0], DDLs2, ti2, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{}) // no DDLs returned
	synced, remain = l.IsSynced()
	c.Assert(synced, Equals, l.synced)
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 1)

	// try sync for another table, got `ADD INDEX` now.
	DDLs, err = l.TrySync(source, db, tbls[1], DDLs2, ti2, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2)
	t.checkLockSynced(c, l)
}

func (t *testLock) TestLockTrySyncNullNotNull(c *C) {
	var (
		ID               = "test_lock_try_sync_null_not_null-`foo`.`bar`"
		task             = "test_lock_try_sync_null_not_null"
		source           = "mysql-replica-1"
		downSchema       = "db"
		downTable        = "bar"
		db               = "db"
		tbls             = []string{"bar1", "bar2"}
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111
		DDLs1            = []string{"ALTER TABLE bar MODIFY COLUMN c1 INT NOT NULL DEFAULT 1234"}
		DDLs2            = []string{"ALTER TABLE bar MODIFY COLUMN c1 INT NULL DEFAULT 1234"}
		ti0              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT NULL DEFAULT 1234)`)
		ti1              = createTableInfo(c, p, se, tblID,
			`CREATE TABLE bar (id INT PRIMARY KEY, c1 INT NOT NULL DEFAULT 1234)`)
		ti2    = ti0
		tables = map[string]map[string]struct{}{
			db: {tbls[0]: struct{}{}, tbls[1]: struct{}{}},
		}
		tts = []TargetTable{
			newTargetTable(task, source, downSchema, downTable, tables),
		}

		l = NewLock(ID, task, downSchema, downTable, ti0, tts)
	)

	// the initial status is synced.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	for i := 0; i < 2; i++ { // two round
		// try sync for one table, from `NULL` to `NOT NULL`, no DDLs returned.
		DDLs, err := l.TrySync(source, db, tbls[0], DDLs1, ti1, tts)
		c.Assert(err, IsNil)
		c.Assert(DDLs, DeepEquals, []string{})

		// try sync for another table, DDLs returned.
		DDLs, err = l.TrySync(source, db, tbls[1], DDLs1, ti1, tts)
		c.Assert(err, IsNil)
		c.Assert(DDLs, DeepEquals, DDLs1)

		// try sync for one table, from `NOT NULL` to `NULL`, DDLs returned.
		DDLs, err = l.TrySync(source, db, tbls[0], DDLs2, ti2, tts)
		c.Assert(err, IsNil)
		c.Assert(DDLs, DeepEquals, DDLs2)

		// try sync for another table, from `NOT NULL` to `NULL`, DDLs, returned.
		DDLs, err = l.TrySync(source, db, tbls[1], DDLs2, ti2, tts)
		c.Assert(err, IsNil)
		c.Assert(DDLs, DeepEquals, DDLs2)
	}
}

func (t *testLock) TestLockTrySyncIntBigint(c *C) {
	var (
		ID               = "test_lock_try_sync_int_bigint-`foo`.`bar`"
		task             = "test_lock_try_sync_int_bigint"
		source           = "mysql-replica-1"
		downSchema       = "db"
		downTable        = "bar"
		db               = "db"
		tbls             = []string{"bar1", "bar2"}
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111
		DDLs1            = []string{"ALTER TABLE bar MODIFY COLUMN c1 BIGINT NOT NULL DEFAULT 1234"}
		ti0              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT NOT NULL DEFAULT 1234)`)
		ti1              = createTableInfo(c, p, se, tblID,
			`CREATE TABLE bar (id INT PRIMARY KEY, c1 BIGINT NOT NULL DEFAULT 1234)`)
		tables = map[string]map[string]struct{}{
			db: {tbls[0]: struct{}{}, tbls[1]: struct{}{}},
		}
		tts = []TargetTable{
			newTargetTable(task, source, downSchema, downTable, tables),
		}

		l = NewLock(ID, task, downSchema, downTable, ti0, tts)
	)

	// the initial status is synced.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// try sync for one table, from `INT` to `BIGINT`, DDLs returned.
	DDLs, err := l.TrySync(source, db, tbls[0], DDLs1, ti1, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)

	// try sync for another table, DDLs returned.
	DDLs, err = l.TrySync(source, db, tbls[1], DDLs1, ti1, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
}

func (t *testLock) TestLockTrySyncNoDiff(c *C) {
	var (
		ID               = "test_lock_try_sync_no_diff-`foo`.`bar`"
		task             = "test_lock_try_sync_no_diff"
		source           = "mysql-replica-1"
		downSchema       = "db"
		downTable        = "bar"
		db               = "db"
		tbls             = []string{"bar1", "bar2"}
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111
		DDLs1            = []string{"ALTER TABLE bar DROP COLUMN c1, ADD COLUMN c2 INT"}
		ti0              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)
		ti1              = createTableInfo(c, p, se, tblID,
			`CREATE TABLE bar (id INT PRIMARY KEY, c2 INT)`) // `c1` dropped, `c2` added
		tables = map[string]map[string]struct{}{
			db: {tbls[0]: struct{}{}, tbls[1]: struct{}{}},
		}
		tts = []TargetTable{
			newTargetTable(task, source, downSchema, downTable, tables),
		}

		l = NewLock(ID, task, downSchema, downTable, ti0, tts)
	)

	// the initial status is synced.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// try sync for one table.
	DDLs, err := l.TrySync(source, db, tbls[0], DDLs1, ti1, tts)
	c.Assert(terror.ErrShardDDLOptimismTrySyncFail.Equal(err), IsTrue)
	c.Assert(DDLs, DeepEquals, []string{})
}

func (t *testLock) TestLockTrySyncNewTable(c *C) {
	var (
		ID               = "test_lock_try_sync_new_table-`foo`.`bar`"
		task             = "test_lock_try_sync_new_table"
		source1          = "mysql-replica-1"
		source2          = "mysql-replica-2"
		downSchema       = "foo"
		downTable        = "bar"
		db1              = "foo1"
		db2              = "foo2"
		tbl1             = "bar1"
		tbl2             = "bar2"
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111
		DDLs1            = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		ti0              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)

		tables = map[string]map[string]struct{}{db1: {tbl1: struct{}{}}}
		tts    = []TargetTable{newTargetTable(task, source1, downSchema, downTable, tables)}
		l      = NewLock(ID, task, downSchema, downTable, ti0, tts)
	)

	// only one table exists before TrySync.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// TrySync for a new table as the caller.
	DDLs, err := l.TrySync(source2, db2, tbl2, DDLs1, ti1, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)

	ready := l.Ready()
	c.Assert(ready, HasLen, 2)
	c.Assert(ready[source1], HasLen, 1)
	c.Assert(ready[source1][db1], HasLen, 1)
	c.Assert(ready[source1][db1][tbl1], IsFalse)
	c.Assert(ready[source2], HasLen, 1)
	c.Assert(ready[source2][db2], HasLen, 1)
	c.Assert(ready[source2][db2][tbl2], IsTrue)

	// TrySync for two new tables as extra sources.
	// we treat all newly added sources as synced.
	tts = append(tts,
		newTargetTable(task, source1, downSchema, downTable, map[string]map[string]struct{}{db1: {tbl2: struct{}{}}}),
		newTargetTable(task, source2, downTable, downTable, map[string]map[string]struct{}{db2: {tbl1: struct{}{}}}),
	)
	DDLs, err = l.TrySync(source1, db1, tbl1, DDLs1, ti1, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)

	ready = l.Ready()
	c.Assert(ready, HasLen, 2)
	c.Assert(ready[source1], HasLen, 1)
	c.Assert(ready[source1][db1], HasLen, 2)
	c.Assert(ready[source1][db1][tbl1], IsTrue)
	c.Assert(ready[source1][db1][tbl2], IsTrue)
	c.Assert(ready[source2], HasLen, 1)
	c.Assert(ready[source2][db2], HasLen, 2)
	c.Assert(ready[source2][db2][tbl1], IsTrue)
	c.Assert(ready[source2][db2][tbl2], IsTrue)
}

func (t *testLock) TestLockTrySyncRevert(c *C) {
	var (
		ID               = "test_lock_try_sync_revert-`foo`.`bar`"
		task             = "test_lock_try_sync_revert"
		source           = "mysql-replica-1"
		downSchema       = "foo"
		downTable        = "bar"
		db               = "foo"
		tbls             = []string{"bar1", "bar2"}
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111

		DDLs1 = []string{"ALTER TABLE bar ADD COLUMN c1 TEXT"}
		DDLs2 = []string{"ALTER TABLE bar DROP COLUMN c1"}
		ti0   = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1   = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT)`)
		ti2   = ti0

		DDLs3 = []string{"ALTER TABLE bar ADD COLUMN c1 TEXT", "ALTER TABLE bar ADD COLUMN c2 INT"}
		DDLs4 = []string{"ALTER TABLE bar DROP COLUMN c2"}
		DDLs5 = []string{"ALTER TABLE bar DROP COLUMN c1"}
		ti3   = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT, c2 INT)`)
		ti4   = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT)`)
		ti5   = ti0

		DDLs6 = DDLs3
		DDLs7 = DDLs4
		DDLs8 = []string{"ALTER TABLE bar ADD COLUMN c1 TEXT"}
		ti6   = ti3
		ti7   = ti4
		ti8   = ti4

		tables = map[string]map[string]struct{}{db: {tbls[0]: struct{}{}, tbls[1]: struct{}{}}}
		tts    = []TargetTable{newTargetTable(task, source, downSchema, downTable, tables)}
		l      = NewLock(ID, task, downSchema, downTable, ti0, tts)
	)

	// the initial status is synced.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: revert for single DDL.
	// TrySync for one table.
	DDLs, err := l.TrySync(source, db, tbls[0], DDLs1, ti1, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	ready := l.Ready()
	c.Assert(ready[source][db][tbls[0]], IsTrue)
	c.Assert(ready[source][db][tbls[1]], IsFalse)
	cmp, err := l.tables[source][db][tbls[0]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// revert for the table, become synced again.
	DDLs, err = l.TrySync(source, db, tbls[0], DDLs2, ti2, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2)
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: revert for multiple DDLs.
	// TrySync for one table.
	DDLs, err = l.TrySync(source, db, tbls[0], DDLs3, ti3, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs3)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[0]], IsTrue)
	c.Assert(ready[source][db][tbls[1]], IsFalse)
	cmp, err = l.tables[source][db][tbls[0]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// revert part of the DDLs.
	DDLs, err = l.TrySync(source, db, tbls[0], DDLs4, ti4, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs4)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[1]], IsFalse)
	cmp, err = l.tables[source][db][tbls[0]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// revert the reset part of the DDLs.
	DDLs, err = l.TrySync(source, db, tbls[0], DDLs5, ti5, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs5)
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: revert part of multiple DDLs.
	// TrySync for one table.
	DDLs, err = l.TrySync(source, db, tbls[0], DDLs6, ti6, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs6)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[1]], IsFalse)
	cmp, err = l.tables[source][db][tbls[0]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// revert part of the DDLs.
	DDLs, err = l.TrySync(source, db, tbls[0], DDLs7, ti7, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs7)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[1]], IsFalse)
	cmp, err = l.tables[source][db][tbls[0]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// TrySync for another table.
	DDLs, err = l.TrySync(source, db, tbls[1], DDLs8, ti8, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs8)
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)
}

func (t *testLock) TestLockTrySyncConflictNonIntrusive(c *C) {
	var (
		ID               = "test_lock_try_sync_conflict_non_intrusive-`foo`.`bar`"
		task             = "test_lock_try_sync_conflict_non_intrusive"
		source           = "mysql-replica-1"
		downSchema       = "foo"
		downTable        = "bar"
		db               = "foo"
		tbls             = []string{"bar1", "bar2"}
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111
		DDLs1            = []string{"ALTER TABLE bar ADD COLUMN c1 TEXT"}
		DDLs2            = []string{"ALTER TABLE bar ADD COLUMN c1 DATETIME", "ALTER TABLE bar ADD COLUMN c2 INT"}
		DDLs3            = []string{"ALTER TABLE bar DROP COLUMN c1"}
		DDLs4            = DDLs2
		ti0              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT)`)
		ti2              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 DATETIME, c2 INT)`)
		ti3              = ti0
		ti4              = ti2

		tables = map[string]map[string]struct{}{db: {tbls[0]: struct{}{}, tbls[1]: struct{}{}}}
		tts    = []TargetTable{newTargetTable(task, source, downSchema, downTable, tables)}
		l      = NewLock(ID, task, downSchema, downTable, ti0, tts)
	)

	// the initial status is synced.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// TrySync for the first table, construct the joined schema.
	DDLs, err := l.TrySync(source, db, tbls[0], DDLs1, ti1, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	ready := l.Ready()
	c.Assert(ready[source][db][tbls[0]], IsTrue)
	c.Assert(ready[source][db][tbls[1]], IsFalse)
	cmp, err := l.tables[source][db][tbls[0]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// TrySync for the second table with another schema (add two columns, one of them will cause conflict).
	DDLs, err = l.TrySync(source, db, tbls[1], DDLs2, ti2, tts)
	c.Assert(terror.ErrShardDDLOptimismTrySyncFail.Equal(err), IsTrue)
	c.Assert(DDLs, DeepEquals, []string{})
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
	c.Assert(err, ErrorMatches, ".*at tuple index.*")
	c.Assert(cmp, Equals, 0)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[1]], IsFalse)

	// TrySync for the first table to resolve the conflict.
	DDLs, err = l.TrySync(source, db, tbls[0], DDLs3, ti3, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs3)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[0]], IsFalse)
	c.Assert(ready[source][db][tbls[1]], IsTrue) // the second table become synced now.
	cmp, err = l.tables[source][db][tbls[0]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	// TrySync for the first table.
	DDLs, err = l.TrySync(source, db, tbls[0], DDLs4, ti4, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs4)
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)
}

func (t *testLock) TestLockTrySyncConflictIntrusive(c *C) {
	var (
		ID               = "test_lock_try_sync_conflict_intrusive-`foo`.`bar`"
		task             = "test_lock_try_sync_conflict_intrusive"
		source           = "mysql-replica-1"
		downSchema       = "foo"
		downTable        = "bar"
		db               = "foo"
		tbls             = []string{"bar1", "bar2"}
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111
		DDLs1            = []string{"ALTER TABLE bar ADD COLUMN c1 TEXT"}
		DDLs2            = []string{"ALTER TABLE bar ADD COLUMN c1 DATETIME", "ALTER TABLE bar ADD COLUMN c2 INT"}
		DDLs3            = []string{"ALTER TABLE bar DROP COLUMN c2"}
		DDLs4            = []string{"ALTER TABLE bar DROP COLUMN c1"}
		ti0              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT)`)
		ti2              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 DATETIME, c2 INT)`)
		ti3              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 DATETIME)`)
		ti4              = ti0

		DDLs5   = []string{"ALTER TABLE bar ADD COLUMN c2 TEXT"}
		DDLs6   = []string{"ALTER TABLE bar ADD COLUMN c2 DATETIME", "ALTER TABLE bar ADD COLUMN c3 INT"}
		DDLs7   = []string{"ALTER TABLE bar DROP COLUMN c2"}
		DDLs8_1 = []string{"ALTER TABLE bar ADD COLUMN c3 INT"}
		DDLs8_2 = []string{"ALTER TABLE bar ADD COLUMN c2 TEXT"}
		ti5     = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT, c2 TEXT)`)
		ti6     = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT, c2 DATETIME, c3 INT)`)
		ti7     = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT, c3 INT)`)
		ti8     = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT, c2 TEXT, c3 INT)`)

		tables = map[string]map[string]struct{}{db: {tbls[0]: struct{}{}, tbls[1]: struct{}{}}}
		tts    = []TargetTable{newTargetTable(task, source, downSchema, downTable, tables)}
		l      = NewLock(ID, task, downSchema, downTable, ti0, tts)
	)

	// the initial status is synced.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: conflict happen, revert all changes to resolve the conflict.
	// TrySync for the first table, construct the joined schema.
	DDLs, err := l.TrySync(source, db, tbls[0], DDLs1, ti1, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	ready := l.Ready()
	c.Assert(ready[source][db][tbls[0]], IsTrue)
	c.Assert(ready[source][db][tbls[1]], IsFalse)
	cmp, err := l.tables[source][db][tbls[0]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// TrySync for the second table with another schema (add two columns, one of them will cause conflict).
	DDLs, err = l.TrySync(source, db, tbls[1], DDLs2, ti2, tts)
	c.Assert(terror.ErrShardDDLOptimismTrySyncFail.Equal(err), IsTrue)
	c.Assert(DDLs, DeepEquals, []string{})
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
	c.Assert(err, ErrorMatches, ".*at tuple index.*")
	c.Assert(cmp, Equals, 0)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[1]], IsFalse)

	// TrySync again.
	DDLs, err = l.TrySync(source, db, tbls[1], DDLs2, ti2, tts)
	c.Assert(terror.ErrShardDDLOptimismTrySyncFail.Equal(err), IsTrue)
	c.Assert(DDLs, DeepEquals, []string{})
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
	c.Assert(err, ErrorMatches, ".*at tuple index.*")
	c.Assert(cmp, Equals, 0)

	// TrySync for the second table to drop the non-conflict column, the conflict should still exist.
	DDLs, err = l.TrySync(source, db, tbls[1], DDLs3, ti3, tts)
	c.Assert(terror.ErrShardDDLOptimismTrySyncFail.Equal(err), IsTrue)
	c.Assert(DDLs, DeepEquals, []string{})
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
	c.Assert(err, ErrorMatches, ".*at tuple index.*")
	c.Assert(cmp, Equals, 0)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[1]], IsFalse)

	// TrySync for the second table to drop the conflict column, the conflict should be resolved.
	DDLs, err = l.TrySync(source, db, tbls[1], DDLs4, ti4, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{})
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[1]], IsFalse)

	// TrySync for the second table as we did for the first table, the lock should be synced.
	DDLs, err = l.TrySync(source, db, tbls[1], DDLs1, ti1, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: conflict happen, revert part of changes to resolve the conflict.
	// TrySync for the first table, construct the joined schema.
	DDLs, err = l.TrySync(source, db, tbls[0], DDLs5, ti5, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs5)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[0]], IsTrue)
	c.Assert(ready[source][db][tbls[1]], IsFalse)
	cmp, err = l.tables[source][db][tbls[0]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// TrySync for the second table with another schema (add two columns, one of them will cause conflict).
	DDLs, err = l.TrySync(source, db, tbls[1], DDLs6, ti6, tts)
	c.Assert(terror.ErrShardDDLOptimismTrySyncFail.Equal(err), IsTrue)
	c.Assert(DDLs, DeepEquals, []string{})
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
	c.Assert(err, ErrorMatches, ".*at tuple index.*")
	c.Assert(cmp, Equals, 0)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[1]], IsFalse)

	// TrySync for the second table to drop the conflict column, the conflict should be resolved.
	// but both of tables are not synced now.
	DDLs, err = l.TrySync(source, db, tbls[1], DDLs7, ti7, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs7) // special case: these DDLs should not be replicated to the downstream.
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[0]], IsFalse)
	c.Assert(ready[source][db][tbls[1]], IsFalse)
	cmp, err = l.tables[source][db][tbls[0]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// TrySync for the first table to become synced.
	DDLs, err = l.TrySync(source, db, tbls[0], DDLs8_1, ti8, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs8_1)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[0]], IsTrue)

	// TrySync for the second table to become synced.
	DDLs, err = l.TrySync(source, db, tbls[1], DDLs8_2, ti8, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs8_2)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[1]], IsTrue)

	// all tables synced now.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)
}

func (t *testLock) TestTryRemoveTable(c *C) {
	var (
		ID               = "test_lock_try_remove_table-`foo`.`bar`"
		task             = "test_lock_try_remove_table"
		source           = "mysql-replica-1"
		downSchema       = "foo"
		downTable        = "bar"
		db               = "foo"
		tbl1             = "bar1"
		tbl2             = "bar2"
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111
		DDLs1            = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		DDLs2            = []string{"ALTER TABLE bar ADD COLUMN c2 INT"}
		ti0              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)
		ti2              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 INT)`)

		tables = map[string]map[string]struct{}{db: {tbl1: struct{}{}, tbl2: struct{}{}}}
		tts    = []TargetTable{newTargetTable(task, source, downSchema, downTable, tables)}
		l      = NewLock(ID, task, downSchema, downTable, ti0, tts)
	)

	// only one table exists before TrySync.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: remove a table as normal.
	// TrySync for the first table.
	DDLs, err := l.TrySync(source, db, tbl1, DDLs1, ti1, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	ready := l.Ready()
	c.Assert(ready, HasLen, 1)
	c.Assert(ready[source], HasLen, 1)
	c.Assert(ready[source][db], HasLen, 2)
	c.Assert(ready[source][db][tbl1], IsTrue)
	c.Assert(ready[source][db][tbl2], IsFalse)

	// TryRemoveTable for the second table.
	c.Assert(l.TryRemoveTable(source, db, tbl2), IsTrue)
	ready = l.Ready()
	c.Assert(ready, HasLen, 1)
	c.Assert(ready[source], HasLen, 1)
	c.Assert(ready[source][db], HasLen, 1)
	c.Assert(ready[source][db][tbl1], IsTrue)

	// CASE: remove a table will not rebuild joined schema now.
	// TrySync to add the second back.
	DDLs, err = l.TrySync(source, db, tbl2, DDLs2, ti2, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2)
	ready = l.Ready()
	c.Assert(ready, HasLen, 1)
	c.Assert(ready[source], HasLen, 1)
	c.Assert(ready[source][db], HasLen, 2)
	c.Assert(ready[source][db][tbl1], IsFalse)
	c.Assert(ready[source][db][tbl2], IsTrue)

	// TryRemoveTable for the second table.
	c.Assert(l.TryRemoveTable(source, db, tbl2), IsTrue)
	ready = l.Ready()
	c.Assert(ready, HasLen, 1)
	c.Assert(ready[source], HasLen, 1)
	c.Assert(ready[source][db], HasLen, 1)
	c.Assert(ready[source][db][tbl1], IsFalse) // the joined schema is not rebuild.

	// CASE: try to remove for not-exists table.
	c.Assert(l.TryRemoveTable(source, db, "not-exist"), IsFalse)
	c.Assert(l.TryRemoveTable(source, "not-exist", tbl1), IsFalse)
	c.Assert(l.TryRemoveTable("not-exist", db, tbl1), IsFalse)
}

func (t *testLock) TestLockTryMarkDone(c *C) {
	var (
		ID               = "test_lock_try_mark_done-`foo`.`bar`"
		task             = "test_lock_try_mark_done"
		source           = "mysql-replica-1"
		downSchema       = "foo"
		downTable        = "bar"
		db               = "foo"
		tbls             = []string{"bar1", "bar2"}
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111
		DDLs1            = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		DDLs2            = []string{"ALTER TABLE bar ADD COLUMN c1 INT", "ALTER TABLE bar ADD COLUMN c2 INT"}
		DDLs3            = []string{"ALTER TABLE bar ADD COLUMN c2 INT"}
		ti0              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)
		ti2              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 INT)`)
		ti3              = ti2

		tables = map[string]map[string]struct{}{db: {tbls[0]: struct{}{}, tbls[1]: struct{}{}}}
		tts    = []TargetTable{newTargetTable(task, source, downSchema, downTable, tables)}
		l      = NewLock(ID, task, downSchema, downTable, ti0, tts)
	)

	// the initial status is synced but not resolved.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)
	c.Assert(l.IsResolved(), IsFalse)

	// TrySync for the first table, no table has done the DDLs operation.
	DDLs, err := l.TrySync(source, db, tbls[0], DDLs1, ti1, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	t.checkLockNoDone(c, l)
	c.Assert(l.IsResolved(), IsFalse)

	// mark done for the synced table, the lock is un-resolved.
	c.Assert(l.TryMarkDone(source, db, tbls[0]), IsTrue)
	c.Assert(l.IsDone(source, db, tbls[0]), IsTrue)
	c.Assert(l.IsDone(source, db, tbls[1]), IsFalse)
	c.Assert(l.IsResolved(), IsFalse)

	// TrySync for the second table, the joined schema become larger.
	DDLs, err = l.TrySync(source, db, tbls[1], DDLs2, ti2, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2)

	// the first table is still keep `done` (for the previous DDLs operation)
	c.Assert(l.IsDone(source, db, tbls[0]), IsTrue)
	c.Assert(l.IsDone(source, db, tbls[1]), IsFalse)
	c.Assert(l.IsResolved(), IsFalse)

	// mark done for the second table, both of them are done (for different DDLs operations)
	c.Assert(l.TryMarkDone(source, db, tbls[1]), IsTrue)
	c.Assert(l.IsDone(source, db, tbls[0]), IsTrue)
	c.Assert(l.IsDone(source, db, tbls[1]), IsTrue)
	// but the lock is still not resolved because tables have different schemas.
	c.Assert(l.IsResolved(), IsFalse)

	// TrySync for the first table, all tables become synced.
	DDLs, err = l.TrySync(source, db, tbls[0], DDLs3, ti3, tts)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs3)

	// the first table become not-done, and the lock is un-resolved.
	c.Assert(l.IsDone(source, db, tbls[0]), IsFalse)
	c.Assert(l.IsDone(source, db, tbls[1]), IsTrue)
	c.Assert(l.IsResolved(), IsFalse)

	// mark done for the first table.
	c.Assert(l.TryMarkDone(source, db, tbls[0]), IsTrue)
	c.Assert(l.IsDone(source, db, tbls[0]), IsTrue)
	c.Assert(l.IsDone(source, db, tbls[1]), IsTrue)

	// the lock become resolved now.
	c.Assert(l.IsResolved(), IsTrue)

	// TryMarkDone for not-existing table take no effect.
	c.Assert(l.TryMarkDone(source, db, "not-exist"), IsFalse)
	c.Assert(l.TryMarkDone(source, "not-exist", tbls[0]), IsFalse)
	c.Assert(l.TryMarkDone("not-exist", db, tbls[0]), IsFalse)

	// check IsDone for not-existing table take no effect.
	c.Assert(l.IsDone(source, db, "not-exist"), IsFalse)
	c.Assert(l.IsDone(source, "not-exist", tbls[0]), IsFalse)
	c.Assert(l.IsDone("not-exist", db, tbls[0]), IsFalse)
}

func (t *testLock) trySyncForAllTablesLarger(c *C, l *Lock,
	DDLs []string, ti *model.TableInfo, tts []TargetTable) {
	for source, schemaTables := range l.Ready() {
		for schema, tables := range schemaTables {
			for table := range tables {
				DDLs2, err := l.TrySync(source, schema, table, DDLs, ti, tts)
				c.Assert(err, IsNil)
				c.Assert(DDLs2, DeepEquals, DDLs)
			}
		}
	}
}

func (t *testLock) checkLockSynced(c *C, l *Lock) {
	synced, remain := l.IsSynced()
	c.Assert(synced, Equals, l.synced)
	c.Assert(synced, IsTrue)
	c.Assert(remain, Equals, 0)

	ready := l.Ready()
	for _, schemaTables := range ready {
		for _, tables := range schemaTables {
			for _, synced := range tables {
				c.Assert(synced, IsTrue)
			}
		}
	}
}

func (t *testLock) checkLockNoDone(c *C, l *Lock) {
	c.Assert(l.IsResolved(), IsFalse)
	for source, schemaTables := range l.Ready() {
		for schema, tables := range schemaTables {
			for table := range tables {
				c.Assert(l.IsDone(source, schema, table), IsFalse)
			}
		}
	}
}
