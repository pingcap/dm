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
	c.Assert(log.InitLogger(&log.Config{}), IsNil)
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

		vers = map[string]map[string]map[string]int64{
			sources[0]: {
				dbs[0]: {tbls[0]: 0, tbls[1]: 0},
				dbs[1]: {tbls[0]: 0, tbls[1]: 0},
			},
			sources[1]: {
				dbs[0]: {tbls[0]: 0, tbls[1]: 0},
				dbs[1]: {tbls[0]: 0, tbls[1]: 0},
			},
		}
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
				vers[source][db][tbl]++
				DDLs, err := l.TrySync(source, db, tbl, DDLs1, []*model.TableInfo{ti1}, tts, vers[source][db][tbl])
				c.Assert(err, IsNil)
				c.Assert(DDLs, DeepEquals, DDLs1)
				c.Assert(l.versions, DeepEquals, vers)

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
	vers[sources[0]][dbs[0]][tbls[0]]++
	DDLs, err := l.TrySync(sources[0], dbs[0], tbls[0], DDLs1, []*model.TableInfo{ti1}, tts, vers[sources[0]][dbs[0]][tbls[0]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	c.Assert(l.versions, DeepEquals, vers)
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: need to add more than one DDL to reach the desired schema (schema become larger).
	// add two columns for one table.
	vers[sources[0]][dbs[0]][tbls[0]]++
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[0], DDLs2, []*model.TableInfo{ti2_1, ti2}, tts, vers[sources[0]][dbs[0]][tbls[0]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2)
	c.Assert(l.versions, DeepEquals, vers)
	ready := l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[0]], IsTrue)
	c.Assert(ready[sources[0]][dbs[0]][tbls[1]], IsFalse)

	// TrySync again is idempotent (more than one DDL).
	vers[sources[0]][dbs[0]][tbls[0]]++
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[0], DDLs2, []*model.TableInfo{ti2_1, ti2}, tts, vers[sources[0]][dbs[0]][tbls[0]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2)
	c.Assert(l.versions, DeepEquals, vers)
	ready = l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[0]], IsTrue)
	c.Assert(ready[sources[0]][dbs[0]][tbls[1]], IsFalse)

	// add only the first column for another table.
	vers[sources[0]][dbs[0]][tbls[1]]++
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[1], DDLs2[0:1], []*model.TableInfo{ti2_1}, tts, vers[sources[0]][dbs[0]][tbls[1]]) // use ti2_1 info
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2[0:1])
	c.Assert(l.versions, DeepEquals, vers)
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
	vers[sources[0]][dbs[0]][tbls[1]]++
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[1], DDLs2[0:1], []*model.TableInfo{ti2_1}, tts, vers[sources[0]][dbs[0]][tbls[1]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{}) // NOTE: special case, joined has larger schema.
	c.Assert(l.versions, DeepEquals, vers)
	ready = l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[1]], IsFalse)

	// add the second column for another table.
	vers[sources[0]][dbs[0]][tbls[1]]++
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[1], DDLs2[1:2], []*model.TableInfo{ti2}, tts, vers[sources[0]][dbs[0]][tbls[1]]) // use ti2 info.
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2[1:2])
	c.Assert(l.versions, DeepEquals, vers)
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
	vers[sources[0]][dbs[0]][tbls[1]]++
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[1], DDLs2[1:2], []*model.TableInfo{ti2}, tts, vers[sources[0]][dbs[0]][tbls[1]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2[1:2])
	c.Assert(l.versions, DeepEquals, vers)

	// try add columns for all tables to reach the same schema.
	resultDDLs := map[string]map[string]map[string][]string{
		sources[0]: {
			dbs[0]: {tbls[0]: DDLs2[1:], tbls[1]: DDLs2[1:]},
			dbs[1]: {tbls[0]: DDLs2, tbls[1]: DDLs2},
		},
		sources[1]: {
			dbs[0]: {tbls[0]: DDLs2, tbls[1]: DDLs2},
			dbs[1]: {tbls[0]: DDLs2, tbls[1]: DDLs2},
		},
	}
	t.trySyncForAllTablesLarger(c, l, DDLs2, []*model.TableInfo{ti2_1, ti2}, tts, vers, resultDDLs)
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
				vers[source][db][tbl]++
				DDLs, err = l.TrySync(source, db, tbl, DDLs3, []*model.TableInfo{ti3}, tts, vers[source][db][tbl])
				c.Assert(err, IsNil)
				c.Assert(l.versions, DeepEquals, vers)
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
	vers[sources[0]][dbs[0]][tbls[0]]++
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[0], DDLs4, []*model.TableInfo{ti4_1, ti4}, tts, vers[sources[0]][dbs[0]][tbls[0]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	ready = l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[0]], IsFalse)
	c.Assert(ready[sources[0]][dbs[0]][tbls[1]], IsTrue)

	// TrySync again is idempotent.
	vers[sources[0]][dbs[0]][tbls[0]]++
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[0], DDLs4, []*model.TableInfo{ti4_1, ti4}, tts, vers[sources[0]][dbs[0]][tbls[0]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs4[:1])
	c.Assert(l.versions, DeepEquals, vers)
	ready = l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[0]], IsFalse)
	c.Assert(ready[sources[0]][dbs[0]][tbls[1]], IsTrue)

	// drop only the first column for another table.
	vers[sources[0]][dbs[0]][tbls[1]]++
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[1], DDLs4[0:1], []*model.TableInfo{ti4_1}, tts, vers[sources[0]][dbs[0]][tbls[1]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	ready = l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[0]], IsFalse)
	c.Assert(ready[sources[0]][dbs[0]][tbls[1]], IsFalse)
	cmp, err = l.tables[sources[0]][dbs[0]][tbls[0]].Compare(l.tables[sources[0]][dbs[0]][tbls[1]])
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// TrySync again (only the first DDL).
	vers[sources[0]][dbs[0]][tbls[1]]++
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[1], DDLs4[0:1], []*model.TableInfo{ti4_1}, tts, vers[sources[0]][dbs[0]][tbls[1]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)

	// drop the second column for another table.
	vers[sources[0]][dbs[0]][tbls[1]]++
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[1], DDLs4[1:2], []*model.TableInfo{ti4}, tts, vers[sources[0]][dbs[0]][tbls[1]]) // use ti4 info.
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	ready = l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[0]], IsFalse)
	c.Assert(ready[sources[0]][dbs[0]][tbls[1]], IsFalse)
	cmp, err = l.tables[sources[0]][dbs[0]][tbls[0]].Compare(l.tables[sources[0]][dbs[0]][tbls[1]])
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	// TrySync again (for the second DDL).
	vers[sources[0]][dbs[0]][tbls[1]]++
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[1], DDLs4[1:2], []*model.TableInfo{ti4}, tts, vers[sources[0]][dbs[0]][tbls[1]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)

	// try drop columns for other tables to reach the same schema.
	remain = tableCount - 2
	for source, schemaTables := range l.Ready() {
		for schema, tables := range schemaTables {
			for table, synced2 := range tables {
				if synced2 { // do not `TrySync` again for previous two (un-synced now).
					DDLs, err = l.TrySync(source, schema, table, DDLs4, []*model.TableInfo{ti4_1, ti4}, tts, vers[source][schema][table])
					c.Assert(err, IsNil)
					c.Assert(l.versions, DeepEquals, vers)
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

		vers = map[string]map[string]map[string]int64{
			source: {
				db: {tbls[0]: 0, tbls[1]: 0},
			},
		}
	)

	// the initial status is synced.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// try sync for one table, `DROP INDEX` returned directly (to make schema become more compatible).
	// `DROP INDEX` is handled like `ADD COLUMN`.
	vers[source][db][tbls[0]]++
	DDLs, err := l.TrySync(source, db, tbls[0], DDLs1, []*model.TableInfo{ti1}, tts, vers[source][db][tbls[0]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	c.Assert(l.versions, DeepEquals, vers)
	synced, remain := l.IsSynced()
	c.Assert(synced, Equals, l.synced)
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 1)

	// try sync for another table, also got `DROP INDEX` now.
	vers[source][db][tbls[1]]++
	DDLs, err = l.TrySync(source, db, tbls[1], DDLs1, []*model.TableInfo{ti1}, tts, vers[source][db][tbls[1]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	c.Assert(l.versions, DeepEquals, vers)
	t.checkLockSynced(c, l)

	// try sync for one table, `ADD INDEX` not returned directly (to keep the schema more compatible).
	// `ADD INDEX` is handled like `DROP COLUMN`.
	vers[source][db][tbls[0]]++
	DDLs, err = l.TrySync(source, db, tbls[0], DDLs2, []*model.TableInfo{ti2}, tts, vers[source][db][tbls[0]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{}) // no DDLs returned
	c.Assert(l.versions, DeepEquals, vers)
	synced, remain = l.IsSynced()
	c.Assert(synced, Equals, l.synced)
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 1)

	// try sync for another table, got `ADD INDEX` now.
	vers[source][db][tbls[1]]++
	DDLs, err = l.TrySync(source, db, tbls[1], DDLs2, []*model.TableInfo{ti2}, tts, vers[source][db][tbls[1]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2)
	c.Assert(l.versions, DeepEquals, vers)
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

		vers = map[string]map[string]map[string]int64{
			source: {
				db: {tbls[0]: 0, tbls[1]: 0},
			},
		}
	)

	// the initial status is synced.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	for i := 0; i < 2; i++ { // two round
		// try sync for one table, from `NULL` to `NOT NULL`, no DDLs returned.
		vers[source][db][tbls[0]]++
		DDLs, err := l.TrySync(source, db, tbls[0], DDLs1, []*model.TableInfo{ti1}, tts, vers[source][db][tbls[0]])
		c.Assert(err, IsNil)
		c.Assert(DDLs, DeepEquals, []string{})
		c.Assert(l.versions, DeepEquals, vers)

		// try sync for another table, DDLs returned.
		vers[source][db][tbls[1]]++
		DDLs, err = l.TrySync(source, db, tbls[1], DDLs1, []*model.TableInfo{ti1}, tts, vers[source][db][tbls[1]])
		c.Assert(err, IsNil)
		c.Assert(DDLs, DeepEquals, DDLs1)
		c.Assert(l.versions, DeepEquals, vers)

		// try sync for one table, from `NOT NULL` to `NULL`, DDLs returned.
		vers[source][db][tbls[0]]++
		DDLs, err = l.TrySync(source, db, tbls[0], DDLs2, []*model.TableInfo{ti2}, tts, vers[source][db][tbls[0]])
		c.Assert(err, IsNil)
		c.Assert(DDLs, DeepEquals, DDLs2)
		c.Assert(l.versions, DeepEquals, vers)

		// try sync for another table, from `NOT NULL` to `NULL`, DDLs, returned.
		vers[source][db][tbls[1]]++
		DDLs, err = l.TrySync(source, db, tbls[1], DDLs2, []*model.TableInfo{ti2}, tts, vers[source][db][tbls[1]])
		c.Assert(err, IsNil)
		c.Assert(DDLs, DeepEquals, DDLs2)
		c.Assert(l.versions, DeepEquals, vers)
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

		vers = map[string]map[string]map[string]int64{
			source: {
				db: {tbls[0]: 0, tbls[1]: 0},
			},
		}
	)

	// the initial status is synced.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// try sync for one table, from `INT` to `BIGINT`, DDLs returned.
	vers[source][db][tbls[0]]++
	DDLs, err := l.TrySync(source, db, tbls[0], DDLs1, []*model.TableInfo{ti1}, tts, vers[source][db][tbls[0]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	c.Assert(l.versions, DeepEquals, vers)

	// try sync for another table, DDLs returned.
	vers[source][db][tbls[1]]++
	DDLs, err = l.TrySync(source, db, tbls[1], DDLs1, []*model.TableInfo{ti1}, tts, vers[source][db][tbls[1]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	c.Assert(l.versions, DeepEquals, vers)
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

		vers = map[string]map[string]map[string]int64{
			source: {
				db: {tbls[0]: 0, tbls[1]: 0},
			},
		}
	)

	// the initial status is synced.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// try sync for one table.
	vers[source][db][tbls[0]]++
	DDLs, err := l.TrySync(source, db, tbls[0], DDLs1, []*model.TableInfo{ti1}, tts, vers[source][db][tbls[0]])
	c.Assert(terror.ErrShardDDLOptimismTrySyncFail.Equal(err), IsTrue)
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
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
		vers   = map[string]map[string]map[string]int64{
			source1: {
				db1: {tbl1: 0},
			},
			source2: {
				db2: {tbl2: 0},
			},
		}
	)

	// only one table exists before TrySync.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// TrySync for a new table as the caller.
	vers[source2][db2][tbl2]++
	DDLs, err := l.TrySync(source2, db2, tbl2, DDLs1, []*model.TableInfo{ti1}, tts, vers[source2][db2][tbl2])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	c.Assert(l.versions, DeepEquals, vers)

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
	vers[source1][db1][tbl2] = 0
	vers[source2][db2][tbl1] = 0

	vers[source1][db1][tbl1]++
	DDLs, err = l.TrySync(source1, db1, tbl1, DDLs1, []*model.TableInfo{ti1}, tts, vers[source1][db1][tbl1])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	c.Assert(l.versions, DeepEquals, vers)

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

		vers = map[string]map[string]map[string]int64{
			source: {
				db: {tbls[0]: 0, tbls[1]: 0},
			},
		}
	)

	// the initial status is synced.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: revert for single DDL.
	// TrySync for one table.
	vers[source][db][tbls[0]]++
	DDLs, err := l.TrySync(source, db, tbls[0], DDLs1, []*model.TableInfo{ti1}, tts, vers[source][db][tbls[0]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	c.Assert(l.versions, DeepEquals, vers)
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
	vers[source][db][tbls[0]]++
	DDLs, err = l.TrySync(source, db, tbls[0], DDLs2, []*model.TableInfo{ti2}, tts, vers[source][db][tbls[0]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2)
	c.Assert(l.versions, DeepEquals, vers)
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: revert for multiple DDLs.
	// TrySync for one table.
	vers[source][db][tbls[0]]++
	DDLs, err = l.TrySync(source, db, tbls[0], DDLs3, []*model.TableInfo{ti4, ti3}, tts, vers[source][db][tbls[0]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs3)
	c.Assert(l.versions, DeepEquals, vers)
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
	vers[source][db][tbls[0]]++
	DDLs, err = l.TrySync(source, db, tbls[0], DDLs4, []*model.TableInfo{ti4}, tts, vers[source][db][tbls[0]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs4)
	c.Assert(l.versions, DeepEquals, vers)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[1]], IsFalse)
	cmp, err = l.tables[source][db][tbls[0]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// revert the reset part of the DDLs.
	vers[source][db][tbls[0]]++
	DDLs, err = l.TrySync(source, db, tbls[0], DDLs5, []*model.TableInfo{ti5}, tts, vers[source][db][tbls[0]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs5)
	c.Assert(l.versions, DeepEquals, vers)
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: revert part of multiple DDLs.
	// TrySync for one table.
	vers[source][db][tbls[0]]++
	DDLs, err = l.TrySync(source, db, tbls[0], DDLs6, []*model.TableInfo{ti7, ti6}, tts, vers[source][db][tbls[0]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs6)
	c.Assert(l.versions, DeepEquals, vers)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[1]], IsFalse)
	cmp, err = l.tables[source][db][tbls[0]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// revert part of the DDLs.
	vers[source][db][tbls[0]]++
	DDLs, err = l.TrySync(source, db, tbls[0], DDLs7, []*model.TableInfo{ti7}, tts, vers[source][db][tbls[0]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs7)
	c.Assert(l.versions, DeepEquals, vers)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[1]], IsFalse)
	cmp, err = l.tables[source][db][tbls[0]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// TrySync for another table.
	vers[source][db][tbls[1]]++
	DDLs, err = l.TrySync(source, db, tbls[1], DDLs8, []*model.TableInfo{ti8}, tts, vers[source][db][tbls[1]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs8)
	c.Assert(l.versions, DeepEquals, vers)
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
		ti2_1            = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 DATETIME)`)
		ti3              = ti0
		ti4              = ti2
		ti4_1            = ti2_1

		tables = map[string]map[string]struct{}{db: {tbls[0]: struct{}{}, tbls[1]: struct{}{}}}
		tts    = []TargetTable{newTargetTable(task, source, downSchema, downTable, tables)}
		l      = NewLock(ID, task, downSchema, downTable, ti0, tts)

		vers = map[string]map[string]map[string]int64{
			source: {
				db: {tbls[0]: 0, tbls[1]: 0},
			},
		}
	)

	// the initial status is synced.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// TrySync for the first table, construct the joined schema.
	vers[source][db][tbls[0]]++
	DDLs, err := l.TrySync(source, db, tbls[0], DDLs1, []*model.TableInfo{ti1}, tts, vers[source][db][tbls[0]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	c.Assert(l.versions, DeepEquals, vers)
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
	vers[source][db][tbls[1]]++
	DDLs, err = l.TrySync(source, db, tbls[1], DDLs2, []*model.TableInfo{ti2_1, ti2}, tts, vers[source][db][tbls[1]])
	c.Assert(terror.ErrShardDDLOptimismTrySyncFail.Equal(err), IsTrue)
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
	c.Assert(err, ErrorMatches, ".*at tuple index.*")
	c.Assert(cmp, Equals, 0)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[1]], IsFalse)

	// TrySync for the first table to resolve the conflict.
	vers[source][db][tbls[0]]++
	DDLs, err = l.TrySync(source, db, tbls[0], DDLs3, []*model.TableInfo{ti3}, tts, vers[source][db][tbls[0]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs3)
	c.Assert(l.versions, DeepEquals, vers)
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
	vers[source][db][tbls[0]]++
	DDLs, err = l.TrySync(source, db, tbls[0], DDLs4, []*model.TableInfo{ti4_1, ti4}, tts, vers[source][db][tbls[0]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs4)
	c.Assert(l.versions, DeepEquals, vers)
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
		ti6_1   = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT, c2 DATETIME)`)
		ti7     = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT, c3 INT)`)
		ti8     = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT, c2 TEXT, c3 INT)`)

		tables = map[string]map[string]struct{}{db: {tbls[0]: struct{}{}, tbls[1]: struct{}{}}}
		tts    = []TargetTable{newTargetTable(task, source, downSchema, downTable, tables)}
		l      = NewLock(ID, task, downSchema, downTable, ti0, tts)

		vers = map[string]map[string]map[string]int64{
			source: {
				db: {tbls[0]: 0, tbls[1]: 0},
			},
		}
	)

	// the initial status is synced.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: conflict happen, revert all changes to resolve the conflict.
	// TrySync for the first table, construct the joined schema.
	vers[source][db][tbls[0]]++
	DDLs, err := l.TrySync(source, db, tbls[0], DDLs1, []*model.TableInfo{ti1}, tts, vers[source][db][tbls[0]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	c.Assert(l.versions, DeepEquals, vers)
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
	vers[source][db][tbls[1]]++
	DDLs, err = l.TrySync(source, db, tbls[1], DDLs2, []*model.TableInfo{ti3, ti2}, tts, vers[source][db][tbls[1]])
	c.Assert(terror.ErrShardDDLOptimismTrySyncFail.Equal(err), IsTrue)
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
	c.Assert(err, ErrorMatches, ".*at tuple index.*")
	c.Assert(cmp, Equals, 0)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[1]], IsFalse)

	// TrySync again.
	vers[source][db][tbls[1]]++
	DDLs, err = l.TrySync(source, db, tbls[1], DDLs2, []*model.TableInfo{ti3, ti2}, tts, vers[source][db][tbls[1]])
	c.Assert(terror.ErrShardDDLOptimismTrySyncFail.Equal(err), IsTrue)
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
	c.Assert(err, ErrorMatches, ".*at tuple index.*")
	c.Assert(cmp, Equals, 0)

	// TrySync for the second table to drop the non-conflict column, the conflict should still exist.
	vers[source][db][tbls[1]]++
	DDLs, err = l.TrySync(source, db, tbls[1], DDLs3, []*model.TableInfo{ti3}, tts, vers[source][db][tbls[1]])
	c.Assert(terror.ErrShardDDLOptimismTrySyncFail.Equal(err), IsTrue)
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
	c.Assert(err, ErrorMatches, ".*at tuple index.*")
	c.Assert(cmp, Equals, 0)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[1]], IsFalse)

	// TrySync for the second table to drop the conflict column, the conflict should be resolved.
	vers[source][db][tbls[1]]++
	DDLs, err = l.TrySync(source, db, tbls[1], DDLs4, []*model.TableInfo{ti4}, tts, vers[source][db][tbls[1]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{})
	c.Assert(l.versions, DeepEquals, vers)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[1]], IsFalse)

	// TrySync for the second table as we did for the first table, the lock should be synced.
	vers[source][db][tbls[1]]++
	DDLs, err = l.TrySync(source, db, tbls[1], DDLs1, []*model.TableInfo{ti1}, tts, vers[source][db][tbls[1]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs)
	c.Assert(l.versions, DeepEquals, vers)
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: conflict happen, revert part of changes to resolve the conflict.
	// TrySync for the first table, construct the joined schema.
	vers[source][db][tbls[0]]++
	DDLs, err = l.TrySync(source, db, tbls[0], DDLs5, []*model.TableInfo{ti5}, tts, vers[source][db][tbls[0]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs5)
	c.Assert(l.versions, DeepEquals, vers)
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
	vers[source][db][tbls[1]]++
	DDLs, err = l.TrySync(source, db, tbls[1], DDLs6, []*model.TableInfo{ti6_1, ti6}, tts, vers[source][db][tbls[1]])
	c.Assert(terror.ErrShardDDLOptimismTrySyncFail.Equal(err), IsTrue)
	c.Assert(DDLs, DeepEquals, []string{})
	cmp, err = l.tables[source][db][tbls[1]].Compare(l.Joined())
	c.Assert(err, ErrorMatches, ".*at tuple index.*")
	c.Assert(cmp, Equals, 0)
	c.Assert(l.versions, DeepEquals, vers)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[1]], IsFalse)

	// TrySync for the second table to drop the conflict column, the conflict should be resolved.
	// but both of tables are not synced now.
	vers[source][db][tbls[1]]++
	DDLs, err = l.TrySync(source, db, tbls[1], DDLs7, []*model.TableInfo{ti7}, tts, vers[source][db][tbls[1]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs7) // special case: these DDLs should not be replicated to the downstream.
	c.Assert(l.versions, DeepEquals, vers)
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
	vers[source][db][tbls[0]]++
	DDLs, err = l.TrySync(source, db, tbls[0], DDLs8_1, []*model.TableInfo{ti8}, tts, vers[source][db][tbls[0]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs8_1)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[0]], IsTrue)

	// TrySync for the second table to become synced.
	vers[source][db][tbls[1]]++
	DDLs, err = l.TrySync(source, db, tbls[1], DDLs8_2, []*model.TableInfo{ti8}, tts, vers[source][db][tbls[1]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs8_2)
	ready = l.Ready()
	c.Assert(ready[source][db][tbls[1]], IsTrue)

	// all tables synced now.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)
}

func (t *testLock) TestLockTrySyncMultipleChangeDDL(c *C) {
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
		DDLs1            = []string{"ALTER TABLE bar ADD COLUMN c2 INT", "ALTER TABLE DROP COLUMN c1"}
		DDLs2            = []string{"ALTER TABLE bar DROP COLUMN c2", "ALTER TABLE bar ADD COLUMN c3 TEXT"}
		//		DDLs3            = []string{"ALTER TABLE bar DROP COLUMN c3"}
		//		DDLs4            = []string{"ALTER TABLE bar DROP COLUMN c2", "ALTER TABLE bar DROP COLUMN c1"}
		ti0   = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)
		ti1_1 = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 INT)`)
		ti1   = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c2 INT)`)
		ti2   = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c3 TEXT)`)
		ti2_1 = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		//		ti3              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 BIGINT)`)
		//		ti4              = ti0
		//		ti4_1            = ti1
		tables = map[string]map[string]struct{}{
			dbs[0]: {tbls[0]: struct{}{}, tbls[1]: struct{}{}},
			dbs[1]: {tbls[0]: struct{}{}, tbls[1]: struct{}{}},
		}
		tts = []TargetTable{
			newTargetTable(task, sources[0], downSchema, downTable, tables),
			newTargetTable(task, sources[1], downSchema, downTable, tables),
		}

		l = NewLock(ID, task, downSchema, downTable, ti0, tts)

		vers = map[string]map[string]map[string]int64{
			sources[0]: {
				dbs[0]: {tbls[0]: 0, tbls[1]: 0},
				dbs[1]: {tbls[0]: 0, tbls[1]: 0},
			},
			sources[1]: {
				dbs[0]: {tbls[0]: 0, tbls[1]: 0},
				dbs[1]: {tbls[0]: 0, tbls[1]: 0},
			},
		}
	)

	// the initial status is synced.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// inconsistent ddls and table infos
	vers[sources[0]][dbs[0]][tbls[0]]++
	DDLs, err := l.TrySync(sources[0], dbs[0], tbls[0], DDLs1[:1], []*model.TableInfo{ti1_1, ti1}, tts, vers[sources[0]][dbs[0]][tbls[0]])
	c.Assert(DDLs, DeepEquals, DDLs1[:1])
	c.Assert(terror.ErrMasterInconsistentOptimisticDDLsAndInfo.Equal(err), IsTrue)

	vers[sources[0]][dbs[0]][tbls[0]]++
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[0], DDLs1, []*model.TableInfo{ti1}, tts, vers[sources[0]][dbs[0]][tbls[0]])
	c.Assert(DDLs, DeepEquals, DDLs1)
	c.Assert(terror.ErrMasterInconsistentOptimisticDDLsAndInfo.Equal(err), IsTrue)

	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: all tables execute a same multiple change DDLs1
	syncedCount := 0
	resultDDLs1 := map[string]map[string]map[string][]string{
		sources[0]: {
			dbs[0]: {tbls[0]: DDLs1[:1], tbls[1]: DDLs1[:1]},
			dbs[1]: {tbls[0]: DDLs1[:1], tbls[1]: DDLs1[:1]},
		},
		sources[1]: {
			dbs[0]: {tbls[0]: DDLs1[:1], tbls[1]: DDLs1[:1]},
			dbs[1]: {tbls[0]: DDLs1[:1], tbls[1]: DDLs1}, // only last table sync DROP COLUMN
		},
	}
	for _, source := range sources {
		for _, db := range dbs {
			for _, tbl := range tbls {
				vers[source][db][tbl]++
				DDLs, err = l.TrySync(source, db, tbl, DDLs1, []*model.TableInfo{ti1_1, ti1}, tts, vers[source][db][tbl])
				c.Assert(err, IsNil)
				c.Assert(DDLs, DeepEquals, resultDDLs1[source][db][tbl])
				c.Assert(l.versions, DeepEquals, vers)

				syncedCount++
				synced, _ := l.IsSynced()
				c.Assert(synced, Equals, syncedCount == tableCount)
				c.Assert(synced, Equals, l.synced)
			}
		}
	}
	// synced again after all tables applied the DDL.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: TrySync again after synced is idempotent.
	// both ddl will sync again
	vers[sources[0]][dbs[0]][tbls[0]]++
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[0], DDLs1, []*model.TableInfo{ti1_1, ti1}, tts, vers[sources[0]][dbs[0]][tbls[0]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	c.Assert(l.versions, DeepEquals, vers)
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: all tables execute a same multiple change DDLs2
	syncedCount = 0
	resultDDLs2 := map[string]map[string]map[string][]string{
		sources[0]: {
			dbs[0]: {tbls[0]: DDLs2[1:], tbls[1]: DDLs2[1:]},
			dbs[1]: {tbls[0]: DDLs2[1:], tbls[1]: DDLs2[1:]},
		},
		sources[1]: {
			dbs[0]: {tbls[0]: DDLs2[1:], tbls[1]: DDLs2[1:]},
			dbs[1]: {tbls[0]: DDLs2[1:], tbls[1]: DDLs2}, // only last table sync DROP COLUMN
		},
	}
	for _, source := range sources {
		for _, db := range dbs {
			for _, tbl := range tbls {
				vers[source][db][tbl]++
				DDLs, err = l.TrySync(source, db, tbl, DDLs2, []*model.TableInfo{ti2_1, ti2}, tts, vers[source][db][tbl])
				c.Assert(err, IsNil)
				c.Assert(DDLs, DeepEquals, resultDDLs2[source][db][tbl])
				c.Assert(l.versions, DeepEquals, vers)

				syncedCount++
				synced, _ := l.IsSynced()
				c.Assert(synced, Equals, syncedCount == tableCount)
				c.Assert(synced, Equals, l.synced)
			}
		}
	}
	// synced again after all tables applied the DDL.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: TrySync again after synced is idempotent.
	// only the second ddl(ADD COLUMN) will sync, the first one(DROP COLUMN) will not sync since oldJoined==newJoined
	vers[sources[0]][dbs[0]][tbls[0]]++
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[0], DDLs2, []*model.TableInfo{ti2_1, ti2}, tts, vers[sources[0]][dbs[0]][tbls[0]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2[1:])
	c.Assert(l.versions, DeepEquals, vers)
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

		vers = map[string]map[string]map[string]int64{
			source: {
				db: {tbl1: 0, tbl2: 0},
			},
		}
	)

	// only one table exists before TrySync.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: remove a table as normal.
	// TrySync for the first table.
	vers[source][db][tbl1]++
	DDLs, err := l.TrySync(source, db, tbl1, DDLs1, []*model.TableInfo{ti1}, tts, vers[source][db][tbl1])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	c.Assert(l.versions, DeepEquals, vers)
	ready := l.Ready()
	c.Assert(ready, HasLen, 1)
	c.Assert(ready[source], HasLen, 1)
	c.Assert(ready[source][db], HasLen, 2)
	c.Assert(ready[source][db][tbl1], IsTrue)
	c.Assert(ready[source][db][tbl2], IsFalse)

	// TryRemoveTable for the second table.
	c.Assert(l.TryRemoveTable(source, db, tbl2), IsTrue)
	delete(vers[source][db], tbl2)
	ready = l.Ready()
	c.Assert(ready, HasLen, 1)
	c.Assert(ready[source], HasLen, 1)
	c.Assert(ready[source][db], HasLen, 1)
	c.Assert(ready[source][db][tbl1], IsTrue)
	c.Assert(l.versions, DeepEquals, vers)

	// CASE: remove a table will not rebuild joined schema now.
	// TrySync to add the second back.
	vers[source][db][tbl2] = 1
	DDLs, err = l.TrySync(source, db, tbl2, DDLs2, []*model.TableInfo{ti2}, tts, vers[source][db][tbl1])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2)
	c.Assert(l.versions, DeepEquals, vers)
	ready = l.Ready()
	c.Assert(ready, HasLen, 1)
	c.Assert(ready[source], HasLen, 1)
	c.Assert(ready[source][db], HasLen, 2)
	c.Assert(ready[source][db][tbl1], IsFalse)
	c.Assert(ready[source][db][tbl2], IsTrue)

	// TryRemoveTable for the second table.
	c.Assert(l.TryRemoveTable(source, db, tbl2), IsTrue)
	delete(vers[source][db], tbl2)
	ready = l.Ready()
	c.Assert(ready, HasLen, 1)
	c.Assert(ready[source], HasLen, 1)
	c.Assert(ready[source][db], HasLen, 1)
	c.Assert(ready[source][db][tbl1], IsFalse) // the joined schema is not rebuild.
	c.Assert(l.versions, DeepEquals, vers)

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

		vers = map[string]map[string]map[string]int64{
			source: {
				db: {tbls[0]: 0, tbls[1]: 0},
			},
		}
	)

	// the initial status is synced but not resolved.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)
	c.Assert(l.IsResolved(), IsFalse)

	// TrySync for the first table, no table has done the DDLs operation.
	vers[source][db][tbls[0]]++
	DDLs, err := l.TrySync(source, db, tbls[0], DDLs1, []*model.TableInfo{ti1}, tts, vers[source][db][tbls[0]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	c.Assert(l.versions, DeepEquals, vers)
	t.checkLockNoDone(c, l)
	c.Assert(l.IsResolved(), IsFalse)

	// mark done for the synced table, the lock is un-resolved.
	c.Assert(l.TryMarkDone(source, db, tbls[0]), IsTrue)
	c.Assert(l.IsDone(source, db, tbls[0]), IsTrue)
	c.Assert(l.IsDone(source, db, tbls[1]), IsFalse)
	c.Assert(l.IsResolved(), IsFalse)

	// TrySync for the second table, the joined schema become larger.
	vers[source][db][tbls[1]]++
	DDLs, err = l.TrySync(source, db, tbls[1], DDLs2, []*model.TableInfo{ti1, ti2}, tts, vers[source][db][tbls[1]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2)
	c.Assert(l.versions, DeepEquals, vers)

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
	vers[source][db][tbls[0]]++
	DDLs, err = l.TrySync(source, db, tbls[0], DDLs3, []*model.TableInfo{ti3}, tts, vers[source][db][tbls[0]])
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs3)
	c.Assert(l.versions, DeepEquals, vers)

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
	DDLs []string, tis []*model.TableInfo, tts []TargetTable, vers map[string]map[string]map[string]int64, resultDDLs map[string]map[string]map[string][]string) {
	for source, schemaTables := range l.Ready() {
		for schema, tables := range schemaTables {
			for table := range tables {
				DDLs2, err := l.TrySync(source, schema, table, DDLs, tis, tts, vers[source][schema][table])
				c.Assert(err, IsNil)
				c.Assert(DDLs2, DeepEquals, resultDDLs[source][schema][table])
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
