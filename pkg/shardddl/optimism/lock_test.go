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
)

type testLock struct{}

var _ = Suite(&testLock{})

func (t *testLock) SetUpSuite(c *C) {
	log.InitLogger(&log.Config{})
}

func (t *testLock) TestLock(c *C) {
	var (
		ID               = "test_lock-`foo`.`bar`"
		task             = "test_lock"
		sources          = []string{"mysql-replica-1", "mysql-replica-2"}
		dbs              = []string{"db1", "db2"}
		tbls             = []string{"tbl1", "tbl2"}
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
		sts = []SourceTables{
			NewSourceTables(task, sources[0], tables),
			NewSourceTables(task, sources[1], tables),
		}

		l = NewLock(ID, task, ti0, sts)
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
				DDLs, err := l.TrySync(source, db, tbl, DDLs1, ti1, sts...)
				c.Assert(err, IsNil)
				c.Assert(DDLs, DeepEquals, DDLs1)

				syncedCount++
				synced, remain := l.IsSynced()
				c.Assert(synced, Equals, syncedCount == tableCount)
				c.Assert(remain, Equals, tableCount-syncedCount)
			}
		}
	}
	// synced again after all tables applied the DDL.
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: TrySync again after synced is idempotent.
	DDLs, err := l.TrySync(sources[0], dbs[0], tbls[0], DDLs1, ti1, sts...)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs1)
	t.checkLockSynced(c, l)
	t.checkLockNoDone(c, l)

	// CASE: need to add more than one DDL to reach the desired schema (schema become larger).
	// add two columns for one table.
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[0], DDLs2, ti2, sts...)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2)
	ready := l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[0]], IsTrue)
	c.Assert(ready[sources[0]][dbs[0]][tbls[1]], IsFalse)

	// TrySync again is idempotent (more than one DDL).
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[0], DDLs2, ti2, sts...)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2)
	ready = l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[0]], IsTrue)

	// add only the first column for another table.
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[1], DDLs2[0:1], ti2_1, sts...) // use ti2_1 info
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2[0:1])
	ready = l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[0]], IsTrue)
	c.Assert(ready[sources[0]][dbs[0]][tbls[1]], IsFalse)
	synced, remain := l.IsSynced()
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, tableCount-1)
	cmp, err := l.tables[sources[0]][dbs[0]][tbls[0]].Compare(l.tables[sources[0]][dbs[0]][tbls[1]])
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 1)

	// TrySync again (only the first DDL).
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[1], DDLs2[0:1], ti2_1, sts...)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{}) // NOTE: special case, joined has larger schema.
	ready = l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[1]], IsFalse)

	// add the second column for another table.
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[1], DDLs2[1:2], ti2, sts...) // use ti2 info.
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2[1:2])
	ready = l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[1]], IsTrue) // ready now.
	synced, remain = l.IsSynced()
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, tableCount-2)
	cmp, err = l.tables[sources[0]][dbs[0]][tbls[0]].Compare(l.tables[sources[0]][dbs[0]][tbls[1]])
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	// Try again (for the second DDL).
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[1], DDLs2[1:2], ti2, sts...)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, DDLs2[1:2])

	// try add columns for all tables to reach the same schema.
	t.trySyncForAllTablesLarger(c, l, DDLs2, ti2, sts...)
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
				DDLs, err = l.TrySync(source, db, tbl, DDLs3, ti3, sts...)
				c.Assert(err, IsNil)
				synced, remain = l.IsSynced()
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
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[0], DDLs4, ti4, sts...)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{})
	ready = l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[0]], IsFalse)
	c.Assert(ready[sources[0]][dbs[0]][tbls[1]], IsTrue)

	// TrySync again is idempotent.
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[0], DDLs4, ti4, sts...)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{})
	ready = l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[0]], IsFalse)

	// drop only the first column for another table.
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[1], DDLs4[0:1], ti4_1, sts...)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{})
	ready = l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[0]], IsFalse)
	c.Assert(ready[sources[0]][dbs[0]][tbls[1]], IsFalse)
	cmp, err = l.tables[sources[0]][dbs[0]][tbls[0]].Compare(l.tables[sources[0]][dbs[0]][tbls[1]])
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// TrySync again (only the first DDL).
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[1], DDLs4[0:1], ti4_1, sts...)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{})

	// drop the second column for another table.
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[1], DDLs4[1:2], ti4, sts...) // use ti4 info.
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{})
	ready = l.Ready()
	c.Assert(ready[sources[0]][dbs[0]][tbls[0]], IsFalse)
	c.Assert(ready[sources[0]][dbs[0]][tbls[1]], IsFalse)
	cmp, err = l.tables[sources[0]][dbs[0]][tbls[0]].Compare(l.tables[sources[0]][dbs[0]][tbls[1]])
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	// TrySync again (for the second DDL).
	DDLs, err = l.TrySync(sources[0], dbs[0], tbls[1], DDLs4[1:2], ti4, sts...)
	c.Assert(err, IsNil)
	c.Assert(DDLs, DeepEquals, []string{})

	// try drop columns for other tables to reach the same schema.
	remain = tableCount - 2
	for source, schemaTables := range l.Ready() {
		for schema, tables := range schemaTables {
			for table, synced2 := range tables {
				if synced2 { // do not `TrySync` again for previous two (un-synced now).
					DDLs, err = l.TrySync(source, schema, table, DDLs4, ti4, sts...)
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

func (t *testLock) trySyncForAllTablesLarger(c *C, l *Lock,
	DDLs []string, ti *model.TableInfo, sts ...SourceTables) {
	for source, schemaTables := range l.Ready() {
		for schema, tables := range schemaTables {
			for table := range tables {
				DDLs2, err := l.TrySync(source, schema, table, DDLs, ti, sts...)
				c.Assert(err, IsNil)
				c.Assert(DDLs2, DeepEquals, DDLs)
			}
		}
	}
}

func (t *testLock) checkLockSynced(c *C, l *Lock) {
	synced, remain := l.IsSynced()
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
