// Copyright 2019 PingCAP, Inc.
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

package schema_test

import (
	"context"
	"encoding/json"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/dm/pkg/schema"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"go.uber.org/zap/zapcore"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&trackerSuite{})

type trackerSuite struct{}

func (s *trackerSuite) TestDDL(c *C) {
	log.SetLevel(zapcore.ErrorLevel)

	tracker, err := schema.NewTracker()
	c.Assert(err, IsNil)

	// Table shouldn't exist before initialization.
	_, err = tracker.GetTable("testdb", "foo")
	c.Assert(err, ErrorMatches, `.*Table 'testdb\.foo' doesn't exist`)
	c.Assert(schema.IsTableNotExists(err), IsTrue)

	ctx := context.Background()
	err = tracker.Exec(ctx, "", "create database testdb;")
	c.Assert(err, IsNil)

	_, err = tracker.GetTable("testdb", "foo")
	c.Assert(err, ErrorMatches, `.*Table 'testdb\.foo' doesn't exist`)
	c.Assert(schema.IsTableNotExists(err), IsTrue)

	// Now create the table with 3 columns.
	err = tracker.Exec(ctx, "testdb", "create table foo (a varchar(255) primary key, b varchar(255) as (concat(a, a)), c int)")
	c.Assert(err, IsNil)

	// Verify the table has 3 columns.
	ti, err := tracker.GetTable("testdb", "foo")
	c.Assert(err, IsNil)
	c.Assert(ti.Columns, HasLen, 3)
	c.Assert(ti.Columns[0].Name.L, Equals, "a")
	c.Assert(ti.Columns[0].IsGenerated(), IsFalse)
	c.Assert(ti.Columns[1].Name.L, Equals, "b")
	c.Assert(ti.Columns[1].IsGenerated(), IsTrue)
	c.Assert(ti.Columns[2].Name.L, Equals, "c")
	c.Assert(ti.Columns[2].IsGenerated(), IsFalse)

	// Drop one column from the table.
	err = tracker.Exec(ctx, "testdb", "alter table foo drop column b")
	c.Assert(err, IsNil)

	// Verify that 2 columns remain.
	ti2, err := tracker.GetTable("testdb", "foo")
	c.Assert(err, IsNil)
	c.Assert(ti, Not(Equals), ti2)
	c.Assert(ti2.Columns, HasLen, 2)
	c.Assert(ti2.Columns[0].Name.L, Equals, "a")
	c.Assert(ti2.Columns[0].IsGenerated(), IsFalse)
	c.Assert(ti2.Columns[1].Name.L, Equals, "c")
	c.Assert(ti2.Columns[1].IsGenerated(), IsFalse)
}

func (s *trackerSuite) TestCreateSchemaIfNotExists(c *C) {
	log.SetLevel(zapcore.ErrorLevel)

	tracker, err := schema.NewTracker()
	c.Assert(err, IsNil)

	// We cannot create a table without a database.
	ctx := context.Background()
	err = tracker.Exec(ctx, "testdb", "create table foo(a int)")
	c.Assert(err, ErrorMatches, `.*Unknown database 'testdb'`)

	// We can create the database directly.
	err = tracker.CreateSchemaIfNotExists("testdb")
	c.Assert(err, IsNil)

	// Creating the same database twice is no-op.
	err = tracker.CreateSchemaIfNotExists("testdb")
	c.Assert(err, IsNil)

	// Now creating a table should be successful
	err = tracker.Exec(ctx, "testdb", "create table foo(a int)")
	c.Assert(err, IsNil)

	ti, err := tracker.GetTable("testdb", "foo")
	c.Assert(err, IsNil)
	c.Assert(ti.Name.L, Equals, "foo")
}

// clearVolatileInfo removes generated information like TS and ID so DeepEquals
// of two compatible schemas can pass.
func clearVolatileInfo(ti *model.TableInfo) {
	ti.ID = 0
	ti.UpdateTS = 0
	if ti.Partition != nil {
		for i := range ti.Partition.Definitions {
			ti.Partition.Definitions[i].ID = 0
		}
	}
}

// asJSON is a convenient wrapper to print a TableInfo in its JSON representation.
type asJSON struct{ *model.TableInfo }

func (aj asJSON) String() string {
	b, _ := json.Marshal(aj.TableInfo)
	return string(b)
}

func (s *trackerSuite) TestCreateTableIfNotExists(c *C) {
	log.SetLevel(zapcore.ErrorLevel)

	tracker, err := schema.NewTracker()
	c.Assert(err, IsNil)

	// Create some sort of complicated table.
	err = tracker.CreateSchemaIfNotExists("testdb")
	c.Assert(err, IsNil)

	ctx := context.Background()
	err = tracker.Exec(ctx, "testdb", `
		create table foo(
			a int primary key auto_increment,
			b int as (c+1) not null,
			c int comment 'some cmt',
			d text,
			key dk(d(255))
		) comment 'more cmt' partition by range columns (a) (
			partition x41 values less than (41),
			partition x82 values less than (82),
			partition rest values less than maxvalue comment 'part cmt'
		);
	`)
	c.Assert(err, IsNil)

	// Save the table info
	ti1, err := tracker.GetTable("testdb", "foo")
	c.Assert(err, IsNil)
	c.Assert(ti1, NotNil)
	c.Assert(ti1.Name.O, Equals, "foo")
	ti1 = ti1.Clone()
	clearVolatileInfo(ti1)

	// Remove the table. Should not be found anymore.
	err = tracker.DropTable("testdb", "foo")
	c.Assert(err, IsNil)

	_, err = tracker.GetTable("testdb", "foo")
	c.Assert(err, ErrorMatches, `.*Table 'testdb\.foo' doesn't exist`)

	// Recover the table using the table info.
	err = tracker.CreateTableIfNotExists("testdb", "foo", ti1)
	c.Assert(err, IsNil)

	// The new table info should be equivalent to the old one except the TS and generated IDs.
	ti2, err := tracker.GetTable("testdb", "foo")
	c.Assert(err, IsNil)
	clearVolatileInfo(ti2)
	c.Assert(ti2, DeepEquals, ti1, Commentf("ti2 = %s\nti1 = %s", asJSON{ti2}, asJSON{ti1}))

	// Can use the table info to recover a table using a different name.
	err = tracker.CreateTableIfNotExists("testdb", "bar", ti1)
	c.Assert(err, IsNil)

	ti3, err := tracker.GetTable("testdb", "bar")
	c.Assert(err, IsNil)
	c.Assert(ti3.Name.O, Equals, "bar")
	clearVolatileInfo(ti3)
	ti3.Name = ti1.Name
	c.Assert(ti3, DeepEquals, ti1, Commentf("ti3 = %s\nti1 = %s", asJSON{ti3}, asJSON{ti1}))
}

func (s *trackerSuite) TestAllSchemas(c *C) {
	log.SetLevel(zapcore.ErrorLevel)
	ctx := context.Background()

	tracker, err := schema.NewTracker()
	c.Assert(err, IsNil)

	// nothing should exist...
	c.Assert(tracker.AllSchemas(), HasLen, 0)

	// Create several schemas and tables.
	err = tracker.CreateSchemaIfNotExists("testdb1")
	c.Assert(err, IsNil)
	err = tracker.CreateSchemaIfNotExists("testdb2")
	c.Assert(err, IsNil)
	err = tracker.CreateSchemaIfNotExists("testdb3")
	c.Assert(err, IsNil)
	err = tracker.Exec(ctx, "testdb2", "create table a(a int)")
	c.Assert(err, IsNil)
	err = tracker.Exec(ctx, "testdb1", "create table b(a int)")
	c.Assert(err, IsNil)
	err = tracker.Exec(ctx, "testdb1", "create table c(a int)")
	c.Assert(err, IsNil)

	// check that all schemas and tables are present.
	allSchemas := tracker.AllSchemas()
	c.Assert(allSchemas, HasLen, 3)
	existingNames := 0
	for _, schema := range allSchemas {
		switch schema.Name.O {
		case "testdb1":
			existingNames |= 1
			c.Assert(schema.Tables, HasLen, 2)
			for _, table := range schema.Tables {
				switch table.Name.O {
				case "b":
					existingNames |= 8
				case "c":
					existingNames |= 16
				default:
					c.Errorf("unexpected table testdb1.%s", table.Name)
				}
			}
		case "testdb2":
			existingNames |= 2
			c.Assert(schema.Tables, HasLen, 1)
			table := schema.Tables[0]
			c.Assert(table.Name.O, Equals, "a")
			c.Assert(table.Columns, HasLen, 1)
			// the table should be equivalent to the result of GetTable.
			table2, err2 := tracker.GetTable("testdb2", "a")
			c.Assert(err2, IsNil)
			c.Assert(table2, DeepEquals, table)
		case "testdb3":
			existingNames |= 4
		default:
			c.Errorf("unexpected schema %s", schema.Name)
		}
	}
	c.Assert(existingNames, Equals, 31)

	// reset the tracker. all schemas should be gone.
	err = tracker.Reset()
	c.Assert(err, IsNil)
	c.Assert(tracker.AllSchemas(), HasLen, 0)
	_, err = tracker.GetTable("testdb2", "a")
	c.Assert(err, ErrorMatches, `.*Table 'testdb2\.a' doesn't exist`)
}
