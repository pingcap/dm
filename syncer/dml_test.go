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

package syncer

import (
	"math"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/filter"
	tiddl "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/mock"
)

func (s *testSyncerSuite) TestCastUnsigned(c *C) {
	// ref: https://dev.mysql.com/doc/refman/5.7/en/integer-types.html
	cases := []struct {
		data     interface{}
		unsigned bool
		Type     byte
		expected interface{}
	}{
		{int8(-math.Exp2(7)), false, mysql.TypeTiny, int8(-math.Exp2(7))}, // TINYINT
		{int8(-math.Exp2(7)), true, mysql.TypeTiny, uint8(math.Exp2(7))},
		{int16(-math.Exp2(15)), false, mysql.TypeShort, int16(-math.Exp2(15))}, // SMALLINT
		{int16(-math.Exp2(15)), true, mysql.TypeShort, uint16(math.Exp2(15))},
		{int32(-math.Exp2(23)), false, mysql.TypeInt24, int32(-math.Exp2(23))}, // MEDIUMINT
		{int32(-math.Exp2(23)), true, mysql.TypeInt24, uint32(math.Exp2(23))},
		{int32(-math.Exp2(31)), false, mysql.TypeLong, int32(-math.Exp2(31))}, // INT
		{int32(-math.Exp2(31)), true, mysql.TypeLong, uint32(math.Exp2(31))},
		{int64(-math.Exp2(63)), false, mysql.TypeLonglong, int64(-math.Exp2(63))}, // BIGINT
		{int64(-math.Exp2(63)), true, mysql.TypeLonglong, uint64(math.Exp2(63))},
	}
	for _, cs := range cases {
		ft := types.NewFieldType(cs.Type)
		if cs.unsigned {
			ft.Flag |= mysql.UnsignedFlag
		}
		obtained := castUnsigned(cs.data, ft)
		c.Assert(obtained, Equals, cs.expected)
	}
}

func createTableInfo(p *parser.Parser, se sessionctx.Context, tableID int64, sql string) (*model.TableInfo, error) {
	node, err := p.ParseOneStmt(sql, "utf8mb4", "utf8mb4_bin")
	if err != nil {
		return nil, err
	}
	return tiddl.MockTableInfo(se, node.(*ast.CreateTableStmt), tableID)
}

func (s *testSyncerSuite) TestFindFitIndex(c *C) {
	p := parser.New()
	se := mock.NewContext()

	ti, err := createTableInfo(p, se, 1, `
		create table t1(
			a int,
			b int,
			c int,
			d int not null,
			primary key(a, b),
			unique key(c),
			unique key(d)
		);
	`)
	c.Assert(err, IsNil)

	columns := findFitIndex(ti)
	c.Assert(columns, NotNil)
	c.Assert(columns.Columns, HasLen, 2)
	c.Assert(columns.Columns[0].Name.L, Equals, "a")
	c.Assert(columns.Columns[1].Name.L, Equals, "b")

	ti, err = createTableInfo(p, se, 2, `create table t2(c int unique);`)
	c.Assert(err, IsNil)
	columns = findFitIndex(ti)
	c.Assert(columns, IsNil)

	ti, err = createTableInfo(p, se, 3, `create table t3(d int not null unique);`)
	c.Assert(err, IsNil)
	columns = findFitIndex(ti)
	c.Assert(columns, NotNil)
	c.Assert(columns.Columns, HasLen, 1)
	c.Assert(columns.Columns[0].Name.L, Equals, "d")

	ti, err = createTableInfo(p, se, 4, `create table t4(e int not null, key(e));`)
	c.Assert(err, IsNil)
	columns = findFitIndex(ti)
	c.Assert(columns, IsNil)

	ti, err = createTableInfo(p, se, 5, `create table t5(f datetime primary key);`)
	c.Assert(err, IsNil)
	columns = findFitIndex(ti)
	c.Assert(columns, NotNil)
	c.Assert(columns.Columns, HasLen, 1)
	c.Assert(columns.Columns[0].Name.L, Equals, "f")

	ti, err = createTableInfo(p, se, 6, `create table t6(g int primary key);`)
	c.Assert(err, IsNil)
	columns = findFitIndex(ti)
	c.Assert(columns, NotNil)
	c.Assert(columns.Columns, HasLen, 1)
	c.Assert(columns.Columns[0].Name.L, Equals, "g")
}

func (s *testSyncerSuite) TestGenMultipleKeys(c *C) {
	p := parser.New()
	se := mock.NewContext()

	testCases := []struct {
		schema string
		values []interface{}
		keys   []string
	}{
		{
			// test no keys
			schema: `create table t1(a int)`,
			values: []interface{}{10},
			keys:   []string{"table"},
		},
		{
			// one primary key
			schema: `create table t2(a int primary key, b double)`,
			values: []interface{}{60, 70.5},
			keys:   []string{"60.a.table"},
		},
		{
			// one unique key
			schema: `create table t3(a int unique, b double)`,
			values: []interface{}{60, 70.5},
			keys:   []string{"60.a.table"},
		},
		{
			// one ordinary key
			schema: `create table t4(a int, b double, key(b))`,
			values: []interface{}{60, 70.5},
			keys:   []string{"table"},
		},
		{
			// multiple keys
			schema: `create table t5(a int, b text, c int, key(a), key(b(3)))`,
			values: []interface{}{13, "abcdef", 15},
			keys:   []string{"table"},
		},
		{
			// multiple keys with primary key
			schema: `create table t6(a int primary key, b varchar(16) unique)`,
			values: []interface{}{16, "xyz"},
			keys:   []string{"16.a.table", "xyz.b.table"},
		},
		{
			// non-integer primary key
			schema: `create table t65(a int unique, b varchar(16) primary key)`,
			values: []interface{}{16, "xyz"},
			keys:   []string{"16.a.table", "xyz.b.table"},
		},
		{
			// primary key of multiple columns
			schema: `create table t7(a int, b int, primary key(a, b))`,
			values: []interface{}{59, 69},
			keys:   []string{"59.a.69.b.table"},
		},
		{
			// ordinary key of multiple columns
			schema: `create table t75(a int, b int, c int, key(a, b), key(c, b))`,
			values: []interface{}{48, 58, 68},
			keys:   []string{"table"},
		},
		{
			// so many keys
			schema: `
				create table t8(
					a int, b int, c int,
					primary key(a, b),
					unique key(b, c),
					key(a, b, c),
					unique key(c, a)
				)
			`,
			values: []interface{}{27, 37, 47},
			keys:   []string{"27.a.37.b.table", "37.b.47.c.table", "47.c.27.a.table"},
		},
		{
			// `null` for unique key
			schema: `
				create table t8(
					a int, b int default null,
					primary key(a),
					unique key(b)
				)
			`,
			values: []interface{}{17, nil},
			keys:   []string{"17.a.table"},
		},
	}

	for i, tc := range testCases {
		schema := tc.schema
		assert := func(obtained interface{}, checker Checker, args ...interface{}) {
			c.Assert(obtained, checker, append(args, Commentf("test case schema: %s", schema))...)
		}

		ti, err := createTableInfo(p, se, int64(i+1), tc.schema)
		assert(err, IsNil)
		keys := genMultipleKeys(ti, tc.values, "table")
		assert(keys, DeepEquals, tc.keys)
	}
}

func (s *testSyncerSuite) TestGenWhere(c *C) {
	p := parser.New()
	se := mock.NewContext()
	schema1 := "create table test.tb(id int primary key, col1 int unique not null, col2 int unique, name varchar(24))"
	ti1, err := createTableInfo(p, se, 0, schema1)
	c.Assert(err, IsNil)
	schema2 := "create table test.tb(id int, col1 int, col2 int, name varchar(24))"
	ti2, err := createTableInfo(p, se, 0, schema2)
	c.Assert(err, IsNil)

	testCases := []struct {
		dml    *DML
		sql    string
		values []interface{}
	}{
		{
			newDML(del, false, "", &filter.Table{}, nil, []interface{}{1, 2, 3, "haha"}, nil, []interface{}{1, 2, 3, "haha"}, ti1.Columns, ti1),
			"`id` = ?",
			[]interface{}{1},
		},
		{
			newDML(update, false, "", &filter.Table{}, []interface{}{1, 2, 3, "haha"}, []interface{}{4, 5, 6, "hihi"}, []interface{}{1, 2, 3, "haha"}, []interface{}{4, 5, 6, "hihi"}, ti1.Columns, ti1),
			"`id` = ?",
			[]interface{}{1},
		},
		{
			newDML(del, false, "", &filter.Table{}, nil, []interface{}{1, 2, 3, "haha"}, nil, []interface{}{1, 2, 3, "haha"}, ti2.Columns, ti2),
			"`id` = ? AND `col1` = ? AND `col2` = ? AND `name` = ?",
			[]interface{}{1, 2, 3, "haha"},
		},
		{
			newDML(update, false, "", &filter.Table{}, []interface{}{1, 2, 3, "haha"}, []interface{}{4, 5, 6, "hihi"}, []interface{}{1, 2, 3, "haha"}, []interface{}{4, 5, 6, "hihi"}, ti2.Columns, ti2),
			"`id` = ? AND `col1` = ? AND `col2` = ? AND `name` = ?",
			[]interface{}{1, 2, 3, "haha"},
		},
	}
	for _, tc := range testCases {
		var buf strings.Builder
		whereValues := tc.dml.genWhere(&buf)
		c.Assert(buf.String(), Equals, tc.sql)
		c.Assert(whereValues, DeepEquals, tc.values)
	}
}

func (s *testSyncerSuite) TestGenSQL(c *C) {
	p := parser.New()
	se := mock.NewContext()
	schema := "create table test.tb(id int primary key, col1 int unique not null, col2 int unique, name varchar(24))"
	ti, err := createTableInfo(p, se, 0, schema)
	c.Assert(err, IsNil)

	testCases := []struct {
		dml     *DML
		queries []string
		args    [][]interface{}
	}{
		{
			newDML(insert, false, "`targetSchema`.`targetTable`", &filter.Table{}, nil, []interface{}{1, 2, 3, "haha"}, nil, []interface{}{1, 2, 3, "haha"}, ti.Columns, ti),
			[]string{"INSERT INTO `targetSchema`.`targetTable` (`id`,`col1`,`col2`,`name`) VALUES (?,?,?,?)"},
			[][]interface{}{{1, 2, 3, "haha"}},
		},
		{
			newDML(insert, true, "`targetSchema`.`targetTable`", &filter.Table{}, nil, []interface{}{1, 2, 3, "haha"}, nil, []interface{}{1, 2, 3, "haha"}, ti.Columns, ti),
			[]string{"INSERT INTO `targetSchema`.`targetTable` (`id`,`col1`,`col2`,`name`) VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE `id`=VALUES(`id`),`col1`=VALUES(`col1`),`col2`=VALUES(`col2`),`name`=VALUES(`name`)"},
			[][]interface{}{{1, 2, 3, "haha"}},
		},
		{
			newDML(del, false, "`targetSchema`.`targetTable`", &filter.Table{}, nil, []interface{}{1, 2, 3, "haha"}, nil, []interface{}{1, 2, 3, "haha"}, ti.Columns, ti),
			[]string{"DELETE FROM `targetSchema`.`targetTable` WHERE `id` = ? LIMIT 1"},
			[][]interface{}{{1}},
		},
		{
			newDML(update, false, "`targetSchema`.`targetTable`", &filter.Table{}, []interface{}{1, 2, 3, "haha"}, []interface{}{4, 5, 6, "hihi"}, []interface{}{1, 2, 3, "haha"}, []interface{}{1, 2, 3, "haha"}, ti.Columns, ti),
			[]string{"UPDATE `targetSchema`.`targetTable` SET `id` = ?, `col1` = ?, `col2` = ?, `name` = ? WHERE `id` = ? LIMIT 1"},
			[][]interface{}{{4, 5, 6, "hihi", 1}},
		},
		{
			newDML(update, true, "`targetSchema`.`targetTable`", &filter.Table{}, []interface{}{1, 2, 3, "haha"}, []interface{}{4, 5, 6, "hihi"}, []interface{}{1, 2, 3, "haha"}, []interface{}{1, 2, 3, "haha"}, ti.Columns, ti),
			[]string{"DELETE FROM `targetSchema`.`targetTable` WHERE `id` = ? LIMIT 1", "INSERT INTO `targetSchema`.`targetTable` (`id`,`col1`,`col2`,`name`) VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE `id`=VALUES(`id`),`col1`=VALUES(`col1`),`col2`=VALUES(`col2`),`name`=VALUES(`name`)"},
			[][]interface{}{{1}, {4, 5, 6, "hihi"}},
		},
	}
	for _, tc := range testCases {
		queries, args := tc.dml.genSQL()
		c.Assert(queries, DeepEquals, tc.queries)
		c.Assert(args, DeepEquals, tc.args)
	}
}

func (s *testSyncerSuite) TestValueHolder(c *C) {
	holder := valuesHolder(0)
	c.Assert(holder, Equals, "()")
	holder = valuesHolder(10)
	c.Assert(holder, Equals, "(?,?,?,?,?,?,?,?,?,?)")
}

func (s *testSyncerSuite) TestGenDMLWithSameOp(c *C) {
	targetTableID1 := "`db1`.`tb1`"
	targetTableID2 := "`db2`.`tb2`"
	sourceTable11 := &filter.Table{Schema: "dba", Name: "tba"}
	sourceTable12 := &filter.Table{Schema: "dba", Name: "tbb"}
	sourceTable21 := &filter.Table{Schema: "dbb", Name: "tba"}
	sourceTable22 := &filter.Table{Schema: "dbb", Name: "tbb"}

	p := parser.New()
	se := mock.NewContext()
	schema11 := "create table dba.tba(id int primary key, col1 int unique not null, name varchar(24))"
	schema12 := schema11
	schema21 := "create table dba.tba(id int primary key, col2 int unique not null, name varchar(24))"
	schema22 := "create table dba.tbb(id int primary key, col3 int unique not null, name varchar(24))"
	ti11, err := createTableInfo(p, se, 0, schema11)
	c.Assert(err, IsNil)
	ti12, err := createTableInfo(p, se, 0, schema12)
	c.Assert(err, IsNil)
	ti21, err := createTableInfo(p, se, 0, schema21)
	c.Assert(err, IsNil)
	ti22, err := createTableInfo(p, se, 0, schema22)
	c.Assert(err, IsNil)

	dmls := []*DML{
		// insert
		newDML(insert, true, targetTableID1, sourceTable11, nil, []interface{}{1, 1, "a"}, nil, []interface{}{1, 1, "a"}, ti11.Columns, ti11),
		newDML(insert, true, targetTableID1, sourceTable11, nil, []interface{}{2, 2, "b"}, nil, []interface{}{2, 2, "b"}, ti11.Columns, ti11),
		newDML(insert, true, targetTableID1, sourceTable12, nil, []interface{}{3, 3, "c"}, nil, []interface{}{3, 3, "c"}, ti12.Columns, ti12),
		// update no index
		newDML(update, true, targetTableID1, sourceTable11, []interface{}{1, 1, "a"}, []interface{}{1, 1, "aa"}, []interface{}{1, 1, "a"}, []interface{}{1, 1, "aa"}, ti11.Columns, ti11),
		newDML(update, true, targetTableID1, sourceTable11, []interface{}{2, 2, "b"}, []interface{}{2, 2, "bb"}, []interface{}{2, 2, "b"}, []interface{}{2, 2, "bb"}, ti11.Columns, ti11),
		newDML(update, true, targetTableID1, sourceTable12, []interface{}{3, 3, "c"}, []interface{}{3, 3, "cc"}, []interface{}{3, 3, "c"}, []interface{}{3, 3, "cc"}, ti12.Columns, ti12),
		// update uk
		newDML(update, true, targetTableID1, sourceTable11, []interface{}{1, 1, "aa"}, []interface{}{1, 4, "aa"}, []interface{}{1, 1, "aa"}, []interface{}{1, 4, "aa"}, ti11.Columns, ti11),
		newDML(update, true, targetTableID1, sourceTable11, []interface{}{2, 2, "bb"}, []interface{}{2, 5, "bb"}, []interface{}{2, 2, "bb"}, []interface{}{2, 5, "bb"}, ti11.Columns, ti11),
		newDML(update, true, targetTableID1, sourceTable12, []interface{}{3, 3, "cc"}, []interface{}{3, 6, "cc"}, []interface{}{3, 3, "cc"}, []interface{}{3, 6, "cc"}, ti12.Columns, ti12),
		// update pk
		newDML(update, true, targetTableID1, sourceTable11, []interface{}{1, 4, "aa"}, []interface{}{4, 4, "aa"}, []interface{}{1, 1, "aa"}, []interface{}{4, 4, "aa"}, ti11.Columns, ti11),
		newDML(update, true, targetTableID1, sourceTable11, []interface{}{2, 5, "bb"}, []interface{}{5, 5, "bb"}, []interface{}{2, 2, "bb"}, []interface{}{5, 5, "bb"}, ti11.Columns, ti11),
		newDML(update, true, targetTableID1, sourceTable12, []interface{}{3, 6, "cc"}, []interface{}{6, 6, "cc"}, []interface{}{3, 3, "cc"}, []interface{}{6, 6, "cc"}, ti12.Columns, ti12),
		// delete
		newDML(del, true, targetTableID1, sourceTable11, nil, []interface{}{4, 4, "aa"}, nil, []interface{}{4, 4, "aa"}, ti11.Columns, ti11),
		newDML(del, true, targetTableID1, sourceTable11, nil, []interface{}{5, 5, "bb"}, nil, []interface{}{5, 5, "bb"}, ti11.Columns, ti11),
		newDML(del, true, targetTableID1, sourceTable12, nil, []interface{}{6, 6, "cc"}, nil, []interface{}{6, 6, "cc"}, ti12.Columns, ti12),

		// target table 2
		// insert
		newDML(insert, true, targetTableID2, sourceTable21, nil, []interface{}{1, 1, "a"}, nil, []interface{}{1, 1, "a"}, ti21.Columns, ti21),
		newDML(insert, false, targetTableID2, sourceTable21, nil, []interface{}{2, 2, "b"}, nil, []interface{}{2, 2, "b"}, ti21.Columns, ti21),
		newDML(insert, false, targetTableID2, sourceTable22, nil, []interface{}{3, 3, "c"}, nil, []interface{}{3, 3, "c"}, ti22.Columns, ti22),
		// update no index
		newDML(update, false, targetTableID2, sourceTable21, []interface{}{1, 1, "a"}, []interface{}{1, 1, "aa"}, []interface{}{1, 1, "a"}, []interface{}{1, 1, "aa"}, ti21.Columns, ti21),
		newDML(update, false, targetTableID2, sourceTable21, []interface{}{2, 2, "b"}, []interface{}{2, 2, "bb"}, []interface{}{2, 2, "b"}, []interface{}{2, 2, "bb"}, ti21.Columns, ti21),
		newDML(update, false, targetTableID2, sourceTable22, []interface{}{3, 3, "c"}, []interface{}{3, 3, "cc"}, []interface{}{3, 3, "c"}, []interface{}{3, 3, "cc"}, ti22.Columns, ti22),
		// update uk
		newDML(update, false, targetTableID2, sourceTable21, []interface{}{1, 1, "aa"}, []interface{}{1, 4, "aa"}, []interface{}{1, 1, "aa"}, []interface{}{1, 4, "aa"}, ti21.Columns, ti21),
		newDML(update, false, targetTableID2, sourceTable21, []interface{}{2, 2, "bb"}, []interface{}{2, 5, "bb"}, []interface{}{2, 2, "bb"}, []interface{}{2, 5, "bb"}, ti21.Columns, ti21),
		newDML(update, false, targetTableID2, sourceTable22, []interface{}{3, 3, "cc"}, []interface{}{3, 6, "cc"}, []interface{}{3, 3, "cc"}, []interface{}{3, 6, "cc"}, ti22.Columns, ti22),
		// update pk
		newDML(update, false, targetTableID2, sourceTable21, []interface{}{1, 4, "aa"}, []interface{}{4, 4, "aa"}, []interface{}{1, 1, "aa"}, []interface{}{4, 4, "aa"}, ti21.Columns, ti21),
		newDML(update, false, targetTableID2, sourceTable21, []interface{}{2, 5, "bb"}, []interface{}{5, 5, "bb"}, []interface{}{2, 2, "bb"}, []interface{}{5, 5, "bb"}, ti21.Columns, ti21),
		newDML(update, false, targetTableID2, sourceTable22, []interface{}{3, 6, "cc"}, []interface{}{6, 6, "cc"}, []interface{}{3, 3, "cc"}, []interface{}{6, 6, "cc"}, ti22.Columns, ti22),
		// delete
		newDML(del, false, targetTableID2, sourceTable21, nil, []interface{}{4, 4, "aa"}, nil, []interface{}{4, 4, "aa"}, ti21.Columns, ti21),
		newDML(del, false, targetTableID2, sourceTable21, nil, []interface{}{5, 5, "bb"}, nil, []interface{}{5, 5, "bb"}, ti21.Columns, ti21),
		newDML(del, false, targetTableID2, sourceTable22, nil, []interface{}{6, 6, "cc"}, nil, []interface{}{6, 6, "cc"}, ti22.Columns, ti22),

		// table1
		// detele
		newDML(del, false, targetTableID1, sourceTable11, nil, []interface{}{44, 44, "aaa"}, nil, []interface{}{44, 44, "aaa"}, ti11.Columns, ti11),
		newDML(del, false, targetTableID1, sourceTable11, nil, []interface{}{55, 55, "bbb"}, nil, []interface{}{55, 55, "bbb"}, ti11.Columns, ti11),
		newDML(del, false, targetTableID1, sourceTable12, nil, []interface{}{66, 66, "ccc"}, nil, []interface{}{66, 66, "ccc"}, ti12.Columns, ti12),
	}

	expectQueries := []string{
		// table1
		"INSERT INTO `db1`.`tb1` (`id`,`col1`,`name`) VALUES (?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?),(?,?,?) ON DUPLICATE KEY UPDATE `id`=VALUES(`id`),`col1`=VALUES(`col1`),`name`=VALUES(`name`)",
		"DELETE FROM `db1`.`tb1` WHERE `id` = ? LIMIT 1",
		"INSERT INTO `db1`.`tb1` (`id`,`col1`,`name`) VALUES (?,?,?) ON DUPLICATE KEY UPDATE `id`=VALUES(`id`),`col1`=VALUES(`col1`),`name`=VALUES(`name`)",
		"DELETE FROM `db1`.`tb1` WHERE `id` = ? LIMIT 1",
		"INSERT INTO `db1`.`tb1` (`id`,`col1`,`name`) VALUES (?,?,?) ON DUPLICATE KEY UPDATE `id`=VALUES(`id`),`col1`=VALUES(`col1`),`name`=VALUES(`name`)",
		"DELETE FROM `db1`.`tb1` WHERE `id` = ? LIMIT 1",
		"INSERT INTO `db1`.`tb1` (`id`,`col1`,`name`) VALUES (?,?,?) ON DUPLICATE KEY UPDATE `id`=VALUES(`id`),`col1`=VALUES(`col1`),`name`=VALUES(`name`)",
		"DELETE FROM `db1`.`tb1` WHERE (`id`) IN ((?),(?),(?))",

		// table2
		"INSERT INTO `db2`.`tb2` (`id`,`col2`,`name`) VALUES (?,?,?) ON DUPLICATE KEY UPDATE `id`=VALUES(`id`),`col2`=VALUES(`col2`),`name`=VALUES(`name`)",
		"INSERT INTO `db2`.`tb2` (`id`,`col2`,`name`) VALUES (?,?,?)",
		"INSERT INTO `db2`.`tb2` (`id`,`col3`,`name`) VALUES (?,?,?)",
		"INSERT INTO `db2`.`tb2` (`id`,`col2`,`name`) VALUES (?,?,?),(?,?,?) ON DUPLICATE KEY UPDATE `id`=VALUES(`id`),`col2`=VALUES(`col2`),`name`=VALUES(`name`)",
		"INSERT INTO `db2`.`tb2` (`id`,`col3`,`name`) VALUES (?,?,?) ON DUPLICATE KEY UPDATE `id`=VALUES(`id`),`col3`=VALUES(`col3`),`name`=VALUES(`name`)",
		"INSERT INTO `db2`.`tb2` (`id`,`col2`,`name`) VALUES (?,?,?),(?,?,?) ON DUPLICATE KEY UPDATE `id`=VALUES(`id`),`col2`=VALUES(`col2`),`name`=VALUES(`name`)",
		"INSERT INTO `db2`.`tb2` (`id`,`col3`,`name`) VALUES (?,?,?) ON DUPLICATE KEY UPDATE `id`=VALUES(`id`),`col3`=VALUES(`col3`),`name`=VALUES(`name`)",
		"UPDATE `db2`.`tb2` SET `id` = ?, `col2` = ?, `name` = ? WHERE `id` = ? LIMIT 1",
		"UPDATE `db2`.`tb2` SET `id` = ?, `col2` = ?, `name` = ? WHERE `id` = ? LIMIT 1",
		"UPDATE `db2`.`tb2` SET `id` = ?, `col3` = ?, `name` = ? WHERE `id` = ? LIMIT 1",
		"DELETE FROM `db2`.`tb2` WHERE (`id`) IN ((?),(?),(?))",

		// table1
		"DELETE FROM `db1`.`tb1` WHERE (`id`) IN ((?),(?),(?))",
	}

	expectArgs := [][]interface{}{
		// table1
		{1, 1, "a", 2, 2, "b", 3, 3, "c", 1, 1, "aa", 2, 2, "bb", 3, 3, "cc", 1, 4, "aa", 2, 5, "bb", 3, 6, "cc"},
		{1},
		{4, 4, "aa"},
		{2},
		{5, 5, "bb"},
		{3},
		{6, 6, "cc"},
		{4, 5, 6},

		// table2
		{1, 1, "a"},
		{2, 2, "b"},
		{3, 3, "c"},
		{1, 1, "aa", 2, 2, "bb"},
		{3, 3, "cc"},
		{1, 4, "aa", 2, 5, "bb"},
		{3, 6, "cc"},
		{4, 4, "aa", 1},
		{5, 5, "bb", 2},
		{6, 6, "cc", 3},
		{4, 5, 6},

		// table1
		{44, 55, 66},
	}

	queries, args := genDMLsWithSameOp(dmls)
	c.Assert(queries, DeepEquals, expectQueries)
	c.Assert(args, DeepEquals, expectArgs)
}
