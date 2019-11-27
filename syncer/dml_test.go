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
	"strconv"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/types"
	tiddl "github.com/pingcap/tidb/ddl"
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
		{int16(-math.Exp2(15)), false, mysql.TypeShort, int16(-math.Exp2(15))}, //SMALLINT
		{int16(-math.Exp2(15)), true, mysql.TypeShort, uint16(math.Exp2(15))},
		{int32(-math.Exp2(23)), false, mysql.TypeInt24, int32(-math.Exp2(23))}, //MEDIUMINT
		{int32(-math.Exp2(23)), true, mysql.TypeInt24, uint32(math.Exp2(23))},
		{int32(-math.Exp2(31)), false, mysql.TypeLong, int32(-math.Exp2(31))}, // INT
		{int32(-math.Exp2(31)), true, mysql.TypeLong, uint32(math.Exp2(31))},
		{int64(-math.Exp2(63)), false, mysql.TypeLonglong, int64(-math.Exp2(63))},                        // BIGINT
		{int64(-math.Exp2(63)), true, mysql.TypeLonglong, strconv.FormatUint(uint64(math.Exp2(63)), 10)}, // special case use string to represent uint64
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

func (s *testSyncerSuite) TestGenColumnPlaceholders(c *C) {
	placeholderStr := genColumnPlaceholders(1)
	c.Assert(placeholderStr, Equals, "?")

	placeholderStr = genColumnPlaceholders(3)
	c.Assert(placeholderStr, Equals, "?,?,?")
}

func createTableInfo(p *parser.Parser, se sessionctx.Context, tableID int64, sql string) (*model.TableInfo, error) {
	node, err := p.ParseOneStmt(sql, "utf8mb4", "utf8mb4_bin")
	if err != nil {
		return nil, err
	}
	return tiddl.MockTableInfo(se, node.(*ast.CreateTableStmt), tableID)
}

func (s *testSyncerSuite) TestGenColumnList(c *C) {
	columns := []*model.ColumnInfo{
		{
			Name: model.NewCIStr("a"),
		}, {
			Name: model.NewCIStr("b"),
		}, {
			Name: model.NewCIStr("c`d"),
		},
	}

	columnList := genColumnList(columns[:1])
	c.Assert(columnList, Equals, "`a`")

	columnList = genColumnList(columns)
	c.Assert(columnList, Equals, "`a`,`b`,`c``d`")
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
