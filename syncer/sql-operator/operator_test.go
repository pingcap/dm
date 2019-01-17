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

package operator

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/siddontang/go-mysql/mysql"

	"github.com/pingcap/dm/dm/pb"
)

var _ = Suite(&testOperatorSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testOperatorSuite struct {
}

func (o *testOperatorSuite) TestOperatorSet(c *C) {
	reqPos := &pb.HandleSubTaskSQLsRequest{
		Op:        pb.SQLOp_SKIP,
		BinlogPos: "mysql-bin.000001:234",
	}
	reqPattern := &pb.HandleSubTaskSQLsRequest{
		Op:         pb.SQLOp_SKIP,
		SqlPattern: "~(?i)ALTER\\s+TABLE\\s+`db1`.`tbl1`\\s+ADD\\s+COLUMN\\s+col1\\s+INT",
	}
	sql := "ALTER TABLE `db1`.`tbl1` ADD COLUMN col1 INT"

	h := NewHolder()

	// nil request
	err := h.Set(nil)
	c.Assert(err, NotNil)

	// not supported op
	err = h.Set(&pb.HandleSubTaskSQLsRequest{Op: pb.SQLOp_INJECT})
	c.Assert(err, NotNil)

	// none of binlog-pos, sql-pattern set
	err = h.Set(&pb.HandleSubTaskSQLsRequest{Op: pb.SQLOp_SKIP})
	c.Assert(err, NotNil)

	// both binlog-pos, sql-pattern set
	err = h.Set(&pb.HandleSubTaskSQLsRequest{
		Op:         pb.SQLOp_SKIP,
		BinlogPos:  reqPos.BinlogPos,
		SqlPattern: reqPattern.SqlPattern,
	})
	c.Assert(err, NotNil)

	// no operator set, apply got nothing
	applied, args, err := h.Apply(mysql.Position{}, []string{sql})
	c.Assert(err, IsNil)
	c.Assert(applied, IsFalse)
	c.Assert(args, IsNil)
}

func (o *testOperatorSuite) TestBinlogPos(c *C) {
	cases := []struct {
		req *pb.HandleSubTaskSQLsRequest
		pos mysql.Position
	}{
		{
			req: &pb.HandleSubTaskSQLsRequest{
				Op:        pb.SQLOp_SKIP,
				BinlogPos: "mysql-bin.000001:234",
				Args:      []string{"CREATE DATABASE shard_db_1;", "CREATE TABLE shard_db_1.shard_table_1 (c1 int PRIMARY KEY, c2 int, INDEX idx_c2 (c2))"},
			},
			pos: mysql.Position{Name: "mysql-bin.000001", Pos: 234},
		},
		{
			req: &pb.HandleSubTaskSQLsRequest{
				Op:        pb.SQLOp_SKIP,
				BinlogPos: "mysql-bin.000005:678",
				Args:      []string{"CREATE DATABASE shard_db_2;", "CREATE TABLE shard_db_2.shard_table_1 (c1 int PRIMARY KEY, c2 int, INDEX idx_c2 (c2))"},
			},
			pos: mysql.Position{Name: "mysql-bin.000005", Pos: 678},
		},
	}

	h := NewHolder()

	// invalid binlog-pos
	err := h.Set(&pb.HandleSubTaskSQLsRequest{Op: pb.SQLOp_SKIP, BinlogPos: "invalid-binlog-pos.123"})
	c.Assert(err, NotNil)

	// no operator set, mismatch
	applied, args, err := h.Apply(cases[0].pos, nil)
	c.Assert(err, IsNil)
	c.Assert(applied, IsFalse)
	c.Assert(args, IsNil) // mismatch, no args used

	// set skip operator
	err = h.Set(cases[0].req)
	c.Assert(err, IsNil)

	// binlog-pos mismatch
	applied, args, err = h.Apply(mysql.Position{}, nil)
	c.Assert(err, IsNil)
	c.Assert(applied, IsFalse)
	c.Assert(args, IsNil)

	// matched
	applied, args, err = h.Apply(cases[0].pos, nil)
	c.Assert(err, IsNil)
	c.Assert(applied, IsTrue)
	c.Assert(args, IsNil) // for skip, no args used

	// op replace, multi operators
	for _, cs := range cases {
		cs.req.Op = pb.SQLOp_REPLACE
		err = h.Set(cs.req)
		c.Assert(err, IsNil)
	}
	for _, cs := range cases {
		applied, args, err = h.Apply(cs.pos, nil)
		c.Assert(err, IsNil)
		c.Assert(applied, IsTrue)
		c.Assert(args, DeepEquals, cs.req.Args)
	}

	// all operators applied, match nothing
	applied, args, err = h.Apply(cases[0].pos, nil)
	c.Assert(err, IsNil)
	c.Assert(applied, IsFalse)
	c.Assert(args, IsNil)
}

func (o *testOperatorSuite) TestSQLPattern(c *C) {
	cases := []struct {
		req  *pb.HandleSubTaskSQLsRequest
		sqls []string
	}{
		{
			req: &pb.HandleSubTaskSQLsRequest{
				Op:         pb.SQLOp_SKIP,
				SqlPattern: "~(?i)ALTER\\s+TABLE\\s+`db1`.`tbl1`\\s+ADD\\s+COLUMN\\s+col1\\s+INT",
				Args:       []string{"CREATE DATABASE shard_db_1;", "CREATE TABLE shard_db_1.shard_table_1 (c1 int PRIMARY KEY, c2 int, INDEX idx_c2 (c2))"},
			},
			sqls: []string{"ALTER TABLE `db1`.`tbl1` ADD COLUMN col1 INT"},
		},
		{
			req: &pb.HandleSubTaskSQLsRequest{
				Op:         pb.SQLOp_SKIP,
				SqlPattern: "~(?i)DROP\\s+TABLE",
				Args:       []string{"CREATE DATABASE shard_db_2;", "DROP TABLE shard_db_2.shard_table_1"},
			},
			sqls: []string{"INSERT INTO `db1`.`tbl` VALUES (1, 2)", "DROP TABLE `db1`.`tbl1`", "INSERT INTO `db1`.`tbl` VALUES (3, 3)"},
		},
	}
	emptyPos := mysql.Position{}

	h := NewHolder()

	// invalid sql-pattern
	err := h.Set(&pb.HandleSubTaskSQLsRequest{Op: pb.SQLOp_SKIP, SqlPattern: "~(invalid-regexp"})
	c.Assert(err, NotNil)

	// no operator set, mismatch
	applied, args, err := h.Apply(emptyPos, cases[0].sqls)
	c.Assert(err, IsNil)
	c.Assert(applied, IsFalse)
	c.Assert(args, IsNil) // mismatch, no args used

	// set skip operator
	err = h.Set(cases[0].req)
	c.Assert(err, IsNil)

	// sql-pattern mismatch
	applied, args, err = h.Apply(emptyPos, []string{"INSERT INTO `db1`.`tbl1` VALUES (1, 2)"})
	c.Assert(err, IsNil)
	c.Assert(applied, IsFalse)
	c.Assert(args, IsNil)

	// matched
	applied, args, err = h.Apply(emptyPos, cases[0].sqls)
	c.Assert(err, IsNil)
	c.Assert(applied, IsTrue)
	c.Assert(args, IsNil) // for skip, no args used

	// op replace, multi operators
	for _, cs := range cases {
		cs.req.Op = pb.SQLOp_REPLACE
		err = h.Set(cs.req)
		c.Assert(err, IsNil)
	}
	for _, cs := range cases {
		applied, args, err = h.Apply(emptyPos, cs.sqls)
		c.Assert(err, IsNil)
		c.Assert(applied, IsTrue)
		c.Assert(args, DeepEquals, cs.req.Args)
	}

	// all operators applied, match nothing
	applied, args, err = h.Apply(emptyPos, cases[0].sqls)
	c.Assert(err, IsNil)
	c.Assert(applied, IsFalse)
	c.Assert(args, IsNil)
}
