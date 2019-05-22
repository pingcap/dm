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

package binlog

import (
	"testing"

	. "github.com/pingcap/check"
	gmysql "github.com/siddontang/go-mysql/mysql"
)

var _ = Suite(&testPositionSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testPositionSuite struct {
}

func (t *testPositionSuite) TestPositionFromStr(c *C) {
	emptyPos := gmysql.Position{}
	cases := []struct {
		str      string
		pos      gmysql.Position
		hasError bool
	}{
		{
			str:      "mysql-bin.000001",
			pos:      emptyPos,
			hasError: true,
		},
		{
			str:      "234",
			pos:      emptyPos,
			hasError: true,
		},
		{
			str:      "mysql-bin.000001:abc",
			pos:      emptyPos,
			hasError: true,
		},
		{
			str:      "mysql-bin.000001:234:567",
			pos:      emptyPos,
			hasError: true,
		},
		{
			str:      "mysql-bin.000001:234",
			pos:      gmysql.Position{Name: "mysql-bin.000001", Pos: 234},
			hasError: false,
		},
	}

	for _, cs := range cases {
		pos, err := PositionFromStr(cs.str)
		if cs.hasError {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
		}
		c.Assert(pos, DeepEquals, cs.pos)
	}
}

func (t *testPositionSuite) TestRealMySQLPos(c *C) {
	cases := []struct {
		pos       gmysql.Position
		expect    gmysql.Position
		errMsgReg string
	}{
		{
			pos:    gmysql.Position{Name: "mysql-bin.000001", Pos: 154},
			expect: gmysql.Position{Name: "mysql-bin.000001", Pos: 154},
		},
		{
			pos:    gmysql.Position{Name: "mysql-bin|000002.000003", Pos: 154},
			expect: gmysql.Position{Name: "mysql-bin.000003", Pos: 154},
		},
		{
			pos:       gmysql.Position{Name: "", Pos: 154},
			expect:    gmysql.Position{Name: "", Pos: 154},
			errMsgReg: ".*invalid binlog filename.*",
		},
	}

	for _, cs := range cases {
		pos, err := RealMySQLPos(cs.pos)
		if len(cs.errMsgReg) > 0 {
			c.Assert(err, ErrorMatches, cs.errMsgReg)
		} else {
			c.Assert(err, IsNil)
		}
		c.Assert(pos, DeepEquals, cs.expect)
	}
}

func (t *testFilenameSuite) TestExtractPos(c *C) {
	cases := []struct {
		pos            gmysql.Position
		uuids          []string
		uuidWithSuffix string
		uuidSuffix     string
		realPos        gmysql.Position
		errMsgReg      string
	}{
		{
			// empty UUIDs
			pos:       gmysql.Position{Name: "mysql-bin.000001", Pos: 666},
			errMsgReg: ".*empty UUIDs.*",
		},
		{
			// invalid UUID in UUIDs
			pos:       gmysql.Position{Name: "mysql-bin.000002", Pos: 666},
			uuids:     []string{"invalid-uuid"},
			errMsgReg: ".*not valid.*",
		},
		{
			// real pos
			pos:            gmysql.Position{Name: "mysql-bin.000003", Pos: 666},
			uuids:          []string{"server-a-uuid.000001", "server-b-uuid.000002", "server-c-uuid.000003"},
			uuidWithSuffix: "server-c-uuid.000003", // use the latest
			uuidSuffix:     "000003",
			realPos:        gmysql.Position{Name: "mysql-bin.000003", Pos: 666},
		},
		{
			// pos match one of UUIDs
			pos:            gmysql.Position{Name: "mysql-bin|000002.000004", Pos: 666},
			uuids:          []string{"server-a-uuid.000001", "server-b-uuid.000002", "server-c-uuid.000003"},
			uuidWithSuffix: "server-b-uuid.000002", // use the latest
			uuidSuffix:     "000002",
			realPos:        gmysql.Position{Name: "mysql-bin.000004", Pos: 666},
		},
		{
			// pos not match one of UUIDs
			pos:       gmysql.Position{Name: "mysql-bin|000111.000005", Pos: 666},
			uuids:     []string{"server-a-uuid.000001", "server-b-uuid.000002", "server-c-uuid.000003"},
			errMsgReg: ".*UUID suffix.*with UUIDs.*",
		},
	}

	for _, cs := range cases {
		uuidWithSuffix, uuidSuffix, realPos, err := ExtractPos(cs.pos, cs.uuids)
		if len(cs.errMsgReg) > 0 {
			c.Assert(err, ErrorMatches, cs.errMsgReg)
		} else {
			c.Assert(err, IsNil)
		}
		c.Assert(uuidWithSuffix, Equals, cs.uuidWithSuffix)
		c.Assert(uuidSuffix, Equals, cs.uuidSuffix)
		c.Assert(realPos, DeepEquals, cs.realPos)
	}
}
