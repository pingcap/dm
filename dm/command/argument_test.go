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

package command

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/siddontang/go-mysql/mysql"
)

var _ = Suite(&testArgumentSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testArgumentSuite struct {
}

func (t *testArgumentSuite) TestTrimQuoteMark(c *C) {
	cases := []struct {
		input  string
		output string
	}{
		{
			input:  "mysql-bin.000001:234",
			output: "mysql-bin.000001:234",
		},
		{
			input:  "\"mysql-bin.000001:234\"",
			output: "mysql-bin.000001:234",
		},
		{
			input:  "\"mysql-bin.000001:234",
			output: "\"mysql-bin.000001:234",
		},
		{
			input:  "mysql-bin.000001:234\"",
			output: "mysql-bin.000001:234\"",
		},
		{
			input:  "\"\"mysql-bin.000001:234\"",
			output: "\"mysql-bin.000001:234",
		},
	}

	for _, cs := range cases {
		c.Assert(TrimQuoteMark(cs.input), Equals, cs.output)
	}
}

func (t *testArgumentSuite) TestVerifySQLOperateArgs(c *C) {
	cases := []struct {
		binlogPosStr string
		sqlPattern   string
		shardingIn   bool
		binlogPos    *mysql.Position
		regNotNil    bool
		hasError     bool
	}{
		{
			hasError: true, // must specify one of --binlog-pos and --sql-pattern,
		},
		{
			binlogPosStr: "mysql-bin.000001:234",
			sqlPattern:   "~(?i)ALTER\\s+TABLE\\s+",
			hasError:     true, // cannot specify both --binlog-pos and --sql-pattern
		},
		{
			binlogPosStr: "mysql-bin.000001:234",
			shardingIn:   true,
			hasError:     true, // cannot specify --binlog-pos with --sharding
		},
		{
			binlogPosStr: "mysql-bin.000001;234",
			hasError:     true, // invalid --binlog-pos
		},
		{
			sqlPattern: "~abc[def",
			hasError:   true, // invalid --sql-pattern
		},
		{
			binlogPosStr: "mysql-bin.000001:234",
			binlogPos:    &mysql.Position{Name: "mysql-bin.000001", Pos: 234},
			hasError:     false, // valid --binlog-pos
		},
		{
			sqlPattern: "~(?i)ALTER\\s+TABLE\\s+",
			regNotNil:  true,
			hasError:   false, // valid --sql-pattern, regexp
		},
		{
			sqlPattern: "ALTER TABLE",
			regNotNil:  true,
			hasError:   false, // valid --sql-pattern, non-regexp
		},
		{
			sqlPattern: "~(?i)ALTER\\s+TABLE\\s+",
			regNotNil:  true,
			shardingIn: true,
			hasError:   false, // valid --sql-pattern with --sharding
		},
	}

	for _, cs := range cases {
		pos, reg, err := VerifySQLOperateArgs(cs.binlogPosStr, cs.sqlPattern, cs.shardingIn)
		if cs.hasError {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
		}
		c.Assert(pos, DeepEquals, cs.binlogPos)
		if cs.regNotNil {
			c.Assert(reg, NotNil)
		} else {
			c.Assert(reg, IsNil)
		}
	}
}
