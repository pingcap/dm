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
)

func (s *testSyncerSuite) TestCastUnsigned(c *C) {
	// ref: https://dev.mysql.com/doc/refman/5.7/en/integer-types.html
	cases := []struct {
		data     interface{}
		unsigned bool
		Type     string
		expected interface{}
	}{
		{int8(-math.Exp2(7)), false, "tinyint(4)", int8(-math.Exp2(7))}, // TINYINT
		{int8(-math.Exp2(7)), true, "tinyint(3) unsigned", uint8(math.Exp2(7))},
		{int16(-math.Exp2(15)), false, "smallint(6)", int16(-math.Exp2(15))}, //SMALLINT
		{int16(-math.Exp2(15)), true, "smallint(5) unsigned", uint16(math.Exp2(15))},
		{int32(-math.Exp2(23)), false, "mediumint(9)", int32(-math.Exp2(23))}, //MEDIUMINT
		{int32(-math.Exp2(23)), true, "mediumint(8) unsigned", uint32(math.Exp2(23))},
		{int32(-math.Exp2(31)), false, "int(11)", int32(-math.Exp2(31))}, // INT
		{int32(-math.Exp2(31)), true, "int(10) unsigned", uint32(math.Exp2(31))},
		{int64(-math.Exp2(63)), false, "bigint(20)", int64(-math.Exp2(63))},                                 // BIGINT
		{int64(-math.Exp2(63)), true, "bigint(20) unsigned", strconv.FormatUint(uint64(math.Exp2(63)), 10)}, // special case use string to represent uint64
	}
	for _, cs := range cases {
		obtained := castUnsigned(cs.data, cs.unsigned, cs.Type)
		c.Assert(obtained, Equals, cs.expected)
	}
}

func (s *testSyncerSuite) TestGenColumnPlaceholders(c *C) {
	placeholderStr := genColumnPlaceholders(1)
	c.Assert(placeholderStr, Equals, "?")

	placeholderStr = genColumnPlaceholders(3)
	c.Assert(placeholderStr, Equals, "?,?,?")
}

func (s *testSyncerSuite) TestGenColumnList(c *C) {
	columns := []*column{
		{
			name: "a",
		}, {
			name: "b",
		}, {
			name: "c",
		},
	}

	columnList := genColumnList(columns[:1])
	c.Assert(columnList, Equals, "`a`")

	columnList = genColumnList(columns)
	c.Assert(columnList, Equals, "`a`,`b`,`c`")
}

func (s *testSyncerSuite) TestFindFitIndex(c *C) {
	pkColumns := []*column{
		{
			name: "a",
		}, {
			name: "b",
		},
	}
	indexColumns := []*column{
		{
			name: "c",
		},
	}
	indexColumnsNotNull := []*column{
		{
			name:    "d",
			NotNull: true,
		},
	}

	columns := findFitIndex(map[string][]*column{
		"primary": pkColumns,
		"index":   indexColumns,
	})
	c.Assert(columns, HasLen, 2)
	c.Assert(columns[0].name, Equals, "a")
	c.Assert(columns[1].name, Equals, "b")

	columns = findFitIndex(map[string][]*column{
		"index": indexColumns,
	})
	c.Assert(columns, HasLen, 0)

	columns = findFitIndex(map[string][]*column{
		"index": indexColumnsNotNull,
	})
	c.Assert(columns, HasLen, 1)
	c.Assert(columns[0].name, Equals, "d")
}
