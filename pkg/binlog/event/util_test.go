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

// binlog events generator for MySQL used to generate some binlog events for tests.
// Readability takes precedence over performance.

package event

import (
	"io"

	. "github.com/pingcap/check"

	"github.com/pingcap/dm/pkg/terror"
)

var _ = Suite(&testUtilSuite{})

type testUtilSuite struct {
}

type testCase struct {
	input  []byte
	output map[byte][]byte
	err    error
}

func (t *testUtilSuite) TestStatusVarsToKV(c *C) {
	testCases := []testCase{
		// only Q_FLAGS2_CODE
		{
			[]byte{0, 0, 0, 0, 0},
			map[byte][]byte{
				0: {0, 0, 0, 0},
			},
			nil,
		},
		// copied from a integration test
		{
			[]byte{0, 0, 0, 0, 0, 1, 4, 0, 8, 0, 0, 0, 0, 0, 6, 3, 115, 116, 100, 4, 33, 0, 33, 0, 8, 0, 12, 1, 97, 108, 108, 95, 109, 111, 100, 101, 0},
			map[byte][]byte{
				0:  {0, 0, 0, 0},
				1:  {4, 0, 8, 0, 0, 0, 0, 0},
				6:  {3, 115, 116, 100},
				4:  {33, 0, 33, 0, 8, 0},
				12: {1, 97, 108, 108, 95, 109, 111, 100, 101, 0},
			},
			nil,
		},
		// wrong input
		{
			[]byte{0, 0, 0, 0, 0, 1},
			nil,
			terror.ErrBinlogStatusVarsParse.Delegate(io.EOF, []byte{0, 0, 0, 0, 0, 1}, 6),
		},
	}

	for _, t := range testCases {
		vars, err := statusVarsToKV(t.input)
		if t.err != nil {
			c.Assert(err.Error(), Equals, t.err.Error())
		} else {
			c.Assert(err, IsNil)
			c.Assert(vars, DeepEquals, t.output)
		}
	}
}
