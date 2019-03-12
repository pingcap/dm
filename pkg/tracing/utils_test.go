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

package tracing

import (
	tc "github.com/pingcap/check"
)

func (ts *TracerTestSuite) TestIDGenerator(c *tc.C) {
	testCases := []struct {
		source   string
		offset   int64
		expected string
	}{
		{"replica-01.syncer", 0, "replica-01.syncer.1"},
		{"replica-01.syncer", 1, "replica-01.syncer.3"},
		{"replica-02.syncer", 2, "replica-02.syncer.3"},
		{"replica-02.syncer", 0, "replica-02.syncer.4"},
		{"replica-01.syncer", 3, "replica-01.syncer.7"},
	}

	idGen := NewIDGen()
	for _, t := range testCases {
		id := idGen.NextID(t.source, t.offset)
		c.Assert(id, tc.Equals, t.expected)
	}
}

func (ts *TracerTestSuite) TestDataChecksum(c *tc.C) {
	testCases := []struct {
		input    []interface{}
		checksum uint32
	}{
		{[]interface{}{}, 0},
		{[]interface{}{12, 301.5, true, "string", []byte("hello")}, 1247364640},
		{[]interface{}{12, 301.5, true, "string", nil, []byte("hello")}, 3571728787},
	}

	for _, t := range testCases {
		checksum, err := DataChecksum(t.input)
		c.Assert(err, tc.IsNil)
		c.Assert(checksum, tc.Equals, t.checksum)
	}
}
