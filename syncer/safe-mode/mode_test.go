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

package mode

import (
	"testing"

	. "github.com/pingcap/check"
)

var _ = Suite(&testModeSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testModeSuite struct {
}

func (t *testModeSuite) TestMode(c *C) {
	m := NewSafeMode()
	c.Assert(m.Enable(), IsFalse)

	// Add 1
	err := m.Add(1)
	c.Assert(err, IsNil)
	c.Assert(m.Enable(), IsTrue)
	err = m.Add(-1)
	c.Assert(m.Enable(), IsFalse)
	c.Assert(err, IsNil)

	// Add n
	err = m.Add(101)
	c.Assert(m.Enable(), IsTrue)
	c.Assert(err, IsNil)
	err = m.Add(-1)
	c.Assert(m.Enable(), IsTrue)
	c.Assert(err, IsNil)
	err = m.Add(-100)
	c.Assert(m.Enable(), IsFalse)
	c.Assert(err, IsNil)

	// IncrForTable
	schema := "schema"
	table := "table"
	err = m.IncrForTable(schema, table)
	c.Assert(err, IsNil)
	err = m.IncrForTable(schema, table) // re-Add
	c.Assert(err, IsNil)
	c.Assert(m.Enable(), IsTrue)
	err = m.DescForTable(schema, table)
	c.Assert(err, IsNil)
	c.Assert(m.Enable(), IsFalse)

	// Add n + IncrForTable
	err = m.Add(100)
	c.Assert(err, IsNil)
	err = m.IncrForTable(schema, table)
	c.Assert(err, IsNil)
	c.Assert(m.Enable(), IsTrue)
	err = m.Add(-100)
	c.Assert(err, IsNil)
	err = m.DescForTable(schema, table)
	c.Assert(m.Enable(), IsFalse)
	c.Assert(err, IsNil)

	// Add becomes to negative
	err = m.Add(-1)
	c.Assert(err, NotNil)
}
