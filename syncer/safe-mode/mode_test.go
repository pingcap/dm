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

	// Add n
	err = m.Add(101)
	c.Assert(m.Enable(), IsTrue)
	err = m.Add(-1)
	c.Assert(m.Enable(), IsTrue)
	err = m.Add(-100)
	c.Assert(m.Enable(), IsFalse)

	// IncrForTable
	schema := "schema"
	table := "table"
	err = m.IncrForTable(schema, table)
	err = m.IncrForTable(schema, table) // re-Add
	c.Assert(m.Enable(), IsTrue)
	err = m.DescForTable(schema, table)
	c.Assert(m.Enable(), IsFalse)

	// Add n + IncrForTable
	err = m.Add(100)
	err = m.IncrForTable(schema, table)
	c.Assert(m.Enable(), IsTrue)
	err = m.Add(-100)
	err = m.DescForTable(schema, table)
	c.Assert(m.Enable(), IsFalse)

	// Add becomes to negative
	err = m.Add(-1)
	c.Assert(err, NotNil)
}
