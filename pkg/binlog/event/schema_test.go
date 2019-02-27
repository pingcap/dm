package event

import (
	"bytes"

	. "github.com/pingcap/check"
)

var _ = Suite(&testSchemaSuite{})

type testSchemaSuite struct {
}

func (t *testSchemaSuite) TestGenDatabaseEvent(c *C) {
	var (
		serverID  uint32 = 101
		latestPos uint32 = 123
		schema           = "test_db"
	)

	// CREATE DATABASE
	events, data, err := GenCreateDatabase(serverID, latestPos, schema)
	c.Assert(err, IsNil)
	c.Assert(len(events), Equals, 1)
	c.Assert(events[0].RawData, DeepEquals, data)
	// simply check content, more check did in `generate_test.go`
	c.Assert(bytes.Contains(data, []byte("CREATE DATABASE")), IsTrue)
	c.Assert(bytes.Contains(data, []byte(schema)), IsTrue)

	// DROP DATABASE
	events, data, err = GenDropDatabase(serverID, latestPos, schema)
	c.Assert(err, IsNil)
	c.Assert(len(events), Equals, 1)
	c.Assert(events[0].RawData, DeepEquals, data)
	// simply check content, more check did in `generate_test.go`
	c.Assert(bytes.Contains(data, []byte("DROP DATABASE")), IsTrue)
	c.Assert(bytes.Contains(data, []byte(schema)), IsTrue)
}
