package event

import (
	"bytes"
	"fmt"

	. "github.com/pingcap/check"
	gmysql "github.com/siddontang/go-mysql/mysql"

	"github.com/pingcap/dm/pkg/gtid"
)

var _ = Suite(&testSchemaSuite{})

type testSchemaSuite struct {
}

func (t *testSchemaSuite) TestGenDatabaseEvent(c *C) {
	var (
		serverID  uint32 = 101
		latestPos uint32 = 123
		schema           = "test_db"
		table            = "test_tbl"
	)

	// only some simple tests in this case and we can test parsing a binlog file including common header, DDL and DML in another case.

	// test CREATE/DROP DATABASE for MySQL
	flavor := gmysql.MySQLFlavor
	gSetStr := "03fc0263-28c7-11e7-a653-6c0b84d59f30:123"
	latestGTID, err := gtid.ParserGTID(flavor, gSetStr)
	c.Assert(err, IsNil)

	// CREATE DATABASE
	result, err := GenCreateDatabase(flavor, serverID, latestPos, schema, latestGTID)
	c.Assert(err, IsNil)
	c.Assert(len(result.Events), Equals, 2)
	// simply check content, more check did in `generate_test.go`
	c.Assert(bytes.Contains(result.Data, []byte("CREATE DATABASE")), IsTrue)
	c.Assert(bytes.Contains(result.Data, []byte(schema)), IsTrue)
	c.Assert(result.LatestPos, Equals, latestPos+uint32(len(result.Data)))
	c.Assert(result.LatestGTID.String(), Equals, "03fc0263-28c7-11e7-a653-6c0b84d59f30:124")

	latestPos = result.LatestPos // update latest pos
	latestGTID = result.LatestGTID

	// DROP DATABASE
	result, err = GenDropDatabase(flavor, serverID, latestPos, schema, latestGTID)
	c.Assert(err, IsNil)
	c.Assert(len(result.Events), Equals, 2)
	c.Assert(bytes.Contains(result.Data, []byte("DROP DATABASE")), IsTrue)
	c.Assert(bytes.Contains(result.Data, []byte(schema)), IsTrue)
	c.Assert(result.LatestPos, Equals, latestPos+uint32(len(result.Data)))
	c.Assert(result.LatestGTID.String(), Equals, "03fc0263-28c7-11e7-a653-6c0b84d59f30:125")

	latestPos = result.LatestPos // update latest pos
	latestGTID = result.LatestGTID

	// test CREATE/DROP table for MariaDB
	flavor = gmysql.MariaDBFlavor
	gSetStr = fmt.Sprintf("1-%d-3", serverID)
	latestGTID, err = gtid.ParserGTID(flavor, gSetStr)
	c.Assert(err, IsNil)

	// CREATE TABLE
	query := fmt.Sprintf("CREATE TABLE `%s` (c1 int)", table)
	result, err = GenCreateTable(flavor, serverID, latestPos, schema, query, latestGTID)
	c.Assert(err, IsNil)
	c.Assert(len(result.Events), Equals, 2)
	c.Assert(bytes.Contains(result.Data, []byte("CREATE TABLE")), IsTrue)
	c.Assert(bytes.Contains(result.Data, []byte(table)), IsTrue)
	c.Assert(result.LatestPos, Equals, latestPos+uint32(len(result.Data)))
	c.Assert(result.LatestGTID.String(), Equals, fmt.Sprintf("1-%d-4", serverID))

	latestPos = result.LatestPos // update latest pos
	latestGTID = result.LatestGTID

	// DROP DATABASE
	result, err = GenDropTable(flavor, serverID, latestPos, schema, table, latestGTID)
	c.Assert(err, IsNil)
	c.Assert(len(result.Events), Equals, 2)
	c.Assert(bytes.Contains(result.Data, []byte("DROP TABLE")), IsTrue)
	c.Assert(bytes.Contains(result.Data, []byte(table)), IsTrue)
	c.Assert(result.LatestPos, Equals, latestPos+uint32(len(result.Data)))
	c.Assert(result.LatestGTID.String(), Equals, fmt.Sprintf("1-%d-5", serverID))

	latestPos = result.LatestPos // update latest pos
	latestGTID = result.LatestGTID
}
