package syncer

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

var _ = Suite(&testUtilSuite{})

type testUtilSuite struct {
}

func (t *testUtilSuite) TestTableNameForDML(c *C) {
	cases := []struct {
		sql      string
		schema   string
		table    string
		hasError bool
	}{
		{
			sql:      "INSERT INTO db1.tbl1 VALUES (1)",
			schema:   "db1",
			table:    "tbl1",
			hasError: false,
		},
		{
			sql:      "REPLACE INTO `db1`.`tbl1` (c1) VALUES (1)", // parsed as an ast.InsertStmt
			schema:   "db1",
			table:    "tbl1",
			hasError: false,
		},
		{
			sql:      "INSERT INTO `tbl1` VALUES (2)",
			schema:   "",
			table:    "tbl1",
			hasError: false,
		},
		{
			sql:      "UPDATE `db1`.`tbl1` SET c1=2 WHERE c1=1",
			schema:   "db1",
			table:    "tbl1",
			hasError: false,
		},
		{
			sql:      "DELETE FROM tbl1 WHERE c1=2",
			schema:   "",
			table:    "tbl1",
			hasError: false,
		},
		{
			sql:      "SELECT * FROM db1.tbl1",
			schema:   "",
			table:    "",
			hasError: true,
		},
	}

	parser2 := parser.New()
	for _, cs := range cases {
		stmt, err := parser2.ParseOneStmt(cs.sql, "", "")
		c.Assert(err, IsNil)
		dml, ok := stmt.(ast.DMLNode)
		c.Assert(ok, IsTrue)
		schema, table, err := tableNameForDML(dml)
		if cs.hasError {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
		}
		c.Assert(schema, Equals, cs.schema)
		c.Assert(table, Equals, cs.table)
	}
}
