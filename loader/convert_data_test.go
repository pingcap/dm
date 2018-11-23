package loader

import (
	. "github.com/pingcap/check"
	cm "github.com/pingcap/tidb-tools/pkg/column-mapping"
	"github.com/pingcap/tidb-tools/pkg/table-router"
)

var _ = Suite(&testConvertDataSuite{})

type testConvertDataSuite struct{}

func (t *testConvertDataSuite) TestReassemble(c *C) {
	table := &tableInfo{
		sourceSchema: "test2",
		sourceTable:  "t3",
		targetSchema: "test",
		targetTable:  "t",
		columnNameList: []string{
			"id",
			"t_boolean",
			"t_bigint",
			"t_double",
			"t_decimal",
			"t_bit",
			"t_date",
			"t_datetime",
			"t_timestamp",
			"t_time",
			"t_year",
			"t_char",
			"t_varchar",
			"t_blob",
			"t_text",
			"t_enum",
			"t_set",
		},
		insertHeadStmt: "INSERT INTO t VALUES",
	}

	sql := `INSERT INTO t1 VALUES
(10,1,9223372036854775807,123.123,123456789012.1234567890120000000,"\0\0\0\0\0\0\0A","1000-01-01","9999-12-31 23:59:59","1973-12-30 15:30:00","23:59:59",1970,"x","x\"x","blob","text","enum2","a,b"),
(9,1,9223372036854775807,123.123,123456789012.1234567890120000000,"\0\0\0\0\0\0\0A","1000-01-01","9999-12-31 23:59:59","1973-12-30 15:30:00","23:59:59",1970,"x","x\",\nx","blob","text","enum2","a,b");
(8,1,9223372036854775807,123.123,123456789012.1234567890120000000,"\0\0\0\0\0\0\0A","1000-01-01","9999-12-31 23:59:59","1973-12-30 15:30:00","23:59:59",1970,"x","x\",\nx","blob","text\n","enum2",  "	a,b ");
`

	expected := []string{
		`INSERT INTO t VALUES(585520728116297738,1,9223372036854775807,123.123,123456789012.1234567890120000000,"\0\0\0\0\0\0\0A","1000-01-01","9999-12-31 23:59:59","1973-12-30 15:30:00","23:59:59",1970,"x","x\"x","blob","text","enum2","a,b"),(585520728116297737,1,9223372036854775807,123.123,123456789012.1234567890120000000,"\0\0\0\0\0\0\0A","1000-01-01","9999-12-31 23:59:59","1973-12-30 15:30:00","23:59:59",1970,"x","x\",\nx","blob","text","enum2","a,b"),(585520728116297736,1,9223372036854775807,123.123,123456789012.1234567890120000000,"\0\0\0\0\0\0\0A","1000-01-01","9999-12-31 23:59:59","1973-12-30 15:30:00","23:59:59",1970,"x","x\",\nx","blob","text\n","enum2","	a,b ");`,
		`INSERT INTO t VALUES(10,1,9223372036854775807,123.123,123456789012.1234567890120000000,"\0\0\0\0\0\0\0A","1000-01-01","9999-12-31 23:59:59","1973-12-30 15:30:00","23:59:59",1970,"test:x","x\"x","blob","text","enum2","a,b"),(9,1,9223372036854775807,123.123,123456789012.1234567890120000000,"\0\0\0\0\0\0\0A","1000-01-01","9999-12-31 23:59:59","1973-12-30 15:30:00","23:59:59",1970,"test:x","x\",\nx","blob","text","enum2","a,b"),(8,1,9223372036854775807,123.123,123456789012.1234567890120000000,"\0\0\0\0\0\0\0A","1000-01-01","9999-12-31 23:59:59","1973-12-30 15:30:00","23:59:59",1970,"test:x","x\",\nx","blob","text\n","enum2","	a,b ");`,
	}

	rules := []*cm.Rule{
		{
			PatternSchema: "test*",
			PatternTable:  "t*",
			TargetColumn:  "id",
			Expression:    cm.PartitionID,
			Arguments:     []string{"1", "test", "t"},
		}, {
			PatternSchema: "test*",
			PatternTable:  "t*",
			TargetColumn:  "t_char",
			Expression:    cm.AddPrefix,
			Arguments:     []string{"test:"},
		},
	}

	for i, r := range rules {
		columnMapping, err := cm.NewMapping(false, []*cm.Rule{r})
		c.Assert(err, IsNil)

		query, err := reassemble([]byte(sql), table, columnMapping)
		c.Assert(err, IsNil)
		c.Assert(expected[i], Equals, query)
	}
}

func (t *testConvertDataSuite) TestParseTable(c *C) {
	rules := []*router.TableRule{
		{"test*", "t*", "test", "t"},
	}

	expectedTableInfo := &tableInfo{
		sourceSchema: "test1",
		sourceTable:  "t2",
		targetSchema: "test",
		targetTable:  "t",
		columnNameList: []string{
			"id",
			"t_boolean",
			"t_bigint",
			"t_double",
			"t_decimal",
			"t_bit",
			"t_date",
			"t_datetime",
			"t_timestamp",
			"t_time",
			"t_year",
			"t_char",
			"t_varchar",
			"t_blob",
			"t_text",
			"t_enum",
			"t_set",
			"t_json",
		},
		insertHeadStmt: "INSERT INTO `t` VALUES",
	}

	r, err := router.NewTableRouter(false, rules)
	c.Assert(err, IsNil)

	tableInfo, err := parseTable(r, "test1", "t2", "./dumpfile/test1.t2-schema.sql")
	c.Assert(err, IsNil)
	c.Assert(tableInfo, DeepEquals, expectedTableInfo)
}
