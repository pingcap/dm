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

package utils

import (
	"bytes"
	"fmt"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/tidb-tools/pkg/filter"
)

var _ = Suite(&testCommonSuite{})

type testCommonSuite struct {
}

func (s *testCommonSuite) TestTrimCtrlChars(c *C) {
	ddl := "create table if not exists foo.bar(id int)"
	controlChars := make([]byte, 0, 33)
	nul := byte(0x00)
	for i := 0; i < 32; i++ {
		controlChars = append(controlChars, nul)
		nul++
	}
	controlChars = append(controlChars, 0x7f)

	parser2 := parser.New()
	var buf bytes.Buffer
	for _, char := range controlChars {
		buf.WriteByte(char)
		buf.WriteByte(char)
		buf.WriteString(ddl)
		buf.WriteByte(char)
		buf.WriteByte(char)

		newDDL := TrimCtrlChars(buf.String())
		c.Assert(newDDL, Equals, ddl)

		_, err := parser2.ParseOneStmt(newDDL, "", "")
		c.Assert(err, IsNil)
		buf.Reset()
	}
}

func (s *testCommonSuite) TestFetchAllDoTables(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	// empty filter, exclude system schemas
	ba, err := filter.New(false, nil)
	c.Assert(err, IsNil)

	// no schemas need to do.
	mock.ExpectQuery(`SHOW DATABASES`).WillReturnRows(sqlmock.NewRows([]string{"Database"}))
	got, err := FetchAllDoTables(db, ba)
	c.Assert(err, IsNil)
	c.Assert(got, HasLen, 0)
	mock.ExpectationsWereMet()

	// only system schemas exist, still no need to do.
	schemas := []string{"information_schema", "mysql", "performance_schema", "sys", filter.DMHeartbeatSchema}
	rows := sqlmock.NewRows([]string{"Database"})
	s.addRowsForSchemas(rows, schemas)
	mock.ExpectQuery(`SHOW DATABASES`).WillReturnRows(rows)
	got, err = FetchAllDoTables(db, ba)
	c.Assert(err, IsNil)
	c.Assert(got, HasLen, 0)
	mock.ExpectationsWereMet()

	// schemas without tables in them.
	doSchema := "test_db"
	schemas = []string{"information_schema", "mysql", "performance_schema", "sys", filter.DMHeartbeatSchema, doSchema}
	rows = sqlmock.NewRows([]string{"Database"})
	s.addRowsForSchemas(rows, schemas)
	mock.ExpectQuery(`SHOW DATABASES`).WillReturnRows(rows)
	mock.ExpectQuery(fmt.Sprintf("SHOW FULL TABLES IN `%s` WHERE Table_Type != 'VIEW'", doSchema)).WillReturnRows(
		sqlmock.NewRows([]string{fmt.Sprintf("Tables_in_%s", doSchema), "Table_type"}))
	got, err = FetchAllDoTables(db, ba)
	c.Assert(err, IsNil)
	c.Assert(got, HasLen, 0)
	mock.ExpectationsWereMet()

	// do all tables under the schema.
	rows = sqlmock.NewRows([]string{"Database"})
	s.addRowsForSchemas(rows, schemas)
	mock.ExpectQuery(`SHOW DATABASES`).WillReturnRows(rows)
	tables := []string{"tbl1", "tbl2", "exclude_tbl"}
	rows = sqlmock.NewRows([]string{fmt.Sprintf("Tables_in_%s", doSchema), "Table_type"})
	s.addRowsForTables(rows, tables)
	mock.ExpectQuery(fmt.Sprintf("SHOW FULL TABLES IN `%s` WHERE Table_Type != 'VIEW'", doSchema)).WillReturnRows(rows)
	got, err = FetchAllDoTables(db, ba)
	c.Assert(err, IsNil)
	c.Assert(got, HasLen, 1)
	c.Assert(got[doSchema], DeepEquals, tables)
	mock.ExpectationsWereMet()

	// use a block-allow-list to fiter some tables
	ba, err = filter.New(false, &filter.Rules{
		DoDBs: []string{doSchema},
		DoTables: []*filter.Table{
			{Schema: doSchema, Name: "tbl1"},
			{Schema: doSchema, Name: "tbl2"},
		},
	})
	c.Assert(err, IsNil)

	rows = sqlmock.NewRows([]string{"Database"})
	s.addRowsForSchemas(rows, schemas)
	mock.ExpectQuery(`SHOW DATABASES`).WillReturnRows(rows)
	rows = sqlmock.NewRows([]string{fmt.Sprintf("Tables_in_%s", doSchema), "Table_type"})
	s.addRowsForTables(rows, tables)
	mock.ExpectQuery(fmt.Sprintf("SHOW FULL TABLES IN `%s` WHERE Table_Type != 'VIEW'", doSchema)).WillReturnRows(rows)
	got, err = FetchAllDoTables(db, ba)
	c.Assert(err, IsNil)
	c.Assert(got, HasLen, 1)
	c.Assert(got[doSchema], DeepEquals, []string{"tbl1", "tbl2"})
	mock.ExpectationsWereMet()
}

func (s *testCommonSuite) addRowsForSchemas(rows *sqlmock.Rows, schemas []string) {
	for _, d := range schemas {
		rows.AddRow(d)
	}
}

func (s *testCommonSuite) addRowsForTables(rows *sqlmock.Rows, tables []string) {
	for _, table := range tables {
		rows.AddRow(table, "BASE TABLE")
	}
}
