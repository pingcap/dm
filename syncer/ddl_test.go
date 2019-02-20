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
	"bytes"
	"database/sql"

	. "github.com/pingcap/check"
	"github.com/pingcap/dm/dm/config"
	parserpkg "github.com/pingcap/dm/pkg/parser"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/pingcap/tidb-tools/pkg/filter"
)

func (s *testSyncerSuite) TestTrimCtrlChars(c *C) {
	ddl := "create table if not exists foo.bar(id int)"
	controlChars := make([]byte, 0, 33)
	nul := byte(0x00)
	for i := 0; i < 32; i++ {
		controlChars = append(controlChars, nul)
		nul++
	}
	controlChars = append(controlChars, 0x7f)

	var buf bytes.Buffer
	p, err := utils.GetParser(s.db, false)
	c.Assert(err, IsNil)

	for _, char := range controlChars {
		buf.WriteByte(char)
		buf.WriteByte(char)
		buf.WriteString(ddl)
		buf.WriteByte(char)
		buf.WriteByte(char)

		newDDL := utils.TrimCtrlChars(buf.String())
		c.Assert(len(newDDL), Equals, len(ddl))

		_, err := p.ParseOneStmt(newDDL, "", "")
		c.Assert(err, IsNil)
		buf.Reset()
	}
}

func (s *testSyncerSuite) TestAnsiQuotes(c *C) {
	ansiQuotesCases := []string{
		"create database `test`",
		"create table `test`.`test`(id int)",
		"create table `test`.\"test\" (id int)",
		"create table \"test\".`test` (id int)",
		"create table \"test\".\"test\"",
		"create table test.test (\"id\" int)",
		"insert into test.test (\"id\") values('a')",
	}
	result, err := s.db.Query("select @@global.sql_mode")
	var sqlMode sql.NullString
	c.Assert(err, IsNil)
	defer result.Close()
	for result.Next() {
		err = result.Scan(&sqlMode)
		c.Assert(err, IsNil)
		break
	}
	c.Assert(sqlMode.Valid, IsTrue)

	_, err = s.db.Exec("set @@global.sql_mode='ANSI_QUOTES'")
	c.Assert(err, IsNil)
	// recover original sql_mode
	defer s.db.Exec("set @@global.sql_mode = ?", sqlMode)

	parser, err := utils.GetParser(s.db, false)
	c.Assert(err, IsNil)

	for _, sql := range ansiQuotesCases {
		_, err = parser.ParseOneStmt(sql, "", "")
		c.Assert(err, IsNil)
	}

}

func (s *testSyncerSuite) TestDDLWithDashComments(c *C) {
	sql := `--
-- this is a comment.
--
CREATE TABLE test.test_table_with_c (id int);
`

	parser, err := utils.GetParser(s.db, false)
	c.Assert(err, IsNil)

	_, err = parserpkg.Parse(parser, sql, "", "")
	c.Assert(err, IsNil)
}

func (s *testSyncerSuite) TestCommentQuote(c *C) {
	sql := "ALTER TABLE schemadb.ep_edu_course_message_auto_reply MODIFY answer JSON COMMENT '回复的内容-格式为list，有两个字段：\"answerType\"：//''发送客服消息类型：1-文本消息，2-图片，3-图文链接''；  answer：回复内容';"
	expectedSQL := "ALTER TABLE `schemadb`.`ep_edu_course_message_auto_reply` MODIFY COLUMN `answer` JSON COMMENT '回复的内容-格式为list，有两个字段：\"answerType\"：//''发送客服消息类型：1-文本消息，2-图片，3-图文链接''；  answer：回复内容'"

	parser, err := utils.GetParser(s.db, false)
	c.Assert(err, IsNil)

	stmt, err := parser.ParseOneStmt(sql, "", "")
	c.Assert(err, IsNil)

	syncer := &Syncer{}
	sqls, _, err := syncer.resolveDDLSQL(parser, stmt, "schemadb")
	c.Assert(err, IsNil)
	c.Assert(len(sqls), Equals, 1)
	c.Assert(sqls[0], Equals, expectedSQL)
}

func (s *testSyncerSuite) TestIgnoreDMLInQuery(c *C) {
	cases := []struct {
		sql      string
		schema   string
		ignore   bool
		isDDL    bool
		hasError bool
	}{
		{
			sql:      "CREATE TABLE do_db.do_table (c1 INT)",
			schema:   "",
			ignore:   false,
			isDDL:    true,
			hasError: false,
		},
		{
			sql:      "INSERT INTO do_db.do_table VALUES (1)",
			schema:   "",
			ignore:   false,
			isDDL:    false,
			hasError: true,
		},
		{
			sql:      "INSERT INTO ignore_db.ignore_table VALUES (1)",
			schema:   "",
			ignore:   true,
			isDDL:    false,
			hasError: false,
		},
		{
			sql:      "UPDATE `ignore_db`.`ignore_table` SET c1=2 WHERE c1=1",
			schema:   "ignore_db",
			ignore:   true,
			isDDL:    false,
			hasError: false,
		},
		{
			sql:      "DELETE FROM `ignore_table` WHERE c1=2",
			schema:   "ignore_db",
			ignore:   true,
			isDDL:    false,
			hasError: false,
		},
		{
			sql:      "SELECT * FROM ignore_db.ignore_table",
			schema:   "",
			ignore:   false,
			isDDL:    false,
			hasError: true,
		},
	}

	cfg := &config.SubTaskConfig{
		BWList: &filter.Rules{
			IgnoreDBs: []string{"ignore_db"},
		},
	}
	syncer := NewSyncer(cfg)

	parser, err := utils.GetParser(s.db, false)
	c.Assert(err, IsNil)

	for _, cs := range cases {
		pr, err := syncer.parseDDLSQL(cs.sql, parser, cs.schema)
		if cs.hasError {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
		}
		c.Assert(pr.ignore, Equals, cs.ignore)
		c.Assert(pr.isDDL, Equals, cs.isDDL)
	}
}
