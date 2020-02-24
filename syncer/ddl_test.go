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

	"github.com/pingcap/dm/dm/config"
	tcontext "github.com/pingcap/dm/pkg/context"
	parserpkg "github.com/pingcap/dm/pkg/parser"
	"github.com/pingcap/dm/pkg/utils"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/tidb-tools/pkg/filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
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
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	p, err := s.mockParser(db, mock)
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

	db, mock, err := sqlmock.New()
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("sql_mode", "ANSI_QUOTES"))
	c.Assert(err, IsNil)

	parser, err := utils.GetParser(db, false)
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

	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	parser, err := s.mockParser(db, mock)
	c.Assert(err, IsNil)

	_, err = parserpkg.Parse(parser, sql, "", "")
	c.Assert(err, IsNil)
}

func (s *testSyncerSuite) TestCommentQuote(c *C) {
	sql := "ALTER TABLE schemadb.ep_edu_course_message_auto_reply MODIFY answer JSON COMMENT '回复的内容-格式为list，有两个字段：\"answerType\"：//''发送客服消息类型：1-文本消息，2-图片，3-图文链接''；  answer：回复内容';"
	expectedSQL := "ALTER TABLE `schemadb`.`ep_edu_course_message_auto_reply` MODIFY COLUMN `answer` JSON COMMENT '回复的内容-格式为list，有两个字段：\"answerType\"：//''发送客服消息类型：1-文本消息，2-图片，3-图文链接''；  answer：回复内容'"

	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	parser, err := s.mockParser(db, mock)
	c.Assert(err, IsNil)

	stmt, err := parser.ParseOneStmt(sql, "", "")
	c.Assert(err, IsNil)

	syncer := &Syncer{}
	sqls, _, err := syncer.resolveDDLSQL(tcontext.Background(), parser, stmt, "schemadb")
	c.Assert(err, IsNil)
	c.Assert(len(sqls), Equals, 1)
	c.Assert(sqls[0], Equals, expectedSQL)
}

func (s *testSyncerSuite) TestresolveDDLSQL(c *C) {
	// duplicate with pkg/parser
	sqls := []string{
		"create schema `s1`",
		"create schema if not exists `s1`",
		"drop schema `s1`",
		"drop schema if exists `s1`",
		"drop table `s1`.`t1`",
		"drop table `s1`.`t1`, `s2`.`t2`",
		"drop table `s1`.`t1`, `s2`.`t2`, `xx`",
		"create table `s1`.`t1` (id int)",
		"create table `t1` (id int)",
		"create table `t1` like `t2`",
		"create table `s1`.`t1` like `t2`",
		"create table `t1` like `xx`.`t2`",
		"truncate table `t1`",
		"truncate table `s1`.`t1`",
		"rename table `s1`.`t1` to `s2`.`t2`",
		"rename table `t1` to `t2`, `s1`.`t1` to `t2`",
		"drop index i1 on `s1`.`t1`",
		"drop index i1 on `t1`",
		"create index i1 on `t1`(`c1`)",
		"create index i1 on `s1`.`t1`(`c1`)",
		"alter table `t1` add column c1 int, drop column c2",
		"alter table `s1`.`t1` add column c1 int, rename to `t2`, drop column c2",
		"alter table `s1`.`t1` add column c1 int, rename to `xx`.`t2`, drop column c2",
	}

	expectedSQLs := [][]string{
		{"CREATE DATABASE IF NOT EXISTS `s1`"},
		{"CREATE DATABASE IF NOT EXISTS `s1`"},
		{"DROP DATABASE IF EXISTS `s1`"},
		{"DROP DATABASE IF EXISTS `s1`"},
		{"DROP TABLE IF EXISTS `s1`.`t1`"},
		{"DROP TABLE IF EXISTS `s1`.`t1`", "DROP TABLE IF EXISTS `s2`.`t2`"},
		{"DROP TABLE IF EXISTS `s1`.`t1`", "DROP TABLE IF EXISTS `s2`.`t2`", "DROP TABLE IF EXISTS `test`.`xx`"},
		{"CREATE TABLE IF NOT EXISTS `s1`.`t1` (`id` INT)"},
		{"CREATE TABLE IF NOT EXISTS `test`.`t1` (`id` INT)"},
		{"CREATE TABLE IF NOT EXISTS `test`.`t1` LIKE `test`.`t2`"},
		{"CREATE TABLE IF NOT EXISTS `s1`.`t1` LIKE `test`.`t2`"},
		{"CREATE TABLE IF NOT EXISTS `test`.`t1` LIKE `xx`.`t2`"},
		{"TRUNCATE TABLE `test`.`t1`"},
		{"TRUNCATE TABLE `s1`.`t1`"},
		{"RENAME TABLE `s1`.`t1` TO `s2`.`t2`"},
		{"RENAME TABLE `test`.`t1` TO `test`.`t2`", "RENAME TABLE `s1`.`t1` TO `test`.`t2`"},
		{"DROP INDEX IF EXISTS `i1` ON `s1`.`t1`"},
		{"DROP INDEX IF EXISTS `i1` ON `test`.`t1`"},
		{"CREATE INDEX `i1` ON `test`.`t1` (`c1`)"},
		{"CREATE INDEX `i1` ON `s1`.`t1` (`c1`)"},
		{"ALTER TABLE `test`.`t1` ADD COLUMN `c1` INT", "ALTER TABLE `test`.`t1` DROP COLUMN `c2`"},
		{"ALTER TABLE `s1`.`t1` ADD COLUMN `c1` INT", "ALTER TABLE `s1`.`t1` RENAME AS `test`.`t2`", "ALTER TABLE `test`.`t2` DROP COLUMN `c2`"},
		{"ALTER TABLE `s1`.`t1` ADD COLUMN `c1` INT", "ALTER TABLE `s1`.`t1` RENAME AS `xx`.`t2`", "ALTER TABLE `xx`.`t2` DROP COLUMN `c2`"},
	}

	targetSQLs := [][]string{
		{"CREATE DATABASE IF NOT EXISTS `xs1`"},
		{"CREATE DATABASE IF NOT EXISTS `xs1`"},
		{"DROP DATABASE IF EXISTS `xs1`"},
		{"DROP DATABASE IF EXISTS `xs1`"},
		{"DROP TABLE IF EXISTS `xs1`.`t1`"},
		{"DROP TABLE IF EXISTS `xs1`.`t1`", ""},
		{"DROP TABLE IF EXISTS `xs1`.`t1`", "", ""},
		{"CREATE TABLE IF NOT EXISTS `xs1`.`t1` (`id` INT)"},
		{""},
		{""},
		{""},
		{""},
		{""},
		{"TRUNCATE TABLE `xs1`.`t1`"},
		{""},
		{"", ""},
		{"DROP INDEX IF EXISTS `i1` ON `xs1`.`t1`"},
		{""},
		{""},
		{"CREATE INDEX `i1` ON `xs1`.`t1` (`c1`)"},
		{"", ""},
		{"ALTER TABLE `xs1`.`t1` ADD COLUMN `c1` INT", "", ""},
		{"ALTER TABLE `xs1`.`t1` ADD COLUMN `c1` INT", "", ""},
	}

	p := parser.New()
	cfg := &config.SubTaskConfig{
		BWList: &filter.Rules{
			DoDBs: []string{"s1"},
		},
	}
	var err error
	syncer := NewSyncer(cfg, nil)
	syncer.bwList, err = filter.New(syncer.cfg.CaseSensitive, syncer.cfg.BWList)
	c.Assert(err, IsNil)

	syncer.tableRouter, err = router.NewTableRouter(false, []*router.TableRule{
		{
			SchemaPattern: "s1",
			TargetSchema:  "xs1",
		},
	})
	c.Assert(err, IsNil)

	for i, sql := range sqls {
		result, err := syncer.parseDDLSQL(sql, p, "test")
		c.Assert(err, IsNil)
		c.Assert(result.ignore, IsFalse)
		c.Assert(result.isDDL, IsTrue)

		statements, _, err := syncer.resolveDDLSQL(tcontext.Background(), p, result.stmt, "test")
		c.Assert(err, IsNil)
		c.Assert(statements, DeepEquals, expectedSQLs[i])

		for j, statement := range statements {
			s, _, _, err := syncer.handleDDL(p, "test", statement)
			c.Assert(err, IsNil)
			c.Assert(s, Equals, targetSQLs[i][j])
		}
	}
}

func (s *testSyncerSuite) TestParseDDLSQL(c *C) {
	cases := []struct {
		sql      string
		schema   string
		ignore   bool
		isDDL    bool
		hasError bool
	}{
		{
			sql:      "FLUSH",
			schema:   "",
			ignore:   true,
			isDDL:    false,
			hasError: false,
		},
		{
			sql:      "BEGIN",
			schema:   "",
			ignore:   false,
			isDDL:    false,
			hasError: false,
		},
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
		{
			sql:      "#",
			schema:   "",
			ignore:   false,
			isDDL:    false,
			hasError: false,
		},
		{
			sql:      "# this is a comment",
			schema:   "",
			ignore:   false,
			isDDL:    false,
			hasError: false,
		},
		{
			sql:      "# a comment with DDL\nCREATE TABLE do_db.do_table (c1 INT)",
			schema:   "",
			ignore:   false,
			isDDL:    true,
			hasError: false,
		},
		{
			sql:      "# a comment with DML\nUPDATE `ignore_db`.`ignore_table` SET c1=2 WHERE c1=1",
			schema:   "ignore_db",
			ignore:   true,
			isDDL:    false,
			hasError: false,
		},
	}

	cfg := &config.SubTaskConfig{
		BWList: &filter.Rules{
			IgnoreDBs: []string{"ignore_db"},
		},
	}
	var err error
	syncer := NewSyncer(cfg, nil)
	syncer.bwList, err = filter.New(syncer.cfg.CaseSensitive, syncer.cfg.BWList)
	c.Assert(err, IsNil)

	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	parser, err := s.mockParser(db, mock)
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

func (s *testSyncerSuite) TestResolveGeneratedColumnSQL(c *C) {
	testCases := []struct {
		sql      string
		expected string
	}{
		{
			"ALTER TABLE `test`.`test` ADD COLUMN d int(11) GENERATED ALWAYS AS (c + 1) VIRTUAL",
			"ALTER TABLE `test`.`test` ADD COLUMN `d` INT(11) GENERATED ALWAYS AS(`c`+1) VIRTUAL",
		},
		{
			"ALTER TABLE `test`.`test` ADD COLUMN d int(11) AS (1 + 1) STORED",
			"ALTER TABLE `test`.`test` ADD COLUMN `d` INT(11) GENERATED ALWAYS AS(1+1) STORED",
		},
	}

	syncer := &Syncer{}
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	parser, err := s.mockParser(db, mock)
	c.Assert(err, IsNil)

	for _, tc := range testCases {
		ast1, err := parser.ParseOneStmt(tc.sql, "", "")
		c.Assert(err, IsNil)

		sqls, _, err := syncer.resolveDDLSQL(tcontext.Background(), parser, ast1, "test")
		c.Assert(err, IsNil)

		c.Assert(len(sqls), Equals, 1)
		getSQL := sqls[0]
		c.Assert(getSQL, Equals, tc.expected)
	}
}
