// Copyright 2017 PingCAP, Inc.
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
	"github.com/pingcap/tidb-tools/pkg/filter"

	"github.com/pingcap/tidb-enterprise-tools/dm/config"
	"github.com/pingcap/tidb-enterprise-tools/pkg/utils"
)

func (s *testSyncerSuite) TestFindTableDefineIndex(c *C) {
	testCase := [][]string{
		{"create table t (id", "(id"},
		{"create table t(id", "(id"},
		{"create table t ( id", "( id"},
		{"create table t( id", "( id"},
		{"create table t", ""},
	}

	for _, t := range testCase {
		c.Assert(findTableDefineIndex(t[0]), Equals, t[1])
	}
}

func (s *testSyncerSuite) TestFindLastWord(c *C) {
	testCase := [][]interface{}{
		{"create table t (id", 15},
		{"create table t(id", 13},
		{"create table t ( id", 17},
		{"create table t( id", 16},
		{"create table t", 13},
	}

	for _, t := range testCase {
		c.Assert(findLastWord(t[0].(string)), Equals, t[1])
	}
}

func (s *testSyncerSuite) TestGenDDLSQL(c *C) {
	originTableNameSingle := []*filter.Table{
		{Schema: "test", Name: "test"},
	}
	originTableNameDouble := []*filter.Table{
		{Schema: "test", Name: "test"},
		{Schema: "test1", Name: "test1"},
	}
	targetTableNameSingle := []*filter.Table{{Schema: "titi", Name: "titi"}}
	targetTableNameDouble := []*filter.Table{
		{Schema: "titi", Name: "titi"},
		{Schema: "titi1", Name: "titi1"},
	}
	testCase := [][]string{
		{"CREATE DATABASE test", "CREATE DATABASE test", "CREATE DATABASE `titi`"},
		{"CREATE SCHEMA test", "CREATE SCHEMA test", "CREATE SCHEMA `titi`"},
		{"CREATE DATABASE IF NOT EXISTS test", "CREATE DATABASE IF NOT EXISTS test", "CREATE DATABASE IF NOT EXISTS `titi`"},
		{"DROP DATABASE test", "DROP DATABASE test", "DROP DATABASE `titi`"},
		{"DROP SCHEMA test", "DROP SCHEMA test", "DROP SCHEMA `titi`"},
		{"DROP DATABASE IF EXISTS test", "DROP DATABASE IF EXISTS test", "DROP DATABASE IF EXISTS `titi`"},
		{"CREATE TABLE test(id int)", "CREATE TABLE `test`.`test`(id int)", "USE `titi`; CREATE TABLE `titi`.`titi`(id int);"},
		{"CREATE TABLE test (id int)", "CREATE TABLE `test`.`test` (id int)", "USE `titi`; CREATE TABLE `titi`.`titi` (id int);"},
		{"DROP TABLE test", "DROP TABLE `test`.`test`", "USE `titi`; DROP TABLE `titi`.`titi`;"},
		{"TRUNCATE TABLE test", "TRUNCATE TABLE `test`.`test`", "USE `titi`; TRUNCATE TABLE `titi`.`titi`;"},
		{"alter table test add column abc int", "ALTER TABLE `test`.`test` add column abc int", "USE `titi`; ALTER TABLE `titi`.`titi` add column abc int;"},
		{"CREATE INDEX `idx1` on test(id)", "CREATE INDEX `idx1` ON `test`.`test` (id)", "USE `titi`; CREATE INDEX `idx1` ON `titi`.`titi` (id);"},
		{"CREATE INDEX `idx1` on test (id)", "CREATE INDEX `idx1` ON `test`.`test` (id)", "USE `titi`; CREATE INDEX `idx1` ON `titi`.`titi` (id);"},
		{"DROP INDEX `idx1` on test", "DROP INDEX `idx1` ON `test`.`test`", "USE `titi`; DROP INDEX `idx1` ON `titi`.`titi`;"},
	}
	for _, t := range testCase {
		p, err := utils.GetParser(s.db, false)
		c.Assert(err, IsNil)
		stmt, err := p.ParseOneStmt(t[0], "", "")
		c.Assert(err, IsNil)
		sql, err := genDDLSQL(t[0], stmt, originTableNameSingle, targetTableNameSingle, true)
		c.Assert(err, IsNil)
		c.Assert(sql, Equals, t[2])
	}

	testCase = [][]string{
		{"rename table test to test1", "RENAME TABLE `test`.`test` TO `test1`.`test1`", "RENAME TABLE `titi`.`titi` TO `titi1`.`titi1`"},
		{"alter table test rename as test1", "ALTER TABLE `test`.`test` rename as `test1`.`test1`", "USE `titi`; ALTER TABLE `titi`.`titi` rename as `titi1`.`titi1`;"},
		{"create table test like test1", "create table `test`.`test` like `test1`.`test1`", "USE `titi`; create table `titi`.`titi` like `titi1`.`titi1`;"},
	}
	for _, t := range testCase {
		p, err := utils.GetParser(s.db, false)
		c.Assert(err, IsNil)
		stmt, err := p.ParseOneStmt(t[0], "", "")
		c.Assert(err, IsNil)
		sql, err := genDDLSQL(t[0], stmt, originTableNameDouble, targetTableNameDouble, true)
		c.Assert(err, IsNil)
		c.Assert(sql, Equals, t[2])
	}

}

func (s *testSyncerSuite) TestComment(c *C) {
	originTableNameSingle := []*filter.Table{
		{Schema: "test", Name: "test"},
	}
	originTableNameDouble := []*filter.Table{
		{Schema: "test", Name: "test"},
		{Schema: "test1", Name: "test1"},
	}
	targetTableNameSingle := []*filter.Table{
		{Schema: "titi", Name: "titi"},
	}
	targetTableNameDouble := []*filter.Table{
		{Schema: "titi", Name: "titi"},
		{Schema: "titi1", Name: "titi1"},
	}
	testCase := [][]string{
		{"CREATE /* gh-ost */ DATABASE test", "CREATE /* gh-ost */ DATABASE `titi`"},
		{"CREATE /* gh-ost */ SCHEMA test", "CREATE /* gh-ost */ SCHEMA `titi`"},
		{"CREATE /* gh-ost */ DATABASE IF NOT EXISTS test", "CREATE /* gh-ost */ DATABASE IF NOT EXISTS `titi`"},
		{"DROP /* gh-ost */ DATABASE test", "DROP /* gh-ost */ DATABASE `titi`"},
		{"DROP /* gh-ost */ SCHEMA test", "DROP /* gh-ost */ SCHEMA `titi`"},
		{"DROP /* gh-ost */ DATABASE IF EXISTS test", "DROP /* gh-ost */ DATABASE IF EXISTS `titi`"},
		{"CREATE /* gh-ost */ TABLE test(id int)", "USE `titi`; CREATE /* gh-ost */ TABLE `titi`.`titi`(id int);"},
		{"CREATE /* gh-ost */ TABLE test (id int)", "USE `titi`; CREATE /* gh-ost */ TABLE `titi`.`titi` (id int);"},
		{"DROP /* gh-ost */ TABLE test", "USE `titi`; DROP /* gh-ost */ TABLE `titi`.`titi`;"},
		{"TRUNCATE TABLE test", "USE `titi`; TRUNCATE TABLE `titi`.`titi`;"},
		{"alter /* gh-ost */ table test add column abc int", "USE `titi`; ALTER TABLE `titi`.`titi` add column abc int;"},
		{"CREATE /* gh-ost*/ INDEX `idx1` on test(id)", "USE `titi`; CREATE /* gh-ost*/ INDEX `idx1` ON `titi`.`titi` (id);"},
		{"CREATE /*gh-ost */ INDEX `idx1` on test (id)", "USE `titi`; CREATE /*gh-ost */ INDEX `idx1` ON `titi`.`titi` (id);"},
		{"DROP /*gh-ost*/ INDEX `idx1` on test", "USE `titi`; DROP /*gh-ost*/ INDEX `idx1` ON `titi`.`titi`;"},
	}

	parser, err := utils.GetParser(s.db, false)
	c.Assert(err, IsNil)

	for _, t := range testCase {
		stmt, err := parser.ParseOneStmt(t[0], "", "")
		c.Assert(err, IsNil)
		sql, err := genDDLSQL(t[0], stmt, originTableNameSingle, targetTableNameSingle, true)
		c.Assert(err, IsNil)
		c.Assert(sql, Equals, t[1])
	}

	testCase = [][]string{
		{"rename table test to test1", "RENAME TABLE `titi`.`titi` TO `titi1`.`titi1`"},
		{"alter /* gh-ost */ table test rename as test1", "USE `titi`; ALTER TABLE `titi`.`titi` rename as `titi1`.`titi1`;"},
		{"create /* gh-ost */ table test like test1", "USE `titi`; create /* gh-ost */ table `titi`.`titi` like `titi1`.`titi1`;"},
	}
	for _, t := range testCase {
		stmt, err := parser.ParseOneStmt(t[0], "", "")
		c.Assert(err, IsNil)
		sql, err := genDDLSQL(t[0], stmt, originTableNameDouble, targetTableNameDouble, true)
		c.Assert(err, IsNil)
		c.Assert(sql, Equals, t[1])
	}
}

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

	_, err = parser.Parse(sql, "", "")
	c.Assert(err, IsNil)
}

func (s *testSyncerSuite) TestCommentQuote(c *C) {
	sql := "ALTER TABLE schemadb.ep_edu_course_message_auto_reply MODIFY answer JSON COMMENT '回复的内容-格式为list，有两个字段：\"answerType\"：//''发送客服消息类型：1-文本消息，2-图片，3-图文链接''；  answer：回复内容';"
	expectedSQL := "ALTER TABLE `schemadb`.`ep_edu_course_message_auto_reply` MODIFY COLUMN `answer` json COMMENT '回复的内容-格式为list，有两个字段：\"answerType\"：//''发送客服消息类型：1-文本消息，2-图片，3-图文链接''；  answer：回复内容'"

	parser, err := utils.GetParser(s.db, false)
	c.Assert(err, IsNil)

	_, err = parser.ParseOneStmt(sql, "", "")
	c.Assert(err, IsNil)

	syncer := &Syncer{}
	sqls, _, _, err := syncer.resolveDDLSQL(sql, parser, "")
	c.Assert(err, IsNil)
	c.Assert(len(sqls), Equals, 1)

	getSQL := sqls[0]
	_, err = parser.ParseOneStmt(getSQL, "", "")
	c.Assert(err, IsNil)
	c.Assert(getSQL, Equals, expectedSQL)
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
