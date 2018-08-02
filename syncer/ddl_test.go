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

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-enterprise-tools/pkg/filter"
	"github.com/pingcap/tidb/parser"
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
		{"test", "test"},
	}
	originTableNameDouble := []*filter.Table{
		{"test", "test"},
		{"test1", "test1"},
	}
	targetTableNameSingle := []*filter.Table{
		{"titi", "titi"},
	}
	targetTableNameDouble := []*filter.Table{
		{"titi", "titi"},
		{"titi1", "titi1"},
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
		sql, err := genDDLSQL(t[0], originTableNameSingle, targetTableNameSingle)
		c.Assert(err, IsNil)
		c.Assert(sql, Equals, t[2])
	}

	testCase = [][]string{
		{"rename table test to test1", "RENAME TABLE `test`.`test` TO `test1`.`test1`", "RENAME TABLE `titi`.`titi` TO `titi1`.`titi1`"},
		{"alter table test rename as test1", "ALTER TABLE `test`.`test` rename as `test1`.`test1`", "USE `titi`; ALTER TABLE `titi`.`titi` rename as `titi1`.`titi1`;"},
		{"create table test like test1", "create table `test`.`test` like `test1`.`test1`", "USE `titi`; create table `titi`.`titi` like `titi1`.`titi1`;"},
	}
	for _, t := range testCase {
		sql, err := genDDLSQL(t[0], originTableNameDouble, targetTableNameDouble)
		c.Assert(err, IsNil)
		c.Assert(sql, Equals, t[2])
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
	parser := parser.New()

	for _, char := range controlChars {
		buf.WriteByte(char)
		buf.WriteByte(char)
		buf.WriteString(ddl)
		buf.WriteByte(char)
		buf.WriteByte(char)

		newDDL := trimCtrlChars(buf.String())
		c.Assert(len(newDDL), Equals, len(ddl))

		_, err := parser.ParseOneStmt(newDDL, "", "")
		c.Assert(err, IsNil)
		buf.Reset()
	}
}
