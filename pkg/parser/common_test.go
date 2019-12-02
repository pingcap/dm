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

package parser

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/tidb-tools/pkg/filter"
)

var _ = Suite(&testParserSuite{})

var sqls = []string{
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
	"alter table `t1` add column if not exists c1 int",
	"alter table `t1` add index if not exists (a) using btree comment 'a'",
	"alter table `t1` add constraint fk_t2_id foreign key if not exists (t2_id) references t2(id)",
	"create index if not exists i1 on `t1`(`c1`)",
	"alter table `t1` add partition if not exists ( partition p2 values less than maxvalue)",
	"alter table `t1` drop column if exists c2",
	"alter table `t1` change column if exists a b varchar(255)",
	"alter table `t1` modify column if exists a varchar(255)",
	"alter table `t1` drop index if exists i1",
	"alter table `t1` drop foreign key if exists fk_t2_id",
	"alter table `t1` drop partition if exists p2",
	"alter table `t1` partition by hash(a)",
	"alter table `t1` partition by key(a)",
	"alter table `t1` partition by range(a) (partition x values less than (75))",
	"alter table `t1` partition by list columns (a, b) (partition x values in ((10, 20)))",
	"alter table `t1` partition by list (a) (partition x default)",
	"alter table `t1` partition by system_time (partition x history, partition y current)",
	"alter database `test` charset utf8mb4",
}

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testParserSuite struct {
}

func (t *testParserSuite) TestParser(c *C) {
	p := parser.New()

	for _, sql := range sqls {
		stmts, err := Parse(p, sql, "", "")
		c.Assert(err, IsNil)
		c.Assert(stmts, HasLen, 1)
	}

	unsupportedSQLs := []string{
		"alter table bar ADD SPATIAL INDEX (`g`)",
	}

	for _, sql := range unsupportedSQLs {
		_, err := Parse(p, sql, "", "")
		c.Assert(err, NotNil)
	}
}

func (t *testParserSuite) TestResolveDDL(c *C) {
	p := parser.New()
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
		{"ALTER TABLE `test`.`t1` ADD COLUMN IF NOT EXISTS `c1` INT"},
		{"ALTER TABLE `test`.`t1` ADD INDEX IF NOT EXISTS(`a`) USING BTREE COMMENT 'a'"},
		{"ALTER TABLE `test`.`t1` ADD CONSTRAINT `fk_t2_id` FOREIGN KEY IF NOT EXISTS (`t2_id`) REFERENCES `t2`(`id`)"},
		{"CREATE INDEX IF NOT EXISTS `i1` ON `test`.`t1` (`c1`)"},
		{"ALTER TABLE `test`.`t1` ADD PARTITION IF NOT EXISTS (PARTITION `p2` VALUES LESS THAN (MAXVALUE))"},
		{"ALTER TABLE `test`.`t1` DROP COLUMN IF EXISTS `c2`"},
		{"ALTER TABLE `test`.`t1` CHANGE COLUMN IF EXISTS `a` `b` VARCHAR(255)"},
		{"ALTER TABLE `test`.`t1` MODIFY COLUMN IF EXISTS `a` VARCHAR(255)"},
		{"ALTER TABLE `test`.`t1` DROP INDEX IF EXISTS `i1`"},
		{"ALTER TABLE `test`.`t1` DROP FOREIGN KEY IF EXISTS `fk_t2_id`"},
		{"ALTER TABLE `test`.`t1` DROP PARTITION IF EXISTS `p2`"},
		{"ALTER TABLE `test`.`t1` PARTITION BY HASH (`a`) PARTITIONS 1"},
		{"ALTER TABLE `test`.`t1` PARTITION BY KEY (`a`) PARTITIONS 1"},
		{"ALTER TABLE `test`.`t1` PARTITION BY RANGE (`a`) (PARTITION `x` VALUES LESS THAN (75))"},
		{"ALTER TABLE `test`.`t1` PARTITION BY LIST COLUMNS (`a`,`b`) (PARTITION `x` VALUES IN ((10, 20)))"},
		{"ALTER TABLE `test`.`t1` PARTITION BY LIST (`a`) (PARTITION `x` DEFAULT)"},
		{"ALTER TABLE `test`.`t1` PARTITION BY SYSTEM_TIME (PARTITION `x` HISTORY,PARTITION `y` CURRENT)"},
		{"ALTER DATABASE `test` CHARACTER SET = utf8mb4"},
	}

	expectedTableName := [][][]*filter.Table{
		{{genTableName("s1", "")}},
		{{genTableName("s1", "")}},
		{{genTableName("s1", "")}},
		{{genTableName("s1", "")}},
		{{genTableName("s1", "t1")}},
		{{genTableName("s1", "t1")}, {genTableName("s2", "t2")}},
		{{genTableName("s1", "t1")}, {genTableName("s2", "t2")}, {genTableName("test", "xx")}},
		{{genTableName("s1", "t1")}},
		{{genTableName("test", "t1")}},
		{{genTableName("test", "t1"), genTableName("test", "t2")}},
		{{genTableName("s1", "t1"), genTableName("test", "t2")}},
		{{genTableName("test", "t1"), genTableName("xx", "t2")}},
		{{genTableName("test", "t1")}},
		{{genTableName("s1", "t1")}},
		{{genTableName("s1", "t1"), genTableName("s2", "t2")}},
		{{genTableName("test", "t1"), genTableName("test", "t2")}, {genTableName("s1", "t1"), genTableName("test", "t2")}},
		{{genTableName("s1", "t1")}},
		{{genTableName("test", "t1")}},
		{{genTableName("test", "t1")}},
		{{genTableName("s1", "t1")}},
		{{genTableName("test", "t1")}, {genTableName("test", "t1")}},
		{{genTableName("s1", "t1")}, {genTableName("s1", "t1"), genTableName("test", "t2")}, {genTableName("test", "t2")}},
		{{genTableName("s1", "t1")}, {genTableName("s1", "t1"), genTableName("xx", "t2")}, {genTableName("xx", "t2")}},
		{{genTableName("test", "t1")}},
		{{genTableName("test", "t1")}},
		{{genTableName("test", "t1")}},
		{{genTableName("test", "t1")}},
		{{genTableName("test", "t1")}},
		{{genTableName("test", "t1")}},
		{{genTableName("test", "t1")}},
		{{genTableName("test", "t1")}},
		{{genTableName("test", "t1")}},
		{{genTableName("test", "t1")}},
		{{genTableName("test", "t1")}},
		{{genTableName("test", "t1")}},
		{{genTableName("test", "t1")}},
		{{genTableName("test", "t1")}},
		{{genTableName("test", "t1")}},
		{{genTableName("test", "t1")}},
		{{genTableName("test", "t1")}},
		{{genTableName("test", "")}},
	}

	targetTableNames := [][][]*filter.Table{
		{{genTableName("xs1", "")}},
		{{genTableName("xs1", "")}},
		{{genTableName("xs1", "")}},
		{{genTableName("xs1", "")}},
		{{genTableName("xs1", "xt1")}},
		{{genTableName("xs1", "xt1")}, {genTableName("xs2", "xt2")}},
		{{genTableName("xs1", "xt1")}, {genTableName("xs2", "xt2")}, {genTableName("xtest", "xxx")}},
		{{genTableName("xs1", "xt1")}},
		{{genTableName("xtest", "xt1")}},
		{{genTableName("xtest", "xt1"), genTableName("xtest", "xt2")}},
		{{genTableName("xs1", "xt1"), genTableName("xtest", "xt2")}},
		{{genTableName("xtest", "xt1"), genTableName("xxx", "xt2")}},
		{{genTableName("xtest", "xt1")}},
		{{genTableName("xs1", "xt1")}},
		{{genTableName("xs1", "xt1"), genTableName("xs2", "xt2")}},
		{{genTableName("xtest", "xt1"), genTableName("xtest", "xt2")}, {genTableName("xs1", "xt1"), genTableName("xtest", "xt2")}},
		{{genTableName("xs1", "xt1")}},
		{{genTableName("xtest", "xt1")}},
		{{genTableName("xtest", "xt1")}},
		{{genTableName("xs1", "xt1")}},
		{{genTableName("xtest", "xt1")}, {genTableName("xtest", "xt1")}},
		{{genTableName("xs1", "xt1")}, {genTableName("xs1", "xt1"), genTableName("xtest", "xt2")}, {genTableName("xtest", "xt2")}},
		{{genTableName("xs1", "xt1")}, {genTableName("xs1", "xt1"), genTableName("xxx", "xt2")}, {genTableName("xxx", "xt2")}},
		{{genTableName("xtest", "xt1")}},
		{{genTableName("xtest", "xt1")}},
		{{genTableName("xtest", "xt1")}},
		{{genTableName("xtest", "xt1")}},
		{{genTableName("xtest", "xt1")}},
		{{genTableName("xtest", "xt1")}},
		{{genTableName("xtest", "xt1")}},
		{{genTableName("xtest", "xt1")}},
		{{genTableName("xtest", "xt1")}},
		{{genTableName("xtest", "xt1")}},
		{{genTableName("xtest", "xt1")}},
		{{genTableName("xtest", "xt1")}},
		{{genTableName("xtest", "xt1")}},
		{{genTableName("xtest", "xt1")}},
		{{genTableName("xtest", "xt1")}},
		{{genTableName("xtest", "xt1")}},
		{{genTableName("xtest", "xt1")}},
		{{genTableName("xtest", "")}},
	}

	targetSQLs := [][]string{
		{"CREATE DATABASE IF NOT EXISTS `xs1`"},
		{"CREATE DATABASE IF NOT EXISTS `xs1`"},
		{"DROP DATABASE IF EXISTS `xs1`"},
		{"DROP DATABASE IF EXISTS `xs1`"},
		{"DROP TABLE IF EXISTS `xs1`.`xt1`"},
		{"DROP TABLE IF EXISTS `xs1`.`xt1`", "DROP TABLE IF EXISTS `xs2`.`xt2`"},
		{"DROP TABLE IF EXISTS `xs1`.`xt1`", "DROP TABLE IF EXISTS `xs2`.`xt2`", "DROP TABLE IF EXISTS `xtest`.`xxx`"},
		{"CREATE TABLE IF NOT EXISTS `xs1`.`xt1` (`id` INT)"},
		{"CREATE TABLE IF NOT EXISTS `xtest`.`xt1` (`id` INT)"},
		{"CREATE TABLE IF NOT EXISTS `xtest`.`xt1` LIKE `xtest`.`xt2`"},
		{"CREATE TABLE IF NOT EXISTS `xs1`.`xt1` LIKE `xtest`.`xt2`"},
		{"CREATE TABLE IF NOT EXISTS `xtest`.`xt1` LIKE `xxx`.`xt2`"},
		{"TRUNCATE TABLE `xtest`.`xt1`"},
		{"TRUNCATE TABLE `xs1`.`xt1`"},
		{"RENAME TABLE `xs1`.`xt1` TO `xs2`.`xt2`"},
		{"RENAME TABLE `xtest`.`xt1` TO `xtest`.`xt2`", "RENAME TABLE `xs1`.`xt1` TO `xtest`.`xt2`"},
		{"DROP INDEX IF EXISTS `i1` ON `xs1`.`xt1`"},
		{"DROP INDEX IF EXISTS `i1` ON `xtest`.`xt1`"},
		{"CREATE INDEX `i1` ON `xtest`.`xt1` (`c1`)"},
		{"CREATE INDEX `i1` ON `xs1`.`xt1` (`c1`)"},
		{"ALTER TABLE `xtest`.`xt1` ADD COLUMN `c1` INT", "ALTER TABLE `xtest`.`xt1` DROP COLUMN `c2`"},
		{"ALTER TABLE `xs1`.`xt1` ADD COLUMN `c1` INT", "ALTER TABLE `xs1`.`xt1` RENAME AS `xtest`.`xt2`", "ALTER TABLE `xtest`.`xt2` DROP COLUMN `c2`"},
		{"ALTER TABLE `xs1`.`xt1` ADD COLUMN `c1` INT", "ALTER TABLE `xs1`.`xt1` RENAME AS `xxx`.`xt2`", "ALTER TABLE `xxx`.`xt2` DROP COLUMN `c2`"},
		{"ALTER TABLE `xtest`.`xt1` ADD COLUMN IF NOT EXISTS `c1` INT"},
		{"ALTER TABLE `xtest`.`xt1` ADD INDEX IF NOT EXISTS(`a`) USING BTREE COMMENT 'a'"},
		{"ALTER TABLE `xtest`.`xt1` ADD CONSTRAINT `fk_t2_id` FOREIGN KEY IF NOT EXISTS (`t2_id`) REFERENCES `t2`(`id`)"},
		{"CREATE INDEX IF NOT EXISTS `i1` ON `xtest`.`xt1` (`c1`)"},
		{"ALTER TABLE `xtest`.`xt1` ADD PARTITION IF NOT EXISTS (PARTITION `p2` VALUES LESS THAN (MAXVALUE))"},
		{"ALTER TABLE `xtest`.`xt1` DROP COLUMN IF EXISTS `c2`"},
		{"ALTER TABLE `xtest`.`xt1` CHANGE COLUMN IF EXISTS `a` `b` VARCHAR(255)"},
		{"ALTER TABLE `xtest`.`xt1` MODIFY COLUMN IF EXISTS `a` VARCHAR(255)"},
		{"ALTER TABLE `xtest`.`xt1` DROP INDEX IF EXISTS `i1`"},
		{"ALTER TABLE `xtest`.`xt1` DROP FOREIGN KEY IF EXISTS `fk_t2_id`"},
		{"ALTER TABLE `xtest`.`xt1` DROP PARTITION IF EXISTS `p2`"},
		{"ALTER TABLE `xtest`.`xt1` PARTITION BY HASH (`a`) PARTITIONS 1"},
		{"ALTER TABLE `xtest`.`xt1` PARTITION BY KEY (`a`) PARTITIONS 1"},
		{"ALTER TABLE `xtest`.`xt1` PARTITION BY RANGE (`a`) (PARTITION `x` VALUES LESS THAN (75))"},
		{"ALTER TABLE `xtest`.`xt1` PARTITION BY LIST COLUMNS (`a`,`b`) (PARTITION `x` VALUES IN ((10, 20)))"},
		{"ALTER TABLE `xtest`.`xt1` PARTITION BY LIST (`a`) (PARTITION `x` DEFAULT)"},
		{"ALTER TABLE `xtest`.`xt1` PARTITION BY SYSTEM_TIME (PARTITION `x` HISTORY,PARTITION `y` CURRENT)"},
		{"ALTER DATABASE `xtest` CHARACTER SET = utf8mb4"},
	}

	for i, sql := range sqls {
		stmts, err := Parse(p, sql, "", "")
		c.Assert(err, IsNil)
		c.Assert(stmts, HasLen, 1)

		statements, err := SplitDDL(stmts[0], "test")
		c.Assert(err, IsNil)
		c.Assert(statements, DeepEquals, expectedSQLs[i])

		tbs := expectedTableName[i]
		for j, statement := range statements {
			s, err := Parse(p, statement, "", "")
			c.Assert(err, IsNil)
			c.Assert(s, HasLen, 1)

			tableNames, err := FetchDDLTableNames("test", s[0])
			c.Assert(err, IsNil)
			c.Assert(tableNames, DeepEquals, tbs[j])

			targetSQL, err := RenameDDLTable(s[0], targetTableNames[i][j])
			c.Assert(err, IsNil)
			c.Assert(targetSQL, Equals, targetSQLs[i][j])
		}
	}

}
