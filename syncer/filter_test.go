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
	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tidb-tools/pkg/filter"
)

func (s *testSyncerSuite) TestSkipQueryEvent(c *C) {
	cases := []struct {
		sql           string
		expectSkipped bool
	}{
		{"SAVEPOINT `a1`", true},

		// flush
		{"flush privileges", true},
		{"flush logs", true},
		{"FLUSH TABLES WITH READ LOCK", true},

		// table maintenance
		{"OPTIMIZE TABLE foo", true},
		{"ANALYZE TABLE foo", true},
		{"REPAIR TABLE foo", true},

		// temporary table
		{"DROP /*!40005 TEMPORARY */ TABLE IF EXISTS `h2`", true},
		{"DROP TEMPORARY TABLE IF EXISTS `foo`.`bar` /* generated by server */", true},
		{"DROP TABLE foo.bar", false},
		{"DROP TABLE `TEMPORARY TABLE`", false},
		{"DROP TABLE `TEMPORARY TABLE` /* generated by server */", false},

		// trigger
		{"CREATE DEFINER=`root`@`%` TRIGGER ins_sum BEFORE INSERT ON bar FOR EACH ROW SET @sum = @sum + NEW.id", true},
		{"CREATE TRIGGER ins_sum BEFORE INSERT ON bar FOR EACH ROW SET @sum = @sum + NEW.id", true},
		{"DROP TRIGGER ins_sum", true},
		{"create table `trigger`(id int)", false},

		// procedure
		{"drop procedure if exists prepare_data", true},
		{"CREATE DEFINER=`root`@`%` PROCEDURE `simpleproc`(OUT param1 INT) BEGIN  select count(*) into param1 from shard_0001; END", true},
		{"CREATE PROCEDURE simpleproc(OUT param1 INT) BEGIN  select count(*) into param1 from shard_0001; END", true},
		{"alter procedure prepare_data comment 'i am a comment'", true},
		{"create table `procedure`(id int)", false},

		{`CREATE DEFINER=root@localhost PROCEDURE simpleproc(OUT param1 INT)
BEGIN
    SELECT COUNT(*) INTO param1 FROM t;
END`, true},

		// view
		{"CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v` AS SELECT qty, price, qty*price AS value FROM t", false},
		{"CREATE OR REPLACE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v` AS SELECT qty, price, qty*price AS value FROM t", false},
		{"ALTER ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v` AS SELECT qty, price, qty*price AS value FROM t", true},
		{"DROP VIEW v", false},
		{"CREATE TABLE `VIEW`(id int)", false},
		{"ALTER TABLE `VIEW`(id int)", false},

		// function
		{"CREATE FUNCTION metaphon RETURNS STRING SONAME 'udf_example.so'", true},
		{"CREATE AGGREGATE FUNCTION avgcost RETURNS REAL SONAME 'udf_example.so'", true},
		{"DROP FUNCTION metaphon", true},
		{"DROP FUNCTION IF EXISTS `rand_string`", true},
		{"ALTER FUNCTION metaphon COMMENT 'hh'", true},
		{"CREATE TABLE `function` (id int)", false},

		{`CREATE DEFINER=root@localhost FUNCTION rand_string(n INT) RETURNS varchar(255) CHARSET utf8
BEGIN
          DECLARE chars_str VARCHAR(100) DEFAULT 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
          DECLARE return_str VARCHAR(255) DEFAULT '';
          DECLARE i INT DEFAULT 0;
          WHILE i<n DO
              SET return_str = CONCAT(return_str,SUBSTRING(chars_str,FLOOR(1+RAND()*52),1));
              SET i = i+1;
          END WHILE;
    RETURN return_str;
END`, true},

		// tablespace
		{"CREATE TABLESPACE `ts1` ADD DATAFILE 'ts1.ibd' ENGINE=INNODB", true},
		{"ALTER TABLESPACE `ts1` DROP DATAFILE 'ts1.idb' ENGIEN=NDB", true},
		{"DROP TABLESPACE ts1", true},

		// event
		{"CREATE DEFINER=CURRENT_USER EVENT myevent ON SCHEDULE AT CURRENT_TIMESTAMP + INTERVAL 1 HOUR DO UPDATE myschema.mytable SET mycol = mycol + 1;", true},
		{"ALTER DEFINER = CURRENT_USER EVENT myevent ON SCHEDULE EVERY 12 HOUR STARTS CURRENT_TIMESTAMP + INTERVAL 4 HOUR;", true},
		{"DROP EVENT myevent;", true},

		// account management
		{"CREATE USER 't'@'%' IDENTIFIED WITH 'mysql_native_password' AS '*93E34F4B81FEC9E8271655EA87646ED01AF377CC'", true},
		{"ALTER USER 't'@'%' IDENTIFIED WITH 'mysql_native_password' AS '*1114744159A0EF13B12FC371C94877763F9512D0'", true},
		{"rename user t to 1", true},
		{"drop user t1", true},
		{"GRANT ALL PRIVILEGES ON *.* TO 't2'@'%' IDENTIFIED WITH 'mysql_native_password' AS '*12033B78389744F3F39AC4CE4CCFCAD6960D8EA0'", true},
		{"revoke reload on *.* from 't2'@'%'", true},
	}

	//filter, err := bf.NewBinlogEvent(nil)
	//c.Assert(err, IsNil)
	syncer := &Syncer{}
	for _, t := range cases {
		skipped, err := syncer.skipQuery(nil, nil, t.sql)
		c.Assert(err, IsNil)
		c.Assert(skipped, Equals, t.expectSkipped)
	}

	// system table
	skipped, err := syncer.skipQuery([]*filter.Table{{Schema: "mysql", Name: "test"}}, nil, "create table mysql.test (id int)")
	c.Assert(err, IsNil)
	c.Assert(skipped, Equals, true)

	// test binlog filter
	filterRules := []*bf.BinlogEventRule{
		{
			SchemaPattern: "*",
			TablePattern:  "",
			Events:        []bf.EventType{bf.DropTable},
			SQLPattern:    []string{"^drop\\s+table"},
			Action:        bf.Ignore,
		}, {
			SchemaPattern: "foo*",
			TablePattern:  "",
			Events:        []bf.EventType{bf.CreateTable},
			SQLPattern:    []string{"^create\\s+table"},
			Action:        bf.Do,
		}, {
			SchemaPattern: "foo*",
			TablePattern:  "bar*",
			Events:        []bf.EventType{bf.CreateTable},
			SQLPattern:    []string{"^create\\s+table"},
			Action:        bf.Ignore,
		},
	}

	syncer.binlogFilter, err = bf.NewBinlogEvent(false, filterRules)
	c.Assert(err, IsNil)

	// test global rule
	sql := "drop table tx.test"
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	c.Assert(err, IsNil)
	skipped, err = syncer.skipQuery([]*filter.Table{{Schema: "tx", Name: "test"}}, stmt, sql)
	c.Assert(err, IsNil)
	c.Assert(skipped, Equals, true)

	sql = "create table tx.test (id int)"
	stmt, err = parser.New().ParseOneStmt(sql, "", "")
	c.Assert(err, IsNil)
	skipped, err = syncer.skipQuery([]*filter.Table{{Schema: "tx", Name: "test"}}, stmt, sql)
	c.Assert(err, IsNil)
	c.Assert(skipped, Equals, false)

	// test schema rule
	sql = "create table foo.test(id int)"
	stmt, err = parser.New().ParseOneStmt(sql, "", "")
	c.Assert(err, IsNil)
	skipped, err = syncer.skipQuery([]*filter.Table{{Schema: "foo", Name: "test"}}, stmt, sql)
	c.Assert(err, IsNil)
	c.Assert(skipped, Equals, false)

	sql = "rename table foo.test to foo.test1"
	stmt, err = parser.New().ParseOneStmt(sql, "", "")
	c.Assert(err, IsNil)
	skipped, err = syncer.skipQuery([]*filter.Table{{Schema: "foo", Name: "test"}}, stmt, sql)
	c.Assert(err, IsNil)
	c.Assert(skipped, Equals, true)

	// test table rule
	sql = "create table foo.bar(id int)"
	stmt, err = parser.New().ParseOneStmt(sql, "", "")
	c.Assert(err, IsNil)
	skipped, err = syncer.skipQuery([]*filter.Table{{Schema: "foo", Name: "bar"}}, stmt, sql)
	c.Assert(err, IsNil)
	c.Assert(skipped, Equals, true)
}
