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
	"fmt"

	. "github.com/pingcap/check"
)

type testCase struct {
	sql      string
	wantSqls []string
	wantErr  bool
}

func (s *testSyncerSuite) TestResolveDDLSQL(c *C) {
	s.testNonDDL(c)
	s.testComments(c)

	// drop table
	s.testDropTable(c)

	s.testFailedCases(c)
	s.testCreateIndex(c)

	// alter table
	s.testAlterTableOption(c)
	s.testAlterTableAddColumn(c)
	s.testAlterTableDropColumn(c)
	s.testAlterTableDropIndex(c)
	s.testAlterTableAddConstraint(c)
	s.testAlterTableDropForeignKey(c)
	s.testAlterTableModifyColumn(c)
	s.testAlterTableChangeColumn(c)
	s.testAlterTableRenameTable(c)
	s.testAlterTableAlterColumn(c)
	s.testAlterTableDropPrimaryKey(c)
	s.testAlterTableLock(c)
	s.testAlterTableConvert(c)
}

func (s *testSyncerSuite) testNonDDL(c *C) {
	tests := []testCase{
		{"/* rds internal mark */ GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, REFERENCES, RELOAD, PROCESS, INDEX, ALTER, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER on *.* to 'username'@'%' identified by password '*ddasdsadsadsadsd' with grant option", nil, false},
	}

	s.run(c, tests)
}

func (s *testSyncerSuite) testComments(c *C) {
	tests := []testCase{
		{`-- create database foo;`, nil, false},
	}
	s.run(c, tests)
}

func (s *testSyncerSuite) testDropTable(c *C) {
	tests := []testCase{
		{"drop table `foo`.`bar`", []string{"DROP TABLE `foo`.`bar`"}, false},
		{"drop table if exists `foo`.`bar`", []string{"DROP TABLE IF EXISTS `foo`.`bar`"}, false},
	}
	s.run(c, tests)
}

func (s *testSyncerSuite) testFailedCases(c *C) {
	tests := []testCase{
		// cases parse failed and won't be supported in the near future
		// {"", nil, false},              // tidb not support fulltext index
		{"alter table bar ADD FULLTEXT INDEX `fulltext` (`name`) WITH PARSER ngram", []string{"alter table bar ADD FULLTEXT INDEX `fulltext` (`name`) WITH PARSER ngram"}, true}, // ditto
		{"alter table bar ADD SPATIAL INDEX (`g`)", []string{"alter table bar ADD SPATIAL INDEX (`g`)"}, true},                                                                   // tidb not support spatial index

		// cases parse failed and should be supported in the near future
		// {"ALTER TABLE bar ENABLE KEYS, DISABLE KEYS", []string{"ALTER TABLE `bar` ENABLE KEYS", "ALTER TABLE `bar` DISABLE KEYS"}, false},
		{"alter table bar ORDER BY id1, id2", []string{"alter table bar ORDER BY id1, id2"}, true}, // tidb not support ORDER BY.
		{"alter table bar add index (`name`), add FOREIGN KEY (product_category, product_id) REFERENCES product(category, id) ON UPDATE CASCADE ON DELETE RESTRICT", []string{"alter table bar add index (`name`), add FOREIGN KEY (product_category, product_id) REFERENCES product(category, id) ON UPDATE CASCADE ON DELETE RESTRICT"}, true}, // tidb not support ON UPDATE CASCADE ON DELETE RESTRICT
	}

	s.run(c, tests)
}

func (s *testSyncerSuite) testCreateIndex(c *C) {
	tests := []testCase{
		{"create indeX id_index ON lookup (id) USING BTREE", []string{"create indeX id_index ON lookup (id) USING BTREE"}, false},
	}
	s.run(c, tests)
}

func (s *testSyncerSuite) testAlterTableOption(c *C) {
	tests := []testCase{

		{"alter table bar add index (id), character set utf8 collate utf8_bin", []string{"ALTER TABLE `bar` ADD INDEX (`id`)", "ALTER TABLE `bar` CHARACTER SET = utf8 COLLATE = utf8_bin"}, false},
		{"alter table bar add index (id), character set utf8", []string{"ALTER TABLE `bar` ADD INDEX (`id`)", "ALTER TABLE `bar` CHARACTER SET = utf8"}, false},
		{"alter table bar add index (id), collate utf8_bin comment 'bar'", []string{"ALTER TABLE `bar` ADD INDEX (`id`)", "ALTER TABLE `bar` COLLATE = utf8_bin COMMENT 'bar'"}, false},

		{"alter table bar add index (`c1`), ENGINE = InnoDB COMMENT 'table bar' ROW_FORMAT = COMPRESSED", []string{"ALTER TABLE `bar` ADD INDEX (`c1`)", "ALTER TABLE `bar` ENGINE = InnoDB COMMENT 'table bar' ROW_FORMAT = COMPRESSED"}, false},
		{"alter table bar add index (`c1`), character set utf8 collate utf8_bin", []string{"ALTER TABLE `bar` ADD INDEX (`c1`)", "ALTER TABLE `bar` CHARACTER SET = utf8 COLLATE = utf8_bin"}, false},
		{"alter table bar add index (`c1`), auto_increment = 1", []string{"ALTER TABLE `bar` ADD INDEX (`c1`)", "ALTER TABLE `bar` AUTO_INCREMENT = 1"}, false},
		{"alter table bar add index (`c1`), COMMENT 'bar'", []string{"ALTER TABLE `bar` ADD INDEX (`c1`)", "ALTER TABLE `bar` COMMENT 'bar'"}, false},
		{"alter table bar add index (`c1`), AVG_ROW_LENGTH = 1024", []string{"ALTER TABLE `bar` ADD INDEX (`c1`)", "ALTER TABLE `bar` AVG_ROW_LENGTH = 1024"}, false},
		{"alter table bar add index (`c1`), CHECKSUM = 1", []string{"ALTER TABLE `bar` ADD INDEX (`c1`)", "ALTER TABLE `bar` CHECKSUM = 1"}, false},
		{"alter table bar add index (`c1`), COMPRESSION = 'zlib'", []string{"ALTER TABLE `bar` ADD INDEX (`c1`)", "ALTER TABLE `bar` COMPRESSION = 'zlib'"}, false}, //
		{"alter table bar add index (`c1`), CONNECTION 'mysql://username:password@hostname:port/database/tablename'", []string{"ALTER TABLE `bar` ADD INDEX (`c1`)", "ALTER TABLE `bar` CONNECTION = 'mysql://username:password@hostname:port/database/tablename'"}, false},
		{"alter table bar add index (`c1`), PASSWORD 'abc123'", []string{"ALTER TABLE `bar` ADD INDEX (`c1`)", "ALTER TABLE `bar` PASSWORD = 'abc123'"}, false},
		{"alter table bar add index (`c1`), KEY_BLOCK_SIZE = 128", []string{"ALTER TABLE `bar` ADD INDEX (`c1`)", "ALTER TABLE `bar` KEY_BLOCK_SIZE = 128"}, false},
		{"alter table bar add index (`c1`), MAX_ROWS 2", []string{"ALTER TABLE `bar` ADD INDEX (`c1`)", "ALTER TABLE `bar` MAX_ROWS = 2"}, false},
		{"alter table bar add index (`c1`), MIN_ROWS 0", []string{"ALTER TABLE `bar` ADD INDEX (`c1`)", "ALTER TABLE `bar` MIN_ROWS = 0"}, false},
		{"alter table bar add index (`c1`), DELAY_KEY_WRITE = 0", []string{"ALTER TABLE `bar` ADD INDEX (`c1`)", "ALTER TABLE `bar` DELAY_KEY_WRITE = 0"}, false},
		{"alter table bar add index (`c1`), ROW_FORMAT = COMPACT", []string{"ALTER TABLE `bar` ADD INDEX (`c1`)", "ALTER TABLE `bar` ROW_FORMAT = COMPACT"}, false},
		{"alter table bar add index (`c1`), ROW_FORMAT = DEFAULT", []string{"ALTER TABLE `bar` ADD INDEX (`c1`)", "ALTER TABLE `bar` ROW_FORMAT = DEFAULT"}, false},
		{"alter table bar add index (`c1`), ROW_FORMAT = DYNAMIC", []string{"ALTER TABLE `bar` ADD INDEX (`c1`)", "ALTER TABLE `bar` ROW_FORMAT = DYNAMIC"}, false},
		{"alter table bar add index (`c1`), ROW_FORMAT = COMPRESSED", []string{"ALTER TABLE `bar` ADD INDEX (`c1`)", "ALTER TABLE `bar` ROW_FORMAT = COMPRESSED"}, false},
		{"alter table bar add index (`c1`), ROW_FORMAT = REDUNDANT", []string{"ALTER TABLE `bar` ADD INDEX (`c1`)", "ALTER TABLE `bar` ROW_FORMAT = REDUNDANT"}, false},
		{"alter table bar add index (`c1`), ROW_FORMAT = FIXED", []string{"ALTER TABLE `bar` ADD INDEX (`c1`)", "ALTER TABLE `bar` ROW_FORMAT = FIXED"}, false},

		{"alter table bar add index (`c1`), STATS_PERSISTENT 1", []string{"ALTER TABLE `bar` ADD INDEX (`c1`)", "ALTER TABLE `bar` STATS_PERSISTENT = DEFAULT"}, false},
		{"alter table bar engine='InnoDB'", []string{"ALTER TABLE `bar` ENGINE = InnoDB"}, false},
		{"alter table bar engine=``", []string{"ALTER TABLE `bar` ENGINE = ''"}, false},
		{"alter table bar engine=''", []string{"ALTER TABLE `bar` ENGINE = ''"}, false},
		{"alter table bar change column `id` `id` int not null, ENGINE=``", []string{"ALTER TABLE `bar` CHANGE COLUMN `id` `id` int(11) NOT NULL", "ALTER TABLE `bar` ENGINE = ''"}, false},
		{"alter table bar engine='', add column id int not null", []string{"ALTER TABLE `bar` ENGINE = ''", "ALTER TABLE `bar` ADD COLUMN `id` int(11) NOT NULL"}, false},
	}

	s.run(c, tests)
}

func (s *testSyncerSuite) testAlterTableAddColumn(c *C) {
	tests := []testCase{
		{"alter table `bar` add column `id` int(11) not null default -1", []string{"ALTER TABLE `bar` ADD COLUMN `id` int(11) NOT NULL DEFAULT -1"}, false},
		{"alter table `bar` add column `id` int(11) not null default +1", []string{"ALTER TABLE `bar` ADD COLUMN `id` int(11) NOT NULL DEFAULT +1"}, false},
		{"alter table `bar` add column `id` int(11) not null default 1", []string{"ALTER TABLE `bar` ADD COLUMN `id` int(11) NOT NULL DEFAULT 1"}, false},
		{"alter table `bar` add column `id` float not null default -1.1", []string{"ALTER TABLE `bar` ADD COLUMN `id` float NOT NULL DEFAULT -1.1"}, false},
		{"alter table `bar` add column `id` float not null default +1.1", []string{"ALTER TABLE `bar` ADD COLUMN `id` float NOT NULL DEFAULT +1.1"}, false},
		{"alter table `bar` add column `id` float not null default 1.1", []string{"ALTER TABLE `bar` ADD COLUMN `id` float NOT NULL DEFAULT 1.1"}, false},

		{"alter table `bar` add column `id` int(11) unsigned zerofill not null", []string{"ALTER TABLE `bar` ADD COLUMN `id` int(11) UNSIGNED ZEROFILL NOT NULL"}, false},
		{"alter table `bar` add column `id1` int(11) not null primary key comment 'id1', add column id2 int(11) primary key, add id3 int(11) unique, add id4 int(11) unique key", []string{"ALTER TABLE `bar` ADD COLUMN `id1` int(11) NOT NULL PRIMARY KEY COMMENT 'id1'", "ALTER TABLE `bar` ADD COLUMN `id2` int(11) PRIMARY KEY", "ALTER TABLE `bar` ADD COLUMN `id3` int(11) UNIQUE KEY", "ALTER TABLE `bar` ADD COLUMN `id4` int(11) UNIQUE KEY"}, false},
		{"alter table `bar` add column `id1` int(11) not null, add column `id2` int(11) not null default 1", []string{"ALTER TABLE `bar` ADD COLUMN `id1` int(11) NOT NULL", "ALTER TABLE `bar` ADD COLUMN `id2` int(11) NOT NULL DEFAULT 1"}, false},
		{"alter table `bar` add column `id1` int(11) not null, add column `id2` decimal(14,2) not null default 0.00", []string{"ALTER TABLE `bar` ADD COLUMN `id1` int(11) NOT NULL", "ALTER TABLE `bar` ADD COLUMN `id2` decimal(14,2) NOT NULL DEFAULT 0.00"}, false},
		{"alter table `bar` add column `id1` int(11) not null, add column `id2` int(11) not null COMMENT 'this is id2'", []string{"ALTER TABLE `bar` ADD COLUMN `id1` int(11) NOT NULL", "ALTER TABLE `bar` ADD COLUMN `id2` int(11) NOT NULL COMMENT 'this is id2'"}, false},
		{"alter table `bar` add column `id2` int(11) not null first", []string{"ALTER TABLE `bar` ADD COLUMN `id2` int(11) NOT NULL FIRST"}, false},
		{"alter table `bar` add column `id1` int(11) not null, add column `id2` int(11) not null first", []string{"ALTER TABLE `bar` ADD COLUMN `id1` int(11) NOT NULL", "ALTER TABLE `bar` ADD COLUMN `id2` int(11) NOT NULL FIRST"}, false},
		{"alter table `bar` add column `id1` int(11) not null, add column `id2` int(11) not null after `id1`", []string{"ALTER TABLE `bar` ADD COLUMN `id1` int(11) NOT NULL", "ALTER TABLE `bar` ADD COLUMN `id2` int(11) NOT NULL AFTER `id1`"}, false},

		{"alter table bar add c1 timestamp not null on update current_timestamp, add index (c1)", []string{"ALTER TABLE `bar` ADD COLUMN `c1` timestamp NOT NULL ON UPDATE CURRENT_TIMESTAMP", "ALTER TABLE `bar` ADD INDEX (`c1`)"}, false},
		{"alter table bar add c1 timestamp null on update current_timestamp, add index (c1)", []string{"ALTER TABLE `bar` ADD COLUMN `c1` timestamp NULL ON UPDATE CURRENT_TIMESTAMP", "ALTER TABLE `bar` ADD INDEX (`c1`)"}, false},
		{"alter table bar add c1 timestamp on update current_timestamp, add index (c1)", []string{"ALTER TABLE `bar` ADD COLUMN `c1` timestamp ON UPDATE CURRENT_TIMESTAMP", "ALTER TABLE `bar` ADD INDEX (`c1`)"}, false},
		{"alter table bar add c1 timestamp null default null on update current_timestamp, add index (c1)", []string{"ALTER TABLE `bar` ADD COLUMN `c1` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP", "ALTER TABLE `bar` ADD INDEX (`c1`)"}, false},

		{"alter table bar add c1 timestamp null default 20150606 on update current_timestamp, add index (c1)", []string{"ALTER TABLE `bar` ADD COLUMN `c1` timestamp NULL DEFAULT 20150606 ON UPDATE CURRENT_TIMESTAMP", "ALTER TABLE `bar` ADD INDEX (`c1`)"}, false},
		{"alter table bar add c1 timestamp not null default 20150606 on update current_timestamp, add index (c1)", []string{"ALTER TABLE `bar` ADD COLUMN `c1` timestamp NOT NULL DEFAULT 20150606 ON UPDATE CURRENT_TIMESTAMP", "ALTER TABLE `bar` ADD INDEX (`c1`)"}, false},
		{"alter table bar add c1 timestamp default 20150606 on update current_timestamp, add index (c1)", []string{"ALTER TABLE `bar` ADD COLUMN `c1` timestamp DEFAULT 20150606 ON UPDATE CURRENT_TIMESTAMP", "ALTER TABLE `bar` ADD INDEX (`c1`)"}, false},
		{"alter table bar add c1 timestamp default current_timestamp on update current_timestamp, add index (c1)", []string{"ALTER TABLE `bar` ADD COLUMN `c1` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP", "ALTER TABLE `bar` ADD INDEX (`c1`)"}, false},
		{"alter table bar add c1 timestamp not null default now()  on update now(), add index (c1)", []string{"ALTER TABLE `bar` ADD COLUMN `c1` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP", "ALTER TABLE `bar` ADD INDEX (`c1`)"}, false},
		{"alter table bar add c1 varchar(10) DEFAULT '' NOT NULL, add c2 varchar(10) NOT NULL DEFAULT 'foo'", []string{"ALTER TABLE `bar` ADD COLUMN `c1` varchar(10) DEFAULT '' NOT NULL", "ALTER TABLE `bar` ADD COLUMN `c2` varchar(10) NOT NULL DEFAULT 'foo'"}, false},
		{"alter table bar add c1 int(11) not null default 100000000000000, add c2 smallint not null default '100000000000000'", []string{"ALTER TABLE `bar` ADD COLUMN `c1` int(11) NOT NULL DEFAULT 100000000000000", "ALTER TABLE `bar` ADD COLUMN `c2` smallint(6) NOT NULL DEFAULT '100000000000000'"}, false},
		{"alter table bar add c1 enum('','UNO','DUE') NOT NULL default '', add index (c1)", []string{"ALTER TABLE `bar` ADD COLUMN `c1` enum('','UNO','DUE') NOT NULL DEFAULT ''", "ALTER TABLE `bar` ADD INDEX (`c1`)"}, false},
		{"alter table od_order add column caculating_string varchar(2) CHARACTER SET utf8 COLLATE utf8_general_ci null COMMENT '计费重量体积' after delivery_status, add index (caculating_string)", []string{"ALTER TABLE `od_order` ADD COLUMN `caculating_string` varchar(2) CHARACTER SET utf8 COLLATE utf8_general_ci NULL COMMENT '计费重量体积' AFTER `delivery_status`", "ALTER TABLE `od_order` ADD INDEX (`caculating_string`)"}, false}, // https://github.com/pingcap/tidb-enterprise-tools/issues/115
		{"alter table bar add column c1 tinyblob", []string{"ALTER TABLE `bar` ADD COLUMN `c1` tinyblob"}, false},
		{"alter table bar add column c1 blob", []string{"ALTER TABLE `bar` ADD COLUMN `c1` blob"}, false},
		{"alter table bar add column c1 mediumblob", []string{"ALTER TABLE `bar` ADD COLUMN `c1` mediumblob"}, false},
		{"alter table bar add column c1 longblob", []string{"ALTER TABLE `bar` ADD COLUMN `c1` longblob"}, false},
		{"alter table bar add column `id` varchar(20) BINARY not null default ''", []string{"ALTER TABLE `bar` ADD COLUMN `id` varchar(20) BINARY NOT NULL DEFAULT ''"}, false},
		{"alter table bar add column `id` char(20) binary not null default ''", []string{"ALTER TABLE `bar` ADD COLUMN `id` char(20) BINARY NOT NULL DEFAULT ''"}, false},
		{"alter table bar add column `id` text binary", []string{"ALTER TABLE `bar` ADD COLUMN `id` text BINARY"}, false},

		{"alter table `bar` add column (`id` int(11) unsigned zerofill not null)", []string{"ALTER TABLE `bar` ADD COLUMN `id` int(11) UNSIGNED ZEROFILL NOT NULL"}, false},
		{"alter /* gh-ost */ table `foo`.`bar` add column ( id int unsigned default 0 comment '')", []string{"ALTER TABLE `foo`.`bar` ADD COLUMN `id` int(11) UNSIGNED DEFAULT 0 COMMENT ''"}, false},

		// add multi-columns in parentheses.
		{"alter table bar add column (`id` int unsigned not null, `name` char(20) not null default 'xxx')", []string{"ALTER TABLE `bar` ADD COLUMN `id` int(11) UNSIGNED NOT NULL", "ALTER TABLE `bar` ADD COLUMN `name` char(20) NOT NULL DEFAULT 'xxx'"}, false},
	}

	s.run(c, tests)
}

func (s *testSyncerSuite) testAlterTableDropColumn(c *C) {

	tests := []testCase{
		{"alter table foo.bar drop a, drop b", []string{"ALTER TABLE `foo`.`bar` DROP COLUMN `a`", "ALTER TABLE `foo`.`bar` DROP COLUMN `b`"}, false},
	}
	s.run(c, tests)

}

func (s *testSyncerSuite) testAlterTableDropIndex(c *C) {
	tests := []testCase{
		{"alter table bar drop key a, drop index b", []string{"ALTER TABLE `bar` DROP INDEX `a`", "ALTER TABLE `bar` DROP INDEX `b`"}, false},
	}
	s.run(c, tests)
}

func (s *testSyncerSuite) testAlterTableAddConstraint(c *C) {
	tests := []testCase{
		{"alter table `bar` add index (`id`)", []string{"ALTER TABLE `bar` ADD INDEX (`id`)"}, false},
		{"alter table `bar` add key (`id`)", []string{"ALTER TABLE `bar` ADD INDEX (`id`)"}, false},
		{"alter table `bar` add index `idx`(`id`, `name`), add index (`name`, `id`) comment 'second index'", []string{"ALTER TABLE `bar` ADD INDEX `idx` (`id`, `name`)", "ALTER TABLE `bar` ADD INDEX (`name`, `id`) COMMENT 'second index'"}, false}, // doubt this. mysql doesn't have ADD CONSTRAINT INDEX syntax
		{"alter table `bar` add index `idx`(`id`, `name`), add key (`name`)", []string{"ALTER TABLE `bar` ADD INDEX `idx` (`id`, `name`)", "ALTER TABLE `bar` ADD INDEX (`name`)"}, false},

		{"alter table bar ADD CONSTRAINT `pri` PRIMARY KEY (`g`), add index (`h`);", []string{"ALTER TABLE `bar` ADD CONSTRAINT `pri` PRIMARY KEY (`g`)", "ALTER TABLE `bar` ADD INDEX (`h`)"}, false},
		{"alter table bar ADD c INT unsigned NOT NULL AUTO_INCREMENT,ADD PRIMARY KEY (c);", []string{"ALTER TABLE `bar` ADD COLUMN `c` int(11) UNSIGNED NOT NULL AUTO_INCREMENT", "ALTER TABLE `bar` ADD CONSTRAINT PRIMARY KEY (`c`)"}, false},
		{"alter table bar ADD index (name), add constraint `u1` unique (`u1`), add unique key (`u2`), add unique index (`u3`);", []string{"ALTER TABLE `bar` ADD INDEX (`name`)", "ALTER TABLE `bar` ADD CONSTRAINT `u1` UNIQUE INDEX (`u1`)", "ALTER TABLE `bar` ADD CONSTRAINT UNIQUE INDEX (`u2`)", "ALTER TABLE `bar` ADD CONSTRAINT UNIQUE INDEX (`u3`)"}, false},
		{"alter table bar add index (`name`), add index `hash_index` using hash (`name1`) COMMENT 'a hash index'", []string{"ALTER TABLE `bar` ADD INDEX (`name`)", "ALTER TABLE `bar` ADD INDEX `hash_index` USING HASH (`name1`) COMMENT 'a hash index'"}, false},
		{"alter table bar add index using btree (`name`), add unique index using hash (`age`), add primary key using btree (`id`)", []string{"ALTER TABLE `bar` ADD INDEX USING BTREE (`name`)", "ALTER TABLE `bar` ADD CONSTRAINT UNIQUE INDEX USING HASH (`age`)", "ALTER TABLE `bar` ADD CONSTRAINT PRIMARY KEY USING BTREE (`id`)"}, false},
		{"alter table bar add index (`name`), add CONSTRAINT `pp` FOREIGN KEY (product_category, product_id) REFERENCES product(category, id)", []string{"ALTER TABLE `bar` ADD INDEX (`name`)", "ALTER TABLE `bar` ADD CONSTRAINT `pp` FOREIGN KEY (`product_category`, `product_id`) REFERENCES `product` (`category`, `id`)"}, false},
		// According to tidb parser/parser.y: 	index order is parsed but just ignored as MySQL did.
		{"alter tabLE bar ADD FULLTEXT INDEX `fulltext` (`name` ASC), add index (`c1`)", []string{"ALTER TABLE `bar` ADD FULLTEXT INDEX `fulltext` (`name`)", "ALTER TABLE `bar` ADD INDEX (`c1`)"}, false},
		{"alter tabLE bar ADD FULLTEXT KEY `fulltext` (`name` ASC), add index (`c1`)", []string{"ALTER TABLE `bar` ADD FULLTEXT INDEX `fulltext` (`name`)", "ALTER TABLE `bar` ADD INDEX (`c1`)"}, false},
	}
	s.run(c, tests)
}

func (s *testSyncerSuite) testAlterTableDropForeignKey(c *C) {
	tests := []testCase{
		{"alter table bar drop key a, drop FOREIGN KEY b", []string{"ALTER TABLE `bar` DROP INDEX `a`", "ALTER TABLE `bar` DROP FOREIGN KEY `b`"}, false},
	}
	s.run(c, tests)
}

func (s *testSyncerSuite) testAlterTableModifyColumn(c *C) {
	tests := []testCase{
		{"alter table bar modify a varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci", []string{"ALTER TABLE `bar` MODIFY COLUMN `a` varchar(255)"}, false},
		{"alter table bar modify a varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci, modify b char(255) first, modify c text after d", []string{"ALTER TABLE `bar` MODIFY COLUMN `a` varchar(255)", "ALTER TABLE `bar` MODIFY COLUMN `b` char(255) FIRST", "ALTER TABLE `bar` MODIFY COLUMN `c` text AFTER `d`"}, false},
		{"alter table bar modify a enum('signup','unique','sliding') CHARACTER SET utf8 COLLATE utf8_general_ci, modify sites set('mt') CHARACTER SET utf8 COLLATE utf8_general_ci first, modify c text after d", []string{"ALTER TABLE `bar` MODIFY COLUMN `a` enum('signup','unique','sliding')", "ALTER TABLE `bar` MODIFY COLUMN `sites` set('mt') FIRST", "ALTER TABLE `bar` MODIFY COLUMN `c` text AFTER `d`"}, false},
		{"alter table bar modify c1 timestamp default now() ON UPDATE now(), drop c1", []string{"ALTER TABLE `bar` MODIFY COLUMN `c1` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP", "ALTER TABLE `bar` DROP COLUMN `c1`"}, false},
		{"alter table `bar` modify column `id` int(11) unsigned zerofill not null", []string{"ALTER TABLE `bar` MODIFY COLUMN `id` int(11) UNSIGNED ZEROFILL NOT NULL"}, false},
		{"alter table `page_question` modify column `pageId` bigint not null default ''", []string{"ALTER TABLE `page_question` MODIFY COLUMN `pageId` bigint(20) NOT NULL DEFAULT ''"}, false},
		{"alter table `page_question` modify column `pageId` binary not null default ''", []string{"ALTER TABLE `page_question` MODIFY COLUMN `pageId` binary(1) NOT NULL DEFAULT ''"}, false},
		{"alter table `page_question` modify column `pageId` varbinary(20) not null default ''", []string{"ALTER TABLE `page_question` MODIFY COLUMN `pageId` varbinary(20) NOT NULL DEFAULT ''"}, false},
		{"alter table `bar` modify column `id` varchar(20) BINARY not null default ''", []string{"ALTER TABLE `bar` MODIFY COLUMN `id` varchar(20) BINARY NOT NULL DEFAULT ''"}, false},
		{"alter table `bar` modify column `id` char(20) binary not null default ''", []string{"ALTER TABLE `bar` MODIFY COLUMN `id` char(20) BINARY NOT NULL DEFAULT ''"}, false},
		{"alter table bar modify column `id` tinyblob", []string{"ALTER TABLE `bar` MODIFY COLUMN `id` tinyblob"}, false},
		{"alter table bar modify column `id` blob", []string{"ALTER TABLE `bar` MODIFY COLUMN `id` blob"}, false},
		{"alter table bar modify column `id` mediumblob", []string{"ALTER TABLE `bar` MODIFY COLUMN `id` mediumblob"}, false},
		{"alter table bar modify column `id` longblob", []string{"ALTER TABLE `bar` MODIFY COLUMN `id` longblob"}, false},
		{"alter table bar modify column `id` text binary", []string{"ALTER TABLE `bar` MODIFY COLUMN `id` text BINARY"}, false},
	}
	s.run(c, tests)
}

func (s *testSyncerSuite) testAlterTableChangeColumn(c *C) {
	tests := []testCase{
		{"alter table bar change a b text CHARACTER SET utf8 COLLATE utf8_general_ci, change c d tinytext CHARACTER SET utf8 COLLATE utf8_general_ci,change e f mediumtext CHARACTER SET utf8 COLLATE utf8_general_ci, change g h longtext CHARACTER SET utf8 COLLATE utf8_general_ci", []string{"ALTER TABLE `bar` CHANGE COLUMN `a` `b` text", "ALTER TABLE `bar` CHANGE COLUMN `c` `d` tinytext", "ALTER TABLE `bar` CHANGE COLUMN `e` `f` mediumtext", "ALTER TABLE `bar` CHANGE COLUMN `g` `h` longtext"}, false},
		{"alter table bar change a b varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci null default null, change c d char(255) CHARACTER SET utf8 COLLATE utf8_general_ci", []string{"ALTER TABLE `bar` CHANGE COLUMN `a` `b` varchar(255) NULL DEFAULT NULL", "ALTER TABLE `bar` CHANGE COLUMN `c` `d` char(255)"}, false},
		{"alter table bar change program program enum('signup','unique','sliding') CHARACTER SET utf8 COLLATE utf8_general_ci not null, change sites sites set('mt') CHARACTER SET utf8 COLLATE utf8_general_ci", []string{"ALTER TABLE `bar` CHANGE COLUMN `program` `program` enum('signup','unique','sliding') NOT NULL", "ALTER TABLE `bar` CHANGE COLUMN `sites` `sites` set('mt')"}, false},
		{"alter table bar change a b varchar(255), change c d varchar(255) first, change e f varchar(255) after g", []string{"ALTER TABLE `bar` CHANGE COLUMN `a` `b` varchar(255)", "ALTER TABLE `bar` CHANGE COLUMN `c` `d` varchar(255) FIRST", "ALTER TABLE `bar` CHANGE COLUMN `e` `f` varchar(255) AFTER `g`"}, false},
		{"alter table `bar` change column `id` `id` int(11) unsigned zerofill not null", []string{"ALTER TABLE `bar` CHANGE COLUMN `id` `id` int(11) UNSIGNED ZEROFILL NOT NULL"}, false},
		{"alter table `page_question` change column `pageId` `pageId` bigint not null default ''", []string{"ALTER TABLE `page_question` CHANGE COLUMN `pageId` `pageId` bigint(20) NOT NULL DEFAULT ''"}, false},
		{"alter table `page_question` change column `pageId` `pageId` binary not null default ''", []string{"ALTER TABLE `page_question` CHANGE COLUMN `pageId` `pageId` binary(1) NOT NULL DEFAULT ''"}, false},
		{"alter table `page_question` change column `pageId` `pageId` varbinary(20) not null default ''", []string{"ALTER TABLE `page_question` CHANGE COLUMN `pageId` `pageId` varbinary(20) NOT NULL DEFAULT ''"}, false},
		{"alter table `bar` change column `id` `id` varchar(20) BINARY not null default ''", []string{"ALTER TABLE `bar` CHANGE COLUMN `id` `id` varchar(20) BINARY NOT NULL DEFAULT ''"}, false},
		{"alter table `bar` change column `id` `id` char(20) binary not null default ''", []string{"ALTER TABLE `bar` CHANGE COLUMN `id` `id` char(20) BINARY NOT NULL DEFAULT ''"}, false},
		{"alter table bar change column `id` `id` tinyblob", []string{"ALTER TABLE `bar` CHANGE COLUMN `id` `id` tinyblob"}, false},
		{"alter table bar change column `id` `id` blob", []string{"ALTER TABLE `bar` CHANGE COLUMN `id` `id` blob"}, false},
		{"alter table bar change column `id` `id` mediumblob", []string{"ALTER TABLE `bar` CHANGE COLUMN `id` `id` mediumblob"}, false},
		{"alter table bar change column `id` `id` longblob", []string{"ALTER TABLE `bar` CHANGE COLUMN `id` `id` longblob"}, false},
		{"alter table bar change column `id` `id` text binary", []string{"ALTER TABLE `bar` CHANGE COLUMN `id` `id` text BINARY"}, false},
	}
	s.run(c, tests)
}

func (s *testSyncerSuite) testAlterTableRenameTable(c *C) {
	tests := []testCase{
		{"alter table bar add index (id), rename to bar1", []string{"ALTER TABLE `bar` ADD INDEX (`id`)", "ALTER TABLE `bar` RENAME TO `bar1`"}, false},
		{"alter table bar rename to bar1, add index (id)", []string{"ALTER TABLE `bar` RENAME TO `bar1`", "ALTER TABLE `bar1` ADD INDEX (`id`)"}, false},
		{"alter table bar rename to bar1, rename to bar2", []string{"ALTER TABLE `bar` RENAME TO `bar1`", "ALTER TABLE `bar1` RENAME TO `bar2`"}, false},
		{"alter table bar add index (id), rename as bar1, drop index id", []string{"ALTER TABLE `bar` ADD INDEX (`id`)", "ALTER TABLE `bar` RENAME TO `bar1`", "ALTER TABLE `bar1` DROP INDEX `id`"}, false},
		{"alter table foo.bar rename to foo.bar1, add index (id)", []string{"ALTER TABLE `foo`.`bar` RENAME TO `foo`.`bar1`", "ALTER TABLE `foo`.`bar1` ADD INDEX (`id`)"}, false},
		{"alter table foo.bar add index (id), rename as bar1", []string{"ALTER TABLE `foo`.`bar` ADD INDEX (`id`)", "ALTER TABLE `foo`.`bar` RENAME TO `bar1`"}, false},
		{"alter table bar1 add index (cat1), add index (cat2), rename to bar", []string{"ALTER TABLE `bar1` ADD INDEX (`cat1`)", "ALTER TABLE `bar1` ADD INDEX (`cat2`)", "ALTER TABLE `bar1` RENAME TO `bar`"}, false},
		{"rename table `t1` to `t2`, `t3` to `t4`", []string{"RENAME TABLE `t1` TO `t2`", "RENAME TABLE `t3` TO `t4`"}, false},
		{"rename table `db`.`t1` to `db`.`t2`, `db`.`t3` to `db`.`t4`", []string{"RENAME TABLE `db`.`t1` TO `db`.`t2`", "RENAME TABLE `db`.`t3` TO `db`.`t4`"}, false},
		{"alter table bar rename index idx_1 to idx_2, rename key idx_3 to idx_4", []string{"ALTER TABLE `bar` RENAME INDEX `idx_1` TO `idx_2`", "ALTER TABLE `bar` RENAME INDEX `idx_3` TO `idx_4`"}, false},
		{"alter table bar rename index `idx_1` to `idx_2`, rename key `idx_3` to `idx_4`", []string{"ALTER TABLE `bar` RENAME INDEX `idx_1` TO `idx_2`", "ALTER TABLE `bar` RENAME INDEX `idx_3` TO `idx_4`"}, false},
		{"alter table bar rename index idx_1 to idx_2", []string{"ALTER TABLE `bar` RENAME INDEX `idx_1` TO `idx_2`"}, false},
	}
	s.run(c, tests)
}

func (s *testSyncerSuite) testAlterTableAlterColumn(c *C) {
	tests := []testCase{
		{"alter table bar alter `id` set default 1, alter `name` drop default", []string{"ALTER TABLE `bar` ALTER COLUMN `id` SET DEFAULT 1", "ALTER TABLE `bar` ALTER COLUMN `name` DROP DEFAULT"}, false},
		{"alter table bar alter column `ctime` set default '2018-01-01 01:01:01'", []string{"ALTER TABLE `bar` ALTER COLUMN `ctime` SET DEFAULT '2018-01-01 01:01:01'"}, false},
	}
	s.run(c, tests)
}

func (s *testSyncerSuite) testAlterTableDropPrimaryKey(c *C) {
	tests := []testCase{
		{"alter table bar DROP PRIMARY KEY, drop a", []string{"ALTER TABLE `bar` DROP PRIMARY KEY", "ALTER TABLE `bar` DROP COLUMN `a`"}, false},
	}
	s.run(c, tests)
}

func (s *testSyncerSuite) testAlterTableLock(c *C) {
	tests := []testCase{
		{"alter table `foo`.`bar` add index `idx_t` (`create_time`), lock=none", []string{"ALTER TABLE `foo`.`bar` ADD INDEX `idx_t` (`create_time`)"}, false},
		{"alter table `foo`.`bar` add index `idx_t` (`create_time`), lock=default", []string{"ALTER TABLE `foo`.`bar` ADD INDEX `idx_t` (`create_time`)"}, false},
		{"alter table `foo`.`bar` add index `idx_t` (`create_time`), lock=shared", []string{"ALTER TABLE `foo`.`bar` ADD INDEX `idx_t` (`create_time`)"}, false},
		{"alter table `foo`.`bar` add index `idx_t` (`create_time`), lock=exclusive", []string{"ALTER TABLE `foo`.`bar` ADD INDEX `idx_t` (`create_time`)"}, false},
	}

	s.run(c, tests)
}

func (s *testSyncerSuite) testAlterTableConvert(c *C) {
	tests := []testCase{
		{"alter table `bar` CONVERT TO CHARACTER SET utf8", []string{"ALTER TABLE `bar` CHARACTER SET = utf8"}, false},
		{"alter table `bar` CONVERT TO CHARSET utf8", []string{"ALTER TABLE `bar` CHARACTER SET = utf8"}, false},
		{"alter table `bar` CONVERT TO CHARACTER SET utf8 COLLATE utf8_bin", []string{"ALTER TABLE `bar` CHARACTER SET = utf8 COLLATE = utf8_bin"}, false},
		{"alter table `bar` CONVERT TO CHARSET utf8 COLLATE utf8_bin", []string{"ALTER TABLE `bar` CHARACTER SET = utf8 COLLATE = utf8_bin"}, false},
		{"ALTER TABLE `foo`.`bar` CONVERT TO CHARACTER SET utf8 COLLATE utf8_bin", []string{"ALTER TABLE `foo`.`bar` CHARACTER SET = utf8 COLLATE = utf8_bin"}, false},
	}
	s.run(c, tests)
}

func (s *testSyncerSuite) run(c *C, tests []testCase) {
	parser, err := getParser(s.db, false)
	c.Assert(err, IsNil)

	for _, tt := range tests {
		sqls, err := resolveDDLSQL(tt.sql, parser)
		if !tt.wantErr && err != nil {
			fmt.Println(err)
		}
		if tt.wantErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
		}
		c.Assert(sqls, DeepEquals, tt.wantSqls)
	}
}
