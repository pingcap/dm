// Copyright 2021 PingCAP, Inc.
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

package fixtures

import (
	"github.com/pingcap/dm/openapi"
)

var (
	source1Name = "mysql-replica-01"
	source2Name = "mysql-replica-02"

	taskName   = "test"
	metaSchema = "dm_meta"

	exportThreads = 4
	importThreads = 16
	dataDir       = "./exported_data"

	replBatch   = 200
	replThreads = 32

	noShardSourceSchema = "some_db"
	noShardSourceTable  = "*"
	noShardTargetSchema = "new_name_db"
	noShardTargetTable  = "*"

	shardSource1BinlogName = "mysql-bin.001"
	shardSource1BinlogPos  = 0
	shardSource1GtidSet    = ""
	shardSource2BinlogName = "mysql-bin.002"
	shardSource2BinlogPos  = 1232
	shardSource2GtidSet    = "12e57f06-f360-11eb-8235-585cc2bc66c9:1-24"

	shardSource1Schema = "db_*"
	shardSource1Table  = "tbl_1"
	shardSource2Schema = "db_*"
	shardSource2Table  = "tbl_1"

	shardTargetSchema = "db1"
	shardTargetTable  = "tbl"

	shardSource1FilterName  = "filterA"
	shardSource1FilterEvent = "drop database"
	shardSource1FilterSQL   = "^Drop"
)

// GenNoShardOpenAPITaskForTest generates a no-shard openapi.Task for test.
func GenNoShardOpenAPITaskForTest() openapi.Task {
	/* no shard task
	name: test
	shard-mode: "pessimistic"

	meta-schema: "dm_meta"
	enhance-online-schema-change: True
	on-duplication: error

	target-config:
	  host: "127.0.0.1"
	  port: 4000
	  user: "root"
	  password: "123456"

	source-config:
	  full-migrate-conf:
		export-threads：4
		import-threads: 16
		data-dir: "./exported_data"
	  incr-migrate-conf:
		repl-threads：32
		repl-batch: 200
	  source:
	  - source-name: "mysql-replica-01"
	    binlog-name: ""
	    binlog-pos: 0
	    gtid: ""

	table-migrate-rule:
	  - source:
	       source-name: "mysql-replica-01"
	       schema: "some_db"
	       table: "*"
	    target:
	       schema: "new_name_db"
		   table: "*"
	*/
	taskSourceConf := openapi.TaskSourceConf{
		SourceName: source1Name,
	}
	tableMigrateRule := openapi.TaskTableMigrateRule{
		Source: struct {
			Schema     string "json:\"schema\""
			SourceName string "json:\"source_name\""
			Table      string "json:\"table\""
		}{
			Schema:     noShardSourceSchema,
			SourceName: source1Name,
			Table:      noShardSourceTable,
		},
		Target: struct {
			Schema string "json:\"schema\""
			Table  string "json:\"table\""
		}{
			Schema: noShardTargetSchema,
			Table:  noShardTargetTable,
		},
	}

	return openapi.Task{
		EnhanceOnlineSchemaChange: true,
		ShardMode:                 nil,
		Name:                      taskName,
		MetaSchema:                &metaSchema,
		TaskMode:                  openapi.TaskTaskModeAll,
		OnDuplicate:               openapi.TaskOnDuplicateError,
		TargetConfig: openapi.TaskTargetDataBase{
			Host:     "root",
			Password: "123456",
			Port:     4000,
			User:     "root",
		},
		SourceConfig: openapi.TaskSourceConfig{
			FullMigrateConf: &openapi.TaskFullMigrateConf{
				DataDir:       &dataDir,
				ExportThreads: &exportThreads,
				ImportThreads: &importThreads,
			},
			IncrMigrateConf: &openapi.TaskIncrMigrateConf{
				ReplBatch:   &replBatch,
				ReplThreads: &replThreads,
			},
			SourceConf: []openapi.TaskSourceConf{taskSourceConf},
		},
		TableMigrateRule: []openapi.TaskTableMigrateRule{tableMigrateRule},
	}
}

// GenShardAndFilterOpenAPITaskForTest generates a shard-and-filter openapi.Task for test.
func GenShardAndFilterOpenAPITaskForTest() openapi.Task {
	/* shard and filter task
	name: test
	meta-schema: "dm_meta"
	enhance-online-schema-change: True
	on-duplication: error

	target-config:
	  host: "127.0.0.1"
	  port: 4000
	  user: "root"
	  password: "123456"

	source-config:
	  full-migrate-conf:
		export-threads：4
		import-threads: 16
		data-dir: "./exported_data"
	  incr-migrate-conf:
		repl-threads：32
		repl-batch: 200
	  source:
	  - source-name: "mysql-replica-01"
	    binlog-name: "mysql-bin.001"
	    binlog-pos: 0
	    gtid: ""
	  - source-name: "mysql-replica-02"
	    binlog-name: "mysql-bin.002"
	    binlog-pos: 1232
	    gtid: "12e57f06-f360-11eb-8235-585cc2bc66c9:1-24"

	table-migrate-rule:
	  - source:
	       source-name: "mysql-replica-01"
	       schema: "db_*"
	       table: "tbl_1"
	    target:
	       schema: "db1"
	       table: "tbl"
	    binlog-filter-rule: ["filterA"]
	  - source:
	       source-name: "mysql-replica-02"
	       schema: "db_*"
	       table: "tbl_1"
	    target:
	       schema: "db1"
	       table: "tbl"

	binlog-filter—rule:
	   -name: "filterA"
	    ignore-event: ["drop database"]
	    ignore-sql: ["^Drop"]
	   -name: "filterB"
	    ignore-event: ["drop database"]
	    ignore-sql: ["^Create"]
	*/
	taskSource1Conf := openapi.TaskSourceConf{
		BinlogGtid: &shardSource1GtidSet,
		BinlogName: &shardSource1BinlogName,
		BinlogPos:  &shardSource1BinlogPos,
		SourceName: source1Name,
	}
	taskSource2Conf := openapi.TaskSourceConf{
		SourceName: source2Name,
		BinlogGtid: &shardSource2GtidSet,
		BinlogName: &shardSource2BinlogName,
		BinlogPos:  &shardSource2BinlogPos,
	}
	ignoreEvent := []string{shardSource1FilterEvent}
	ignoreSQL := []string{shardSource1FilterSQL}
	binlogFilterNameList := []string{shardSource1FilterName}
	eventFilterRule := openapi.TaskBinLogFilterRule{IgnoreEvent: &ignoreEvent, IgnoreSql: &ignoreSQL}
	binlogFilterRuleMap := openapi.Task_BinlogFilterRule{}
	binlogFilterRuleMap.Set(shardSource1FilterName, eventFilterRule)

	tableMigrateRule1 := openapi.TaskTableMigrateRule{
		BinlogFilterRule: &binlogFilterNameList,
		Source: struct {
			Schema     string "json:\"schema\""
			SourceName string "json:\"source_name\""
			Table      string "json:\"table\""
		}{
			Schema:     shardSource1Schema,
			SourceName: source1Name,
			Table:      shardSource1Table,
		},
		Target: struct {
			Schema string "json:\"schema\""
			Table  string "json:\"table\""
		}{
			Schema: shardTargetSchema,
			Table:  shardTargetTable,
		},
	}
	tableMigrateRule2 := openapi.TaskTableMigrateRule{
		Source: struct {
			Schema     string "json:\"schema\""
			SourceName string "json:\"source_name\""
			Table      string "json:\"table\""
		}{
			Schema:     shardSource2Schema,
			SourceName: source2Name,
			Table:      shardSource2Table,
		},
		Target: struct {
			Schema string "json:\"schema\""
			Table  string "json:\"table\""
		}{
			Schema: shardTargetSchema,
			Table:  shardTargetTable,
		},
	}
	shardMode := openapi.TaskShardModePessimistic
	return openapi.Task{
		EnhanceOnlineSchemaChange: true,
		ShardMode:                 &shardMode,
		Name:                      taskName,
		MetaSchema:                &metaSchema,
		TaskMode:                  openapi.TaskTaskModeAll,
		OnDuplicate:               openapi.TaskOnDuplicateError,
		BinlogFilterRule:          &binlogFilterRuleMap,
		TargetConfig: openapi.TaskTargetDataBase{
			Host:     "root",
			Password: "123456",
			Port:     4000,
			User:     "root",
		},
		SourceConfig: openapi.TaskSourceConfig{
			FullMigrateConf: &openapi.TaskFullMigrateConf{
				DataDir:       &dataDir,
				ExportThreads: &exportThreads,
				ImportThreads: &importThreads,
			},
			IncrMigrateConf: &openapi.TaskIncrMigrateConf{
				ReplBatch:   &replBatch,
				ReplThreads: &replThreads,
			},
			SourceConf: []openapi.TaskSourceConf{taskSource1Conf, taskSource2Conf},
		},
		TableMigrateRule: []openapi.TaskTableMigrateRule{tableMigrateRule1, tableMigrateRule2},
	}
}
