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

// this file implement all of the APIs of the DataMigration service.

package master

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/check"
	filter "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/tikv/pd/pkg/tempurl"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/openapi"
	"github.com/pingcap/dm/pkg/utils"
)

const (
	sourceSampleFile = "source.yaml"
)

// some data for test.
var (
	taskName    = "test"
	source1Name = "mysql-replica-01"
	source2Name = "mysql-replica-02"
	metaSchema  = "dm_meta"

	exportThreads = 4
	importThreads = 16
	dataDir       = "./exported_data"

	replBatch   = 200
	replThreads = 32

	noShardSourceSchema = "some_db"
	noShardSourceTable  = "*"
	noShardTargetSchema = "new_name_db"
	noShardTargetTable  = ""

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

var _ = check.Suite(&testOpenAPISuite{})

type testOpenAPISuite struct{}

func genNoShardTask() openapi.Task {
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
		   table: ""
	*/
	taskSourceConf := openapi.TaskSourceConf{
		SourceName: source1Name,
	}
	tableMigrateRule := openapi.TaskTableMigrateRule{
		EventFilterName: nil,
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
		OnDuplication:             openapi.TaskOnDuplicationError,
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

func genShardAndFilterTask() openapi.Task {
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
	    event-filter: ["filterA"]
	  - source:
	       source-name: "mysql-replica-02"
	       schema: "db_*"
	       table: "tbl_1"
	    target:
	       schema: "db1"
	       table: "tbl"

	event-filter:
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
	eventFilterRule := openapi.TaskEventFilterRule{
		IgnoreEvent: &ignoreEvent,
		IgnoreSql:   &ignoreSQL,
		RuleName:    shardSource1FilterName,
	}
	eventFilterNameList := []string{"filterA"}
	eventFilterList := []openapi.TaskEventFilterRule{eventFilterRule}
	tableMigrateRule1 := openapi.TaskTableMigrateRule{
		EventFilterName: &eventFilterNameList,
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
		EventFilterName: nil,
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
		OnDuplication:             openapi.TaskOnDuplicationError,
		EventFilterRule:           &eventFilterList,
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

func testNoShardTaskToSubTaskConfig(c *check.C) {
	sourceCfg1, err := config.LoadFromFile(sourceSampleFile)
	c.Assert(err, check.IsNil)
	sourceCfg1.SourceID = source1Name
	task := genNoShardTask()
	toDBCfg := &config.DBConfig{
		Host:     task.TargetConfig.Host,
		Port:     task.TargetConfig.Port,
		User:     task.TargetConfig.User,
		Password: task.TargetConfig.Password,
	}
	sourceCfgMap := make(map[string]*config.SourceConfig)
	sourceCfgMap[source1Name] = sourceCfg1
	subTaskConfigList, err := modelToSubTaskConfigList(toDBCfg, sourceCfgMap, task)
	c.Assert(err, check.IsNil)
	c.Assert(subTaskConfigList, check.HasLen, 1)
	subTaskConfig := subTaskConfigList[0]

	// check task name and mode
	c.Assert(subTaskConfig.Name, check.Equals, taskName)
	// check task meta
	c.Assert(subTaskConfig.MetaSchema, check.Equals, metaSchema)
	c.Assert(subTaskConfig.Meta, check.IsNil)
	// check shard config
	c.Assert(subTaskConfig.ShardMode, check.Equals, "")
	// check online schema change
	c.Assert(subTaskConfig.OnlineDDL, check.Equals, true)
	// check case sensitive
	c.Assert(subTaskConfig.CaseSensitive, check.Equals, sourceCfg1.CaseSensitive)
	// check from
	c.Assert(subTaskConfig.From.Host, check.Equals, sourceCfg1.From.Host)
	// check to
	c.Assert(subTaskConfig.To.Host, check.Equals, toDBCfg.Host)
	// check dumpling loader syncer config
	c.Assert(subTaskConfig.MydumperConfig.Threads, check.Equals, exportThreads)
	c.Assert(subTaskConfig.LoaderConfig.Dir, check.Equals, fmt.Sprintf("%s.%s", dataDir, taskName))
	c.Assert(subTaskConfig.LoaderConfig.PoolSize, check.Equals, importThreads)
	c.Assert(subTaskConfig.SyncerConfig.WorkerCount, check.Equals, replThreads)
	c.Assert(subTaskConfig.SyncerConfig.Batch, check.Equals, replBatch)
	// check route
	c.Assert(subTaskConfig.RouteRules, check.HasLen, 1)
	rule := subTaskConfig.RouteRules[0]
	c.Assert(rule.SchemaPattern, check.Equals, noShardSourceSchema)
	c.Assert(rule.TablePattern, check.Equals, noShardSourceTable)
	c.Assert(rule.TargetSchema, check.Equals, noShardTargetSchema)
	c.Assert(rule.TargetTable, check.Equals, noShardTargetTable)
	// check filter
	c.Assert(subTaskConfig.FilterRules, check.HasLen, 0)
	// check balist
	c.Assert(subTaskConfig.BAList, check.NotNil)
	ba := subTaskConfig.BAList
	c.Assert(ba.DoDBs, check.HasLen, 1)
	c.Assert(ba.DoDBs[0], check.Equals, noShardSourceSchema)
	c.Assert(ba.DoTables, check.HasLen, 1)
	c.Assert(ba.DoTables[0].Name, check.Equals, noShardSourceTable)
	c.Assert(ba.DoTables[0].Schema, check.Equals, noShardSourceSchema)
}

func testShardAndFilterTaskToSubTaskConfig(c *check.C) {
	sourceCfg1, err := config.LoadFromFile(sourceSampleFile)
	c.Assert(err, check.IsNil)
	sourceCfg1.SourceID = source1Name
	// sourceCfg1.From.m
	sourceCfg2, err := config.LoadFromFile(sourceSampleFile)
	c.Assert(err, check.IsNil)
	sourceCfg2.SourceID = source2Name

	task := genShardAndFilterTask()
	toDBCfg := &config.DBConfig{
		Host:     task.TargetConfig.Host,
		Port:     task.TargetConfig.Port,
		User:     task.TargetConfig.User,
		Password: task.TargetConfig.Password,
	}
	sourceCfgMap := make(map[string]*config.SourceConfig)
	sourceCfgMap[source1Name] = sourceCfg1
	sourceCfgMap[source2Name] = sourceCfg2
	subTaskConfigList, err := modelToSubTaskConfigList(toDBCfg, sourceCfgMap, task)
	c.Assert(err, check.IsNil)
	c.Assert(subTaskConfigList, check.HasLen, 2)

	// check sub task 1
	subTask1Config := subTaskConfigList[0]
	// check task name and mode
	c.Assert(subTask1Config.Name, check.Equals, taskName)
	// check task meta
	c.Assert(subTask1Config.MetaSchema, check.Equals, metaSchema)
	c.Assert(subTask1Config.Meta, check.NotNil)
	c.Assert(subTask1Config.Meta.BinLogGTID, check.Equals, shardSource1GtidSet)
	c.Assert(subTask1Config.Meta.BinLogName, check.Equals, shardSource1BinlogName)
	c.Assert(subTask1Config.Meta.BinLogPos, check.Equals, uint32(shardSource1BinlogPos))
	// check shard config
	c.Assert(subTask1Config.ShardMode, check.Equals, string(openapi.TaskShardModePessimistic))
	// check online schema change
	c.Assert(subTask1Config.OnlineDDL, check.Equals, true)
	// check case sensitive
	c.Assert(subTask1Config.CaseSensitive, check.Equals, sourceCfg1.CaseSensitive)
	// check from
	c.Assert(subTask1Config.From.Host, check.Equals, sourceCfg1.From.Host)
	// check to
	c.Assert(subTask1Config.To.Host, check.Equals, toDBCfg.Host)
	// check dumpling loader syncer config
	c.Assert(subTask1Config.MydumperConfig.Threads, check.Equals, exportThreads)
	c.Assert(subTask1Config.LoaderConfig.Dir, check.Equals, fmt.Sprintf("%s.%s", dataDir, taskName))
	c.Assert(subTask1Config.LoaderConfig.PoolSize, check.Equals, importThreads)
	c.Assert(subTask1Config.SyncerConfig.WorkerCount, check.Equals, replThreads)
	c.Assert(subTask1Config.SyncerConfig.Batch, check.Equals, replBatch)
	// check route
	c.Assert(subTask1Config.RouteRules, check.HasLen, 1)
	rule := subTask1Config.RouteRules[0]
	c.Assert(rule.SchemaPattern, check.Equals, shardSource1Schema)
	c.Assert(rule.TablePattern, check.Equals, shardSource1Table)
	c.Assert(rule.TargetSchema, check.Equals, shardTargetSchema)
	c.Assert(rule.TargetTable, check.Equals, shardTargetTable)
	// check filter
	c.Assert(subTask1Config.FilterRules, check.HasLen, 1)
	c.Assert(subTask1Config.FilterRules[0].SchemaPattern, check.Equals, shardSource1Schema)
	c.Assert(subTask1Config.FilterRules[0].TablePattern, check.Equals, shardSource1Table)
	c.Assert(subTask1Config.FilterRules[0].Action, check.Equals, filter.Ignore)
	c.Assert(subTask1Config.FilterRules[0].SQLPattern, check.HasLen, 1)
	c.Assert(subTask1Config.FilterRules[0].SQLPattern[0], check.Equals, shardSource1FilterSQL)
	c.Assert(subTask1Config.FilterRules[0].Events, check.HasLen, 1)
	c.Assert(string(subTask1Config.FilterRules[0].Events[0]), check.Equals, shardSource1FilterEvent)
	// check balist
	c.Assert(subTask1Config.BAList, check.NotNil)
	ba := subTask1Config.BAList
	c.Assert(ba.DoDBs, check.HasLen, 1)
	c.Assert(ba.DoDBs[0], check.Equals, shardSource1Schema)
	c.Assert(ba.DoTables, check.HasLen, 1)
	c.Assert(ba.DoTables[0].Name, check.Equals, shardSource1Table)
	c.Assert(ba.DoTables[0].Schema, check.Equals, shardSource1Schema)

	// check sub task 2
	subTask2Config := subTaskConfigList[1]
	// check task name and mode
	c.Assert(subTask2Config.Name, check.Equals, taskName)
	// check task meta
	c.Assert(subTask2Config.MetaSchema, check.Equals, metaSchema)
	c.Assert(subTask2Config.Meta, check.NotNil)
	c.Assert(subTask2Config.Meta.BinLogGTID, check.Equals, shardSource2GtidSet)
	c.Assert(subTask2Config.Meta.BinLogName, check.Equals, shardSource2BinlogName)
	c.Assert(subTask2Config.Meta.BinLogPos, check.Equals, uint32(shardSource2BinlogPos))
	// check shard config
	c.Assert(subTask2Config.ShardMode, check.Equals, string(openapi.TaskShardModePessimistic))
	// check online schema change
	c.Assert(subTask2Config.OnlineDDL, check.Equals, true)
	// check case sensitive
	c.Assert(subTask2Config.CaseSensitive, check.Equals, sourceCfg2.CaseSensitive)
	// check from
	c.Assert(subTask2Config.From.Host, check.Equals, sourceCfg2.From.Host)
	// check to
	c.Assert(subTask2Config.To.Host, check.Equals, toDBCfg.Host)
	// check dumpling loader syncer config
	c.Assert(subTask2Config.MydumperConfig.Threads, check.Equals, exportThreads)
	c.Assert(subTask2Config.LoaderConfig.Dir, check.Equals, fmt.Sprintf("%s.%s", dataDir, taskName))
	c.Assert(subTask2Config.LoaderConfig.PoolSize, check.Equals, importThreads)
	c.Assert(subTask2Config.SyncerConfig.WorkerCount, check.Equals, replThreads)
	c.Assert(subTask2Config.SyncerConfig.Batch, check.Equals, replBatch)
	// check route
	c.Assert(subTask2Config.RouteRules, check.HasLen, 1)
	rule = subTask2Config.RouteRules[0]
	c.Assert(rule.SchemaPattern, check.Equals, shardSource2Schema)
	c.Assert(rule.TablePattern, check.Equals, shardSource2Table)
	c.Assert(rule.TargetSchema, check.Equals, shardTargetSchema)
	c.Assert(rule.TargetTable, check.Equals, shardTargetTable)
	// check filter
	c.Assert(subTask2Config.FilterRules, check.HasLen, 0)
	// check balist
	c.Assert(subTask1Config.BAList, check.NotNil)
	ba = subTask2Config.BAList
	c.Assert(ba.DoDBs, check.HasLen, 1)
	c.Assert(ba.DoDBs[0], check.Equals, shardSource2Schema)
	c.Assert(ba.DoTables, check.HasLen, 1)
	c.Assert(ba.DoTables[0].Name, check.Equals, shardSource2Table)
	c.Assert(ba.DoTables[0].Schema, check.Equals, shardSource2Schema)
}

func (t *testOpenAPISuite) TestModelToSubTaskConfigList(c *check.C) {
	testNoShardTaskToSubTaskConfig(c)
	testShardAndFilterTaskToSubTaskConfig(c)
}

func testNoShardSubTaskConfigMapToModelTask(c *check.C) {
	sourceCfg1, err := config.LoadFromFile(sourceSampleFile)
	c.Assert(err, check.IsNil)
	sourceCfg1.SourceID = source1Name
	task := genNoShardTask()
	toDBCfg := &config.DBConfig{
		Host:     task.TargetConfig.Host,
		Port:     task.TargetConfig.Port,
		User:     task.TargetConfig.User,
		Password: task.TargetConfig.Password,
	}
	sourceCfgMap := make(map[string]*config.SourceConfig)
	sourceCfgMap[source1Name] = sourceCfg1
	subTaskConfigList, err := modelToSubTaskConfigList(toDBCfg, sourceCfgMap, task)
	c.Assert(err, check.IsNil)
	c.Assert(subTaskConfigList, check.HasLen, 1)

	// prepare sub task config
	subTaskConfigMap := make(map[string]map[string]config.SubTaskConfig)
	subTaskConfigMap[taskName] = make(map[string]config.SubTaskConfig)
	subTaskConfigMap[taskName][source1Name] = subTaskConfigList[0]

	taskList := subTaskConfigMapToModelTask(subTaskConfigMap)
	c.Assert(taskList, check.HasLen, 1)
	newTask := taskList[0]

	// check global config
	c.Assert(newTask.Name, check.Equals, task.Name)
	c.Assert(newTask.TaskMode, check.Equals, task.TaskMode)
	// check no shard task
	c.Assert(newTask.ShardMode, check.IsNil)
	c.Assert(*newTask.MetaSchema, check.Equals, *task.MetaSchema)
	c.Assert(newTask.OnDuplication, check.Equals, task.OnDuplication)
	c.Assert(newTask.EnhanceOnlineSchemaChange, check.Equals, task.EnhanceOnlineSchemaChange)
	// check event filter is nil
	c.Assert(newTask.EventFilterRule, check.Equals, task.EventFilterRule)
	// check target config
	c.Assert(newTask.TargetConfig, check.Equals, task.TargetConfig)
	// check source config
	c.Assert(newTask.SourceConfig.SourceConf[0].SourceName, check.Equals, task.SourceConfig.SourceConf[0].SourceName)
	// check table migrate rule
	c.Assert(newTask.TableMigrateRule[0].Source, check.Equals, newTask.TableMigrateRule[0].Source)
	c.Assert(newTask.TableMigrateRule[0].Target, check.Equals, newTask.TableMigrateRule[0].Target)
	c.Assert(newTask.TableMigrateRule[0].EventFilterName, check.Equals, newTask.TableMigrateRule[0].EventFilterName)
}

func testShardAndFilterSubTaskConfigMapToModelTask(c *check.C) {
	sourceCfg1, err := config.LoadFromFile(sourceSampleFile)
	c.Assert(err, check.IsNil)
	sourceCfg1.SourceID = source1Name
	// sourceCfg1.From.m
	sourceCfg2, err := config.LoadFromFile(sourceSampleFile)
	c.Assert(err, check.IsNil)
	sourceCfg2.SourceID = source2Name

	task := genShardAndFilterTask()
	toDBCfg := &config.DBConfig{
		Host:     task.TargetConfig.Host,
		Port:     task.TargetConfig.Port,
		User:     task.TargetConfig.User,
		Password: task.TargetConfig.Password,
	}
	sourceCfgMap := make(map[string]*config.SourceConfig)
	sourceCfgMap[source1Name] = sourceCfg1
	sourceCfgMap[source2Name] = sourceCfg2
	subTaskConfigList, err := modelToSubTaskConfigList(toDBCfg, sourceCfgMap, task)
	c.Assert(err, check.IsNil)
	c.Assert(subTaskConfigList, check.HasLen, 2)

	// prepare sub task config
	subTaskConfigMap := make(map[string]map[string]config.SubTaskConfig)
	subTaskConfigMap[taskName] = make(map[string]config.SubTaskConfig)
	subTaskConfigMap[taskName][source1Name] = subTaskConfigList[0]

	taskList := subTaskConfigMapToModelTask(subTaskConfigMap)
	c.Assert(taskList, check.HasLen, 1)
	newTask := taskList[0]
	// check global config
	c.Assert(newTask.Name, check.Equals, task.Name)
	c.Assert(newTask.TaskMode, check.Equals, task.TaskMode)
	// check shard task shard mode
	c.Assert(*newTask.ShardMode, check.Equals, *task.ShardMode)
	c.Assert(*newTask.MetaSchema, check.Equals, *task.MetaSchema)
	c.Assert(newTask.OnDuplication, check.Equals, task.OnDuplication)
	c.Assert(newTask.EnhanceOnlineSchemaChange, check.Equals, task.EnhanceOnlineSchemaChange)
	// check event filter is not nil
	newfilterRuleList := *newTask.EventFilterRule
	oldfilterRuleList := *task.EventFilterRule
	c.Assert(newfilterRuleList, check.HasLen, 1)
	c.Assert(oldfilterRuleList, check.HasLen, 1)
	newIgnoreEventList := *newfilterRuleList[0].IgnoreEvent
	oldIgnoreEventList := *oldfilterRuleList[0].IgnoreEvent
	c.Assert(newIgnoreEventList, check.HasLen, 1)
	c.Assert(oldIgnoreEventList, check.HasLen, 1)
	c.Assert(newIgnoreEventList[0], check.Equals, oldIgnoreEventList[0])
	newIgnoreSQLList := *newfilterRuleList[0].IgnoreSql
	oldIgnoreSQLList := *oldfilterRuleList[0].IgnoreSql
	c.Assert(newIgnoreSQLList, check.HasLen, 1)
	c.Assert(oldIgnoreSQLList, check.HasLen, 1)
	c.Assert(newIgnoreSQLList[0], check.Equals, oldIgnoreSQLList[0])
	// check target config
	c.Assert(newTask.TargetConfig, check.Equals, task.TargetConfig)
	// check source config
	c.Assert(newTask.SourceConfig.SourceConf[0].SourceName, check.Equals, task.SourceConfig.SourceConf[0].SourceName)
	// check table migrate rule
	c.Assert(newTask.TableMigrateRule[0].Source, check.Equals, newTask.TableMigrateRule[0].Source)
	c.Assert(newTask.TableMigrateRule[0].Target, check.Equals, newTask.TableMigrateRule[0].Target)
	c.Assert(newTask.TableMigrateRule[0].EventFilterName, check.Equals, newTask.TableMigrateRule[0].EventFilterName)
}

func (t *testOpenAPISuite) TestSubTaskConfigMapToModelTask(c *check.C) {
	testNoShardSubTaskConfigMapToModelTask(c)
	testShardAndFilterSubTaskConfigMapToModelTask(c)
}

func (t *testOpenAPISuite) TestredirectRequestToLeader(c *check.C) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// create a new cluster
	cfg1 := NewConfig()
	c.Assert(cfg1.Parse([]string{"-config=./dm-master.toml"}), check.IsNil)
	cfg1.Name = "dm-master-1"
	cfg1.DataDir = c.MkDir()
	cfg1.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfg1.PeerUrls = tempurl.Alloc()
	cfg1.AdvertisePeerUrls = cfg1.PeerUrls
	cfg1.InitialCluster = fmt.Sprintf("%s=%s", cfg1.Name, cfg1.AdvertisePeerUrls)
	cfg1.OpenAPIAddr = tempurl.Alloc()[len("http://"):]

	s1 := NewServer(cfg1)
	c.Assert(s1.Start(ctx), check.IsNil)
	defer s1.Close()

	// wait the first one become the leader
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return s1.election.IsLeader()
	}), check.IsTrue)

	// join to an existing cluster
	cfg2 := NewConfig()
	c.Assert(cfg2.Parse([]string{"-config=./dm-master.toml"}), check.IsNil)
	cfg2.Name = "dm-master-2"
	cfg2.DataDir = c.MkDir()
	cfg2.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfg2.PeerUrls = tempurl.Alloc()
	cfg2.AdvertisePeerUrls = cfg2.PeerUrls
	cfg2.Join = cfg1.MasterAddr // join to an existing cluster
	cfg2.OpenAPIAddr = tempurl.Alloc()[len("http://"):]

	s2 := NewServer(cfg2)
	c.Assert(s2.Start(ctx), check.IsNil)
	defer s2.Close()

	needRedirect1, openAPIAddrFromS1, err := s1.redirectRequestToLeader(ctx)
	c.Assert(err, check.IsNil)
	c.Assert(needRedirect1, check.Equals, false)
	c.Assert(openAPIAddrFromS1, check.Equals, s1.cfg.OpenAPIAddr)

	needRedirect2, openAPIAddrFromS2, err := s2.redirectRequestToLeader(ctx)
	c.Assert(err, check.IsNil)
	c.Assert(needRedirect2, check.Equals, true)
	c.Assert(openAPIAddrFromS2, check.Equals, s1.cfg.OpenAPIAddr)
}
