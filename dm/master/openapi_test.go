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
	"github.com/tikv/pd/pkg/tempurl"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/openapi"
	"github.com/pingcap/dm/pkg/utils"
)

const (
	sourceSampleFile = "source.yaml"
)

var _ = check.Suite(&testOpenAPISuite{})

type testOpenAPISuite struct{}

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

func testNoShardTaskToSubTaskConfig(c *check.C) {
	/* no shard task
	name: test
	conf-ver:v2
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

	sourceCfg1, err := config.LoadFromFile(sourceSampleFile)
	c.Assert(err, check.IsNil)

	taskName := "test"
	sourceName := "mysql-replica-01"
	metaSchema := "dm_meta"

	exportThreads := 4
	importThreads := 16
	dataDir := "./exported_data"

	replBatch := 200
	replThreads := 32

	sourceSchema := "some_db"
	sourceTable := "*"
	targetSchema := "new_name_db"
	targetTable := ""

	taskSourceConf := openapi.TaskSourceConf{
		SourceName: sourceName,
	}
	tableMigrateRule := openapi.TaskTableMigrateRule{
		EventFilterName: nil,
		Source: struct {
			Schema     string "json:\"schema\""
			SourceName string "json:\"source_name\""
			Table      string "json:\"table\""
		}{
			Schema:     sourceSchema,
			SourceName: sourceName,
			Table:      sourceTable,
		},
		Target: struct {
			Schema string "json:\"schema\""
			Table  string "json:\"table\""
		}{
			Schema: targetSchema,
			Table:  targetTable,
		},
	}

	noShardTask := openapi.Task{
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

	toDBCfg := &config.DBConfig{
		Host:     noShardTask.TargetConfig.Host,
		Port:     noShardTask.TargetConfig.Port,
		User:     noShardTask.TargetConfig.User,
		Password: noShardTask.TargetConfig.Password,
	}

	sourceCfgMap := make(map[string]*config.SourceConfig)
	sourceCfgMap[sourceName] = sourceCfg1

	subTaskConfigList, err := modelToSubTaskConfigList(toDBCfg, sourceCfgMap, noShardTask)
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
	c.Assert(rule.SchemaPattern, check.Equals, sourceSchema)
	c.Assert(rule.TablePattern, check.Equals, sourceTable)
	c.Assert(rule.TargetSchema, check.Equals, targetSchema)
	c.Assert(rule.TargetTable, check.Equals, targetTable)
	// check filter
	c.Assert(subTaskConfig.FilterRules, check.HasLen, 0)
	// check balist
	c.Assert(subTaskConfig.BAList, check.NotNil)
	ba := subTaskConfig.BAList
	c.Assert(ba.DoDBs, check.HasLen, 1)
	c.Assert(ba.DoDBs[0], check.Equals, sourceSchema)
	c.Assert(ba.DoTables, check.HasLen, 1)
	c.Assert(ba.DoTables[0].Name, check.Equals, sourceTable)
	c.Assert(ba.DoTables[0].Schema, check.Equals, sourceSchema)
}

func testShardAndFilterTaskToSubTaskConfig(c *check.C) {
	/* shard and filter task
	 */
	println("todo")
}

func (t *testOpenAPISuite) TestModelToSubTaskConfigList(c *check.C) {
	testNoShardTaskToSubTaskConfig(c)
	testShardAndFilterTaskToSubTaskConfig(c)
}

func (t *testOpenAPISuite) TestSubTaskConfigMapToModelTask(c *check.C) {
	// normal simple task
	// shard merge task
	// shard merge task with event filter
}
