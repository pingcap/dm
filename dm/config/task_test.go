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

package config

import (
	"io/ioutil"
	"path"
	"sort"

	"github.com/pingcap/dm/pkg/terror"

	. "github.com/pingcap/check"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tidb-tools/pkg/filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	gmysql "github.com/siddontang/go-mysql/mysql"
)

func (t *testConfig) TestInvalidTaskConfig(c *C) {
	var errorTaskConfig1 = `---
name: test
task-mode: all
is-sharding: true
meta-schema: "dm_meta"
enable-heartbeat: true
timezone: "Asia/Shanghai"
ignore-checking-items: ["all"]

target-database:
  host: "127.0.0.1"
  port: 4000
  user: "root"
  password: ""

mysql-instances:
  - source-id: "mysql-replica-01"
    server-id: 101 
    block-allow-list:  "instance"
    route-rules: ["sharding-route-rules-table", "sharding-route-rules-schema"]
    column-mapping-rules: ["instance-1"]
    mydumper-config-name: "global"
    loader-config-name: "global"
    syncer-config-name: "global"
`
	var errorTaskConfig2 = `---
name: test
name: test1
task-mode: all
is-sharding: true
meta-schema: "dm_meta"
enable-heartbeat: true
timezone: "Asia/Shanghai"
ignore-checking-items: ["all"]

target-database:
  host: "127.0.0.1"
  port: 4000
  user: "root"
  password: ""

mysql-instances:
  - source-id: "mysql-replica-01"
    block-allow-list:  "instance"
    route-rules: ["sharding-route-rules-table", "sharding-route-rules-schema"]
    column-mapping-rules: ["instance-1"]
    mydumper-config-name: "global"
    loader-config-name: "global"
    syncer-config-name: "global"
`
	taskConfig := NewTaskConfig()
	err := taskConfig.Decode(errorTaskConfig1)
	// field server-id is not a member of TaskConfig
	c.Check(err, NotNil)
	c.Assert(err, ErrorMatches, "*line 18: field server-id not found in type config.MySQLInstance.*")

	err = taskConfig.Decode(errorTaskConfig2)
	// field name duplicate
	c.Check(err, NotNil)
	c.Assert(err, ErrorMatches, "*line 3: field name already set in type config.TaskConfig.*")

	filepath := path.Join(c.MkDir(), "test_invalid_task.yaml")
	configContent := []byte(`---
aaa: xxx
name: test
task-mode: all
is-sharding: true
meta-schema: "dm_meta"
enable-heartbeat: true
timezone: "Asia/Shanghai"
ignore-checking-items: ["all"]
`)
	err = ioutil.WriteFile(filepath, configContent, 0644)
	c.Assert(err, IsNil)
	taskConfig = NewTaskConfig()
	err = taskConfig.DecodeFile(filepath)
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "*line 2: field aaa not found in type config.TaskConfig.*")

	filepath = path.Join(c.MkDir(), "test_invalid_task.yaml")
	configContent = []byte(`---
name: test
task-mode: all
task-mode: all
is-sharding: true
meta-schema: "dm_meta"
enable-heartbeat: true
timezone: "Asia/Shanghai"
ignore-checking-items: ["all"]
`)
	err = ioutil.WriteFile(filepath, configContent, 0644)
	c.Assert(err, IsNil)
	taskConfig = NewTaskConfig()
	err = taskConfig.DecodeFile(filepath)
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, "*line 4: field task-mode already set in type config.TaskConfig.*")

	// test valid task config
	configContent = []byte(`---
name: test
task-mode: all
is-sharding: true
meta-schema: "dm_meta"
enable-heartbeat: true
heartbeat-update-interval: 1
heartbeat-report-interval: 1
timezone: "Asia/Shanghai"

target-database:
  host: "127.0.0.1"
  port: 4000
  user: "root"
  password: ""

mysql-instances:
  - source-id: "mysql-replica-01"
    block-allow-list:  "instance"
    mydumper-thread: 11
    mydumper-config-name: "global"
    loader-thread: 22
    loader-config-name: "global"
    syncer-thread: 33
    syncer-config-name: "global"

  - source-id: "mysql-replica-02"
    block-allow-list:  "instance"
    mydumper-config-name: "global"
    loader-config-name: "global"
    syncer-config-name: "global"

  - source-id: "mysql-replica-03"
    block-allow-list:  "instance"
    mydumper-thread: 44
    loader-thread: 55
    syncer-thread: 66

block-allow-list:
  instance:
    do-dbs: ["test"]

mydumpers:
  global:
    threads: 4
    chunk-filesize: 64
    skip-tz-utc: true
    extra-args: "-B test"

loaders:
  global:
    pool-size: 16
    dir: "./dumped_data"

syncers:
  global:
    worker-count: 16
    batch: 100
`)

	err = ioutil.WriteFile(filepath, configContent, 0644)
	c.Assert(err, IsNil)
	taskConfig = NewTaskConfig()
	err = taskConfig.DecodeFile(filepath)
	c.Assert(err, IsNil)
	c.Assert(taskConfig.IsSharding, IsTrue)
	c.Assert(taskConfig.ShardMode, Equals, ShardPessimistic)
	c.Assert(taskConfig.MySQLInstances[0].Mydumper.Threads, Equals, 11)
	c.Assert(taskConfig.MySQLInstances[0].Loader.PoolSize, Equals, 22)
	c.Assert(taskConfig.MySQLInstances[0].Syncer.WorkerCount, Equals, 33)
	c.Assert(taskConfig.MySQLInstances[1].Mydumper.Threads, Equals, 4)
	c.Assert(taskConfig.MySQLInstances[1].Loader.PoolSize, Equals, 16)
	c.Assert(taskConfig.MySQLInstances[1].Syncer.WorkerCount, Equals, 16)
	c.Assert(taskConfig.MySQLInstances[2].Mydumper.Threads, Equals, 44)
	c.Assert(taskConfig.MySQLInstances[2].Loader.PoolSize, Equals, 55)
	c.Assert(taskConfig.MySQLInstances[2].Syncer.WorkerCount, Equals, 66)

	configContent = []byte(`---
name: test
task-mode: all
is-sharding: true
shard-mode: "optimistic"
meta-schema: "dm_meta"
enable-heartbeat: true
heartbeat-update-interval: 1
heartbeat-report-interval: 1
timezone: "Asia/Shanghai"

target-database:
  host: "127.0.0.1"
  port: 4000
  user: "root"
  password: ""

mysql-instances:
  - source-id: "mysql-replica-01"
  - source-id: "mysql-replica-02"
  - source-id: "mysql-replica-03"

block-allow-list:
  instance:
    do-dbs: ["test"]

routes: 
  route-rule-1:
  route-rule-2:
  route-rule-3:
  route-rule-4:

filters:
  filter-rule-1:
  filter-rule-2:
  filter-rule-3:
  filter-rule-4:
`)

	err = ioutil.WriteFile(filepath, configContent, 0644)
	c.Assert(err, IsNil)
	taskConfig = NewTaskConfig()
	err = taskConfig.DecodeFile(filepath)
	c.Assert(err, IsNil)
	c.Assert(taskConfig.IsSharding, IsTrue)
	c.Assert(taskConfig.ShardMode, Equals, ShardOptimistic)
	taskConfig.MySQLInstances[0].RouteRules = []string{"route-rule-1", "route-rule-2", "route-rule-1", "route-rule-2"}
	taskConfig.MySQLInstances[1].FilterRules = []string{"filter-rule-1", "filter-rule-2", "filter-rule-3", "filter-rule-2"}
	err = taskConfig.adjust()
	c.Assert(terror.ErrConfigDuplicateCfgItem.Equal(err), IsTrue)
	c.Assert(err, ErrorMatches, `[\s\S]*mysql-instance\(0\)'s route-rules: route-rule-1, route-rule-2[\s\S]*`)
	c.Assert(err, ErrorMatches, `[\s\S]*mysql-instance\(1\)'s filter-rules: filter-rule-2[\s\S]*`)

}

func (t *testConfig) TestCheckDuplicateString(c *C) {
	a := []string{"a", "b", "c", "d"}
	dupeStrings := checkDuplicateString(a)
	c.Assert(dupeStrings, HasLen, 0)
	a = []string{"a", "a", "b", "b", "c", "c"}
	dupeStrings = checkDuplicateString(a)
	c.Assert(dupeStrings, HasLen, 3)
	sort.Strings(dupeStrings)
	c.Assert(dupeStrings, DeepEquals, []string{"a", "b", "c"})
}

func (t *testConfig) TestTaskBlockAllowList(c *C) {
	filterRules1 := &filter.Rules{
		DoDBs: []string{"s1"},
	}

	filterRules2 := &filter.Rules{
		DoDBs: []string{"s2"},
	}

	cfg := &TaskConfig{
		Name:           "test",
		TaskMode:       "full",
		TargetDB:       &DBConfig{},
		MySQLInstances: []*MySQLInstance{{SourceID: "source-1"}},
		BWList:         map[string]*filter.Rules{"source-1": filterRules1},
	}

	// BAList is nil, will set BAList = BWList
	err := cfg.adjust()
	c.Assert(err, IsNil)
	c.Assert(cfg.BAList["source-1"], Equals, filterRules1)

	// BAList is not nil, will not update it
	cfg.BAList = map[string]*filter.Rules{"source-1": filterRules2}
	err = cfg.adjust()
	c.Assert(err, IsNil)
	c.Assert(cfg.BAList["source-1"], Equals, filterRules2)
}

func (t *testConfig) TestFromSubTaskConfigs(c *C) {
	var (
		shardMode           = ShardOptimistic
		onlineDDLScheme     = "pt"
		name                = "from-sub-tasks"
		taskMode            = "incremental"
		ignoreCheckingItems = []string{VersionChecking, BinlogRowImageChecking}
		source1             = "mysql-replica-01"
		source2             = "mysql-replica-02"
		serverID1           = uint32(123)
		serverID2           = uint32(456)
		metaSchema          = "meta-sub-tasks"
		heartbeatUI         = 12
		heartbeatRI         = 21
		timezone            = "Asia/Shanghai"
		relayDir            = "/path/to/relay"
		maxAllowedPacket    = 10244201
		session             = map[string]string{
			"sql_mode": " NO_AUTO_VALUE_ON_ZERO,ANSI_QUOTES",
		}
		security = Security{
			SSLCA:         "/path/to/ca",
			SSLCert:       "/path/to/cert",
			SSLKey:        "/path/to/key",
			CertAllowedCN: []string{"allowed-cn"},
		}
		rawDBCfg = RawDBConfig{
			MaxIdleConns: 333,
			ReadTimeout:  "2m",
			WriteTimeout: "1m",
		}
		routeRule1 = router.TableRule{
			SchemaPattern: "db*",
			TablePattern:  "tbl*",
		}
		routeRule2 = router.TableRule{
			SchemaPattern: "db*",
			TablePattern:  "tbl*",
			TargetSchema:  "db",
			TargetTable:   "tbl",
		}
		filterRule1 = bf.BinlogEventRule{
			SchemaPattern: "db*",
			TablePattern:  "tbl1*",
			Events:        []bf.EventType{bf.CreateIndex, bf.AlertTable},
			Action:        bf.Do,
		}
		filterRule2 = bf.BinlogEventRule{
			SchemaPattern: "db*",
			TablePattern:  "tbl2",
			SQLPattern:    []string{"^DROP\\s+PROCEDURE", "^CREATE\\s+PROCEDURE"},
			Action:        bf.Ignore,
		}
		baList1 = filter.Rules{
			DoDBs: []string{"db1", "db2"},
			DoTables: []*filter.Table{
				{Schema: "db1", Name: "tbl1"},
				{Schema: "db2", Name: "tbl2"},
			},
		}
		baList2 = filter.Rules{
			IgnoreDBs: []string{"bd1", "bd2"},
			IgnoreTables: []*filter.Table{
				{Schema: "bd1", Name: "lbt1"},
				{Schema: "bd2", Name: "lbt2"},
			},
		}

		stCfg1 = &SubTaskConfig{
			IsSharding:              true,
			ShardMode:               shardMode,
			OnlineDDLScheme:         onlineDDLScheme,
			CaseSensitive:           true,
			Name:                    name,
			Mode:                    taskMode,
			IgnoreCheckingItems:     ignoreCheckingItems,
			SourceID:                source1,
			ServerID:                serverID1,
			Flavor:                  gmysql.MariaDBFlavor,
			MetaSchema:              metaSchema,
			HeartbeatUpdateInterval: heartbeatUI,
			HeartbeatReportInterval: heartbeatRI,
			EnableHeartbeat:         true,
			Meta: &Meta{
				BinLogName: "mysql-bin.000123",
				BinLogPos:  456,
				BinLogGTID: "1-1-12,4-4-4",
			},
			Timezone: timezone,
			RelayDir: relayDir,
			UseRelay: true,
			From: DBConfig{
				Host:             "127.0.0.1",
				Port:             3306,
				User:             "user_from_1",
				Password:         "123",
				MaxAllowedPacket: &maxAllowedPacket,
				Session:          session,
				Security:         &security,
				RawDBCfg:         &rawDBCfg,
			},
			To: DBConfig{
				Host:             "127.0.0.1",
				Port:             4000,
				User:             "user_to",
				Password:         "abc",
				MaxAllowedPacket: &maxAllowedPacket,
				Session:          session,
				Security:         &security,
				RawDBCfg:         &rawDBCfg,
			},
			RouteRules:  []*router.TableRule{&routeRule1, &routeRule2},
			FilterRules: []*bf.BinlogEventRule{&filterRule1, &filterRule2},
			BAList:      &baList1,
			MydumperConfig: MydumperConfig{
				MydumperPath:  "",
				Threads:       16,
				ChunkFilesize: "64",
				StatementSize: 1000000,
				Rows:          1024,
				Where:         "",
				SkipTzUTC:     true,
				ExtraArgs:     "--escape-backslash",
			},
			LoaderConfig: LoaderConfig{
				PoolSize: 32,
				Dir:      "./dumpped_data",
			},
			SyncerConfig: SyncerConfig{
				WorkerCount:             32,
				Batch:                   100,
				QueueSize:               512,
				CheckpointFlushInterval: 15,
				MaxRetry:                10,
				AutoFixGTID:             true,
				EnableGTID:              true,
				DisableCausality:        false,
				SafeMode:                true,
			},
			CleanDumpFile:    true,
			EnableANSIQuotes: true,
		}
	)

	stCfg2, err := stCfg1.Clone()
	c.Assert(err, IsNil)
	stCfg2.SourceID = source2
	stCfg2.ServerID = serverID2
	stCfg2.Meta = &Meta{
		BinLogName: "mysql-bin.000321",
		BinLogPos:  123,
		BinLogGTID: "1-1-21,2-2-2",
	}
	stCfg2.From = DBConfig{
		Host:             "127.0.0.1",
		Port:             3307,
		User:             "user_from_2",
		Password:         "abc",
		MaxAllowedPacket: &maxAllowedPacket,
		Session:          session,
		Security:         &security,
		RawDBCfg:         &rawDBCfg,
	}
	stCfg2.BAList = &baList2

	var cfg TaskConfig
	cfg.FromSubTaskConfigs(stCfg1, stCfg2)
	c.Log(cfg.String())

	cfg2 := TaskConfig{
		Name:                    name,
		TaskMode:                taskMode,
		IsSharding:              stCfg1.IsSharding,
		ShardMode:               shardMode,
		IgnoreCheckingItems:     ignoreCheckingItems,
		MetaSchema:              metaSchema,
		EnableHeartbeat:         stCfg1.EnableHeartbeat,
		HeartbeatUpdateInterval: heartbeatUI,
		HeartbeatReportInterval: heartbeatRI,
		Timezone:                timezone,
		CaseSensitive:           stCfg1.CaseSensitive,
		TargetDB:                &stCfg1.To,
		MySQLInstances: []*MySQLInstance{
			{
				SourceID:           source1,
				Meta:               stCfg1.Meta,
				FilterRules:        []string{"filter-01-01", "filter-01-02"},
				ColumnMappingRules: []string{},
				RouteRules:         []string{"route-01-01", "route-01-02"},
				BWListName:         "",
				BAListName:         "balist-01",
				MydumperConfigName: "dump-01",
				Mydumper:           nil,
				MydumperThread:     0,
				LoaderConfigName:   "load-01",
				Loader:             nil,
				LoaderThread:       0,
				SyncerConfigName:   "sync-01",
				Syncer:             nil,
				SyncerThread:       0,
			},
			{
				SourceID:           source2,
				Meta:               stCfg2.Meta,
				FilterRules:        []string{"filter-02-01", "filter-02-02"},
				ColumnMappingRules: []string{},
				RouteRules:         []string{"route-02-01", "route-02-02"},
				BWListName:         "",
				BAListName:         "balist-02",
				MydumperConfigName: "dump-02",
				Mydumper:           nil,
				MydumperThread:     0,
				LoaderConfigName:   "load-02",
				Loader:             nil,
				LoaderThread:       0,
				SyncerConfigName:   "sync-02",
				Syncer:             nil,
				SyncerThread:       0,
			},
		},
		OnlineDDLScheme: onlineDDLScheme,
		Routes: map[string]*router.TableRule{
			"route-01-01": stCfg1.RouteRules[0],
			"route-01-02": stCfg1.RouteRules[1],
			"route-02-01": stCfg2.RouteRules[0],
			"route-02-02": stCfg2.RouteRules[1],
		},
		Filters: map[string]*bf.BinlogEventRule{
			"filter-01-01": stCfg1.FilterRules[0],
			"filter-01-02": stCfg1.FilterRules[1],
			"filter-02-01": stCfg2.FilterRules[0],
			"filter-02-02": stCfg2.FilterRules[1],
		},
		ColumnMappings: nil,
		BWList:         nil,
		BAList: map[string]*filter.Rules{
			"balist-01": stCfg1.BAList,
			"balist-02": stCfg2.BAList,
		},
		Mydumpers: map[string]*MydumperConfig{
			"dump-01": &stCfg1.MydumperConfig,
			"dump-02": &stCfg2.MydumperConfig,
		},
		Loaders: map[string]*LoaderConfig{
			"load-01": &stCfg1.LoaderConfig,
			"load-02": &stCfg2.LoaderConfig,
		},
		Syncers: map[string]*SyncerConfig{
			"sync-01": &stCfg1.SyncerConfig,
			"sync-02": &stCfg2.SyncerConfig,
		},
		CleanDumpFile: stCfg1.CleanDumpFile,
	}

	c.Assert(cfg.String(), Equals, cfg2.String()) // some nil/(null value) compare may not equal, so use YAML format to compare.
}

func (t *testConfig) TestMetaVerify(c *C) {
	var m *Meta
	c.Assert(m.Verify(), IsNil) // nil meta is fine (for not incremental task mode)

	// none
	m = &Meta{}
	c.Assert(terror.ErrConfigMetaInvalid.Equal(m.Verify()), IsTrue)

	// only `binlog-name`.
	m = &Meta{
		BinLogName: "mysql-bin.000123",
	}
	c.Assert(m.Verify(), IsNil)

	// only `binlog-pos`.
	m = &Meta{
		BinLogPos: 456,
	}
	c.Assert(terror.ErrConfigMetaInvalid.Equal(m.Verify()), IsTrue)

	// only `binlog-gtid`.
	m = &Meta{
		BinLogGTID: "1-1-12,4-4-4",
	}
	c.Assert(m.Verify(), IsNil)

	// all
	m = &Meta{
		BinLogName: "mysql-bin.000123",
		BinLogPos:  456,
		BinLogGTID: "1-1-12,4-4-4",
	}
	c.Assert(m.Verify(), IsNil)
}
