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

	"github.com/pingcap/dm/openapi"
	"github.com/pingcap/dm/pkg/utils"
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

func (t *testOpenAPISuite) TestModelToSubTaskConfigList(c *check.C) {
	/* no shard task
	name: test
	conf-ver:v2
	shard-mode: "pessimistic"

	meta-schema: "dm_meta"
	enhance-online-schema-change: True
	on-duplication: error

	target-config:
	  host: "192.168.0.1"
	  port: 4000
	  user: "root"
	  password: "123456"

	source-config:
	  full-migrate-conf:
		export-threads：4
		import-threads: 16
		data-dir: "./exported_data"
		consistency: snapshot/none
	  incr-migrate-conf:
		repl-threads：32
		repl-batch: 200
	  source:
	  - source-name: "source-01"
	    binlog-name: ""
	    binlog-pos: 0
	    gtid: ""

	table-migrate-rule:
	  - source:
	       source-name: "source-01"
	       schema: "some_db"
	       table: "*"
	    target:
	       schema: "new_name_db"
		   table: ""
	*/

	metaSchema := "dm_meta"

	noShardTask := openapi.Task{
		EnhanceOnlineSchemaChange: true,
		MetaSchema:                &metaSchema,
		Name:                      "test",
		OnDuplication:             "error",
		ShardMode:                 nil,
		SourceConfig: openapi.TaskSourceConfig{
			FullMigrateConf: &openapi.TaskFullMigrateConf{
				DataDir:       nil,
				ExportThreads: nil,
				ImportThreads: nil,
			},
			IncrMigrateConf: &openapi.TaskIncrMigrateConf{
				ReplBatch:   nil,
				ReplThreads: nil,
			},
			SourceConf: nil,
		},
		TableMigrateRule: nil,
		TargetConfig: openapi.TaskTargetDataBase{
			Host:     "",
			Password: "",
			Port:     0,
			Security: &openapi.Security{
				SslCaContent:   nil,
				SslCertContent: nil,
				SslKeyContent:  nil,
			},
			User: "",
		},
		TaskMode: "",
	}

	println(noShardTask)
	// shard merge task with event filter
}

func (t *testOpenAPISuite) TestSubTaskConfigMapToModelTask(c *check.C) {
	// normal simple task
	// shard merge task
	// shard merge task with event filter
}
