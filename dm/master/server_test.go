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

package master

import (
	"context"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/check"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/pbmock"
)

func TestMaster(t *testing.T) {
	check.TestingT(t)
}

type testMaster struct {
}

var _ = check.Suite(&testMaster{})

func defaultMasterServer(c *check.C) *Server {
	cfg := NewConfig()
	err := cfg.Parse([]string{"-config=./dm-master.toml"})
	c.Assert(err, check.IsNil)
	server := NewServer(cfg)

	return server
}

func (t *testMaster) TestQueryStatus(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	server := defaultMasterServer(c)

	// test query all workers
	for _, workerAddr := range server.cfg.DeployMap {
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		mockWorkerClient.EXPECT().QueryStatus(
			gomock.Any(),
			&pb.QueryStatusRequest{},
		).Return(&pb.QueryStatusResponse{Result: true}, nil)
		server.workerClients[workerAddr] = mockWorkerClient
	}
	resp, err2 := server.QueryStatus(context.Background(), &pb.QueryStatusListRequest{})
	c.Assert(err2, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)

	// query specified dm-worker[s]
	workers := make([]string, 0, len(server.cfg.DeployMap))
	for _, workerAddr := range server.cfg.DeployMap {
		workers = append(workers, workerAddr)
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		mockWorkerClient.EXPECT().QueryStatus(
			gomock.Any(),
			&pb.QueryStatusRequest{},
		).Return(&pb.QueryStatusResponse{Result: true}, nil)
		server.workerClients[workerAddr] = mockWorkerClient
	}
	resp, err2 = server.QueryStatus(context.Background(), &pb.QueryStatusListRequest{
		Workers: workers,
	})
	c.Assert(err2, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)

	// query with invalid dm-worker[s]
	resp, err2 = server.QueryStatus(context.Background(), &pb.QueryStatusListRequest{
		Workers: []string{"invalid-worker1", "invalid-worker2"},
	})
	c.Assert(err2, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)
	c.Assert(resp.Msg, check.Matches, ".*relevant worker-client not found")

	// query with invalid task name
	resp, err2 = server.QueryStatus(context.Background(), &pb.QueryStatusListRequest{
		Name: "invalid-task-name",
	})
	c.Assert(err2, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)
	c.Assert(resp.Msg, check.Matches, "task .* has no workers or not exist, can try `refresh-worker-tasks` cmd first")

	// TODO: test query with correct task name, this needs to add task first
}

func (t *testMaster) TestShowDDLLocks(c *check.C) {
	server := defaultMasterServer(c)

	resp, err := server.ShowDDLLocks(context.Background(), &pb.ShowDDLLocksRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Locks, check.HasLen, 0)

	workers := make([]string, 0, len(server.cfg.DeployMap))
	for _, workerAddr := range server.cfg.DeployMap {
		workers = append(workers, workerAddr)
	}

	// prepare ddl lock keeper, mainly use code from ddl_lock_test.go
	sqls := []string{"stmt"}
	cases := []struct {
		task   string
		schema string
		table  string
	}{
		{"testA", "test_db1", "test_tbl1"},
		{"testB", "test_db2", "test_tbl2"},
	}
	lk := NewLockKeeper()
	var wg sync.WaitGroup
	for _, tc := range cases {
		wg.Add(1)
		go func(task, schema, table string) {
			defer wg.Done()
			id, synced, remain, err := lk.TrySync(task, schema, table, workers[0], sqls, workers)
			c.Assert(err, check.IsNil)
			c.Assert(synced, check.IsFalse)
			c.Assert(remain, check.Greater, 0) // multi-goroutines TrySync concurrently, can only confirm remain > 0
			c.Assert(lk.FindLock(id), check.NotNil)
		}(tc.task, tc.schema, tc.table)
	}
	wg.Wait()
	server.lockKeeper = lk

	// test query with task name
	resp, err = server.ShowDDLLocks(context.Background(), &pb.ShowDDLLocksRequest{
		Task:    "testA",
		Workers: workers,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Locks, check.HasLen, 1)

	// test specify a mismatch worker
	resp, err = server.ShowDDLLocks(context.Background(), &pb.ShowDDLLocksRequest{
		Workers: []string{"invalid-worker"},
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Locks, check.HasLen, 0)

	// test query all ddl locks
	resp, err = server.ShowDDLLocks(context.Background(), &pb.ShowDDLLocksRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Locks, check.HasLen, 2)
}

func (t *testMaster) TestCheckTask(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	server := defaultMasterServer(c)

	// use task config from integration test `sharding`
	taskConfig := `---
name: test
task-mode: all
is-sharding: true
meta-schema: "dm_meta"
remove-meta: false
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
    black-white-list:  "instance"
    route-rules: ["sharding-route-rules-table", "sharding-route-rules-schema"]
    column-mapping-rules: ["instance-1"]
    mydumper-config-name: "global"
    loader-config-name: "global"
    syncer-config-name: "global"

  - source-id: "mysql-replica-02"
    server-id: 102
    black-white-list:  "instance"
    route-rules: ["sharding-route-rules-table", "sharding-route-rules-schema"]
    column-mapping-rules: ["instance-2"]
    mydumper-config-name: "global"
    loader-config-name: "global"
    syncer-config-name: "global"

black-white-list:
  instance:
    do-dbs: ["~^sharding[\\d]+"]
    do-tables:
    -  db-name: "~^sharding[\\d]+"
       tbl-name: "~^t[\\d]+"

routes:
  sharding-route-rules-table:
    schema-pattern: sharding*
    table-pattern: t*
    target-schema: db_target
    target-table: t_target

  sharding-route-rules-schema:
    schema-pattern: sharding*
    target-schema: db_target

column-mappings:
  instance-1:
    schema-pattern: "sharding*"
    table-pattern: "t*"
    expression: "partition id"
    source-column: "id"
    target-column: "id"
    arguments: ["1", "sharding", "t"]

  instance-2:
    schema-pattern: "sharding*"
    table-pattern: "t*"
    expression: "partition id"
    source-column: "id"
    target-column: "id"
    arguments: ["2", "sharding", "t"]

mydumpers:
  global:
    mydumper-path: "./bin/mydumper"
    threads: 4
    chunk-filesize: 64
    skip-tz-utc: true
    extra-args: "--regex '^sharding.*'"

loaders:
  global:
    pool-size: 16
    dir: "./dumped_data"

syncers:
  global:
    worker-count: 16
    batch: 100
    max-retry: 100
`

	mockFunc := func(password string, result bool) {
		// mock QueryWorkerConfig API to be used in s.allWorkerConfigs
		for idx, deploy := range server.cfg.Deploy {
			dbCfg := &config.DBConfig{
				Host:     "127.0.0.1",
				Port:     3306 + idx,
				User:     "root",
				Password: password,
			}
			rawConfig, err := dbCfg.Toml()
			c.Assert(err, check.IsNil)
			mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
			mockWorkerClient.EXPECT().QueryWorkerConfig(
				gomock.Any(),
				&pb.QueryWorkerConfigRequest{},
			).Return(&pb.QueryWorkerConfigResponse{
				Result:   result,
				SourceID: deploy.Source,
				Content:  rawConfig,
			}, nil)
			server.workerClients[deploy.Worker] = mockWorkerClient
		}
	}

	// check task successfully
	mockFunc("", true)
	resp, err := server.CheckTask(context.Background(), &pb.CheckTaskRequest{
		Task: taskConfig,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)

	// decode task with error
	resp, err = server.CheckTask(context.Background(), &pb.CheckTaskRequest{
		Task: "invalid toml config",
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)

	// simulate invalid password returned from dm-workers, so cfg.SubTaskConfigs will fail
	mockFunc("invalid-encrypt-password", true)
	resp, err = server.CheckTask(context.Background(), &pb.CheckTaskRequest{
		Task: taskConfig,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)

	// test query worker config failed
	mockFunc("", false)
	resp, err = server.CheckTask(context.Background(), &pb.CheckTaskRequest{
		Task: taskConfig,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)
}
