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
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/pbmock"
)

// use task config from integration test `sharding`
var taskConfig = `---
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

var (
	errGRPCFailed = "test grpc request failed"
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

func mockWorkerConfig(c *check.C, server *Server, ctrl *gomock.Controller, password string, result bool) {
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
	resp, err := server.QueryStatus(context.Background(), &pb.QueryStatusListRequest{})
	c.Assert(err, check.IsNil)
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
	resp, err = server.QueryStatus(context.Background(), &pb.QueryStatusListRequest{
		Workers: workers,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)

	// query with invalid dm-worker[s]
	resp, err = server.QueryStatus(context.Background(), &pb.QueryStatusListRequest{
		Workers: []string{"invalid-worker1", "invalid-worker2"},
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)
	c.Assert(resp.Msg, check.Matches, ".*relevant worker-client not found")

	// query with invalid task name
	resp, err = server.QueryStatus(context.Background(), &pb.QueryStatusListRequest{
		Name: "invalid-task-name",
	})
	c.Assert(err, check.IsNil)
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

	// check task successfully
	mockWorkerConfig(c, server, ctrl, "", true)
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
	mockWorkerConfig(c, server, ctrl, "invalid-encrypt-password", true)
	resp, err = server.CheckTask(context.Background(), &pb.CheckTaskRequest{
		Task: taskConfig,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)

	// test query worker config failed
	mockWorkerConfig(c, server, ctrl, "", false)
	resp, err = server.CheckTask(context.Background(), &pb.CheckTaskRequest{
		Task: taskConfig,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)
}

func (t *testMaster) TestStartTask(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	server := defaultMasterServer(c)

	// s.generateSubTask with error
	resp, err := server.StartTask(context.Background(), &pb.StartTaskRequest{
		Task: "invalid toml config",
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)

	workers := make([]string, 0, len(server.cfg.DeployMap))
	for _, deploy := range server.cfg.Deploy {
		workers = append(workers, deploy.Worker)
	}

	// generate subtask configs, need to mock query worker configs RPC
	mockWorkerConfig(c, server, ctrl, "", true)
	workerCfg := make(map[string]*config.SubTaskConfig)
	_, stCfgs, err := server.generateSubTask(context.Background(), taskConfig)
	c.Assert(err, check.IsNil)
	for _, stCfg := range stCfgs {
		worker, ok := server.cfg.DeployMap[stCfg.SourceID]
		c.Assert(ok, check.IsTrue)
		workerCfg[worker] = stCfg
	}

	mockStartTask := func(rpcSuccess bool) {
		for idx, deploy := range server.cfg.Deploy {
			mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)

			dbCfg := &config.DBConfig{
				Host:     "127.0.0.1",
				Port:     3306 + idx,
				User:     "root",
				Password: "",
			}
			rawConfig, err := dbCfg.Toml()
			c.Assert(err, check.IsNil)

			// mock query worker config
			mockWorkerClient.EXPECT().QueryWorkerConfig(
				gomock.Any(),
				&pb.QueryWorkerConfigRequest{},
			).Return(&pb.QueryWorkerConfigResponse{
				Result:   true,
				SourceID: deploy.Source,
				Content:  rawConfig,
			}, nil)

			stCfg, ok := workerCfg[deploy.Worker]
			c.Assert(ok, check.IsTrue)
			stCfgToml, err := stCfg.Toml()
			c.Assert(err, check.IsNil)

			// mock start sub task
			rets := make([]interface{}, 0, 2)
			if rpcSuccess {
				rets = []interface{}{
					&pb.CommonWorkerResponse{
						Result: true,
						Worker: deploy.Worker,
					},
					nil,
				}
			} else {
				rets = []interface{}{
					nil,
					errors.New(errGRPCFailed),
				}
			}
			mockWorkerClient.EXPECT().StartSubTask(
				gomock.Any(),
				&pb.StartSubTaskRequest{Task: stCfgToml},
			).Return(rets...)

			server.workerClients[deploy.Worker] = mockWorkerClient
		}
	}

	// test start task successfully
	mockStartTask(true)
	resp, err = server.StartTask(context.Background(), &pb.StartTaskRequest{
		Task:    taskConfig,
		Workers: workers,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)

	// check start-task with an invalid worker
	invalidWorker := "invalid-worker"
	mockWorkerConfig(c, server, ctrl, "", true)
	resp, err = server.StartTask(context.Background(), &pb.StartTaskRequest{
		Task:    taskConfig,
		Workers: []string{invalidWorker},
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 1)
	c.Assert(resp.Workers[0].Result, check.IsFalse)
	c.Assert(resp.Workers[0].Worker, check.Equals, invalidWorker)

	// test start sub task request to worker returns error
	mockStartTask(false)
	resp, err = server.StartTask(context.Background(), &pb.StartTaskRequest{
		Task: taskConfig,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 2)
	for _, workerResp := range resp.Workers {
		c.Assert(workerResp.Result, check.IsFalse)
		lines := strings.Split(workerResp.Msg, "\n")
		c.Assert(len(lines), check.Greater, 1)
		c.Assert(lines[0], check.Equals, errGRPCFailed)
	}
}

func (t *testMaster) TestQueryError(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()
	server := defaultMasterServer(c)

	// test query all workers
	for _, workerAddr := range server.cfg.DeployMap {
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		mockWorkerClient.EXPECT().QueryError(
			gomock.Any(),
			&pb.QueryErrorRequest{},
		).Return(&pb.QueryErrorResponse{Result: true}, nil)
		server.workerClients[workerAddr] = mockWorkerClient
	}
	resp, err := server.QueryError(context.Background(), &pb.QueryErrorListRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)

	// query specified dm-worker[s]
	workers := make([]string, 0, len(server.cfg.DeployMap))
	for _, workerAddr := range server.cfg.DeployMap {
		workers = append(workers, workerAddr)
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		mockWorkerClient.EXPECT().QueryError(
			gomock.Any(),
			&pb.QueryErrorRequest{},
		).Return(&pb.QueryErrorResponse{Result: true}, nil)
		server.workerClients[workerAddr] = mockWorkerClient
	}
	resp, err = server.QueryError(context.Background(), &pb.QueryErrorListRequest{
		Workers: workers,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)

	// query with invalid dm-worker[s]
	resp, err = server.QueryError(context.Background(), &pb.QueryErrorListRequest{
		Workers: []string{"invalid-worker1", "invalid-worker2"},
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)
	c.Assert(resp.Msg, check.Matches, ".*relevant worker-client not found")

	// query with invalid task name
	resp, err = server.QueryError(context.Background(), &pb.QueryErrorListRequest{
		Name: "invalid-task-name",
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)
	c.Assert(resp.Msg, check.Matches, "task .* has no workers or not exist, can try `refresh-worker-tasks` cmd first")

	// TODO: test query with correct task name, this needs to add task first
}

func (t *testMaster) TestOperateTask(c *check.C) {
	var (
		taskName = "unit-test-task"
		workers  = make([]string, 0, 2)
	)

	ctrl := gomock.NewController(c)
	defer ctrl.Finish()
	server := defaultMasterServer(c)
	for _, workerAddr := range server.cfg.DeployMap {
		workers = append(workers, workerAddr)
	}

	// test operate-task with invalid task name
	resp, err := server.OperateTask(context.Background(), &pb.OperateTaskRequest{
		Op:   pb.TaskOp_Pause,
		Name: taskName,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)
	c.Assert(resp.Msg, check.Equals, fmt.Sprintf("task %s has no workers or not exist, can try `refresh-worker-tasks` cmd first", taskName))

	// test operate-task while worker clients not found
	server.taskWorkers[taskName] = workers
	resp, err = server.OperateTask(context.Background(), &pb.OperateTaskRequest{
		Op:   pb.TaskOp_Pause,
		Name: taskName,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 2)
	for _, opResp := range resp.Workers {
		c.Assert(opResp.Result, check.IsFalse)
		c.Assert(opResp.Msg, check.Matches, ".* relevant worker-client not found")
	}

	// test pause task successfully
	for _, workerAddr := range server.cfg.DeployMap {
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		mockWorkerClient.EXPECT().OperateSubTask(
			gomock.Any(),
			&pb.OperateSubTaskRequest{
				Op:   pb.TaskOp_Pause,
				Name: taskName,
			},
		).Return(&pb.OperateSubTaskResponse{Result: true}, nil)
		server.workerClients[workerAddr] = mockWorkerClient
	}
	resp, err = server.OperateTask(context.Background(), &pb.OperateTaskRequest{
		Op:   pb.TaskOp_Pause,
		Name: taskName,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 2)
	for _, opResp := range resp.Workers {
		c.Assert(opResp.Result, check.IsTrue)
	}

	// test operate sub task to worker returns error
	for _, workerAddr := range server.cfg.DeployMap {
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		mockWorkerClient.EXPECT().OperateSubTask(
			gomock.Any(),
			&pb.OperateSubTaskRequest{
				Op:   pb.TaskOp_Pause,
				Name: taskName,
			},
		).Return(nil, errors.New(errGRPCFailed))
		server.workerClients[workerAddr] = mockWorkerClient
	}
	resp, err = server.OperateTask(context.Background(), &pb.OperateTaskRequest{
		Op:      pb.TaskOp_Pause,
		Name:    taskName,
		Workers: workers,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 2)
	for _, opResp := range resp.Workers {
		c.Assert(opResp.Result, check.IsFalse)
		c.Assert(strings.Split(opResp.Msg, "\n")[0], check.Equals, errGRPCFailed)
	}

	// test stop task successfully, remove partial workers
	mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
	mockWorkerClient.EXPECT().OperateSubTask(
		gomock.Any(),
		&pb.OperateSubTaskRequest{
			Op:   pb.TaskOp_Stop,
			Name: taskName,
		},
	).Return(&pb.OperateSubTaskResponse{Result: true}, nil)
	server.workerClients[workers[0]] = mockWorkerClient
	resp, err = server.OperateTask(context.Background(), &pb.OperateTaskRequest{
		Op:      pb.TaskOp_Stop,
		Name:    taskName,
		Workers: []string{workers[0]},
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 1)
	c.Assert(resp.Workers[0].Result, check.IsTrue)
	c.Assert(server.taskWorkers, check.HasKey, taskName)
	c.Assert(server.taskWorkers[taskName], check.DeepEquals, []string{workers[1]})

	// test stop task successfully, remove all workers
	server.taskWorkers[taskName] = workers
	for _, workerAddr := range server.cfg.DeployMap {
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		mockWorkerClient.EXPECT().OperateSubTask(
			gomock.Any(),
			&pb.OperateSubTaskRequest{
				Op:   pb.TaskOp_Stop,
				Name: taskName,
			},
		).Return(&pb.OperateSubTaskResponse{Result: true}, nil)
		server.workerClients[workerAddr] = mockWorkerClient
	}
	resp, err = server.OperateTask(context.Background(), &pb.OperateTaskRequest{
		Op:   pb.TaskOp_Stop,
		Name: taskName,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 2)
	for _, opResp := range resp.Workers {
		c.Assert(opResp.Result, check.IsTrue)
	}
	c.Assert(len(server.taskWorkers), check.Equals, 0)
}

func (t *testMaster) TestUpdateTask(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	server := defaultMasterServer(c)

	// s.generateSubTask with error
	resp, err := server.UpdateTask(context.Background(), &pb.UpdateTaskRequest{
		Task: "invalid toml config",
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)

	// prepare workers
	workers := make([]string, 0, len(server.cfg.DeployMap))
	for _, deploy := range server.cfg.Deploy {
		workers = append(workers, deploy.Worker)
	}

	// generate subtask configs, need to mock query worker configs RPC
	mockWorkerConfig(c, server, ctrl, "", true)
	workerCfg := make(map[string]*config.SubTaskConfig)
	_, stCfgs, err := server.generateSubTask(context.Background(), taskConfig)
	c.Assert(err, check.IsNil)
	for _, stCfg := range stCfgs {
		worker, ok := server.cfg.DeployMap[stCfg.SourceID]
		c.Assert(ok, check.IsTrue)
		workerCfg[worker] = stCfg
	}

	mockUpdateTask := func(rpcSuccess bool) {
		for idx, deploy := range server.cfg.Deploy {
			mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)

			dbCfg := &config.DBConfig{
				Host:     "127.0.0.1",
				Port:     3306 + idx,
				User:     "root",
				Password: "",
			}
			rawConfig, err := dbCfg.Toml()
			c.Assert(err, check.IsNil)

			// mock query worker config
			mockWorkerClient.EXPECT().QueryWorkerConfig(
				gomock.Any(),
				&pb.QueryWorkerConfigRequest{},
			).Return(&pb.QueryWorkerConfigResponse{
				Result:   true,
				SourceID: deploy.Source,
				Content:  rawConfig,
			}, nil)

			stCfg, ok := workerCfg[deploy.Worker]
			c.Assert(ok, check.IsTrue)
			stCfgToml, err := stCfg.Toml()
			c.Assert(err, check.IsNil)

			// mock update sub task
			rets := make([]interface{}, 0, 2)
			if rpcSuccess {
				rets = []interface{}{
					&pb.CommonWorkerResponse{
						Result: true,
						Worker: deploy.Worker,
					},
					nil,
				}
			} else {
				rets = []interface{}{
					nil,
					errors.New(errGRPCFailed),
				}
			}
			mockWorkerClient.EXPECT().UpdateSubTask(
				gomock.Any(),
				&pb.UpdateSubTaskRequest{Task: stCfgToml},
			).Return(rets...)

			server.workerClients[deploy.Worker] = mockWorkerClient
		}
	}

	// test update task successfully
	mockUpdateTask(true)
	resp, err = server.UpdateTask(context.Background(), &pb.UpdateTaskRequest{
		Task:    taskConfig,
		Workers: workers,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)

	// check update-task with an invalid worker
	invalidWorker := "invalid-worker"
	mockWorkerConfig(c, server, ctrl, "", true)
	resp, err = server.UpdateTask(context.Background(), &pb.UpdateTaskRequest{
		Task:    taskConfig,
		Workers: []string{invalidWorker},
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 1)
	c.Assert(resp.Workers[0].Result, check.IsFalse)
	c.Assert(resp.Workers[0].Worker, check.Equals, invalidWorker)

	// test update sub task request to worker returns error
	mockUpdateTask(false)
	resp, err = server.UpdateTask(context.Background(), &pb.UpdateTaskRequest{
		Task: taskConfig,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 2)
	for _, workerResp := range resp.Workers {
		c.Assert(workerResp.Result, check.IsFalse)
		lines := strings.Split(workerResp.Msg, "\n")
		c.Assert(len(lines), check.Greater, 1)
		c.Assert(lines[0], check.Equals, errGRPCFailed)
	}
}

func (t *testMaster) TestUnlockDDLLock(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	server := defaultMasterServer(c)

	workers := make([]string, 0, len(server.cfg.DeployMap))
	for _, workerAddr := range server.cfg.DeployMap {
		workers = append(workers, workerAddr)
	}

	// prepare ddl lock keeper, mainly use code from ddl_lock_test.go
	sqls := []string{"stmt"}
	info := struct {
		task   string
		schema string
		table  string
	}{
		"testA", "test_db", "test_table",
	}
	lk := NewLockKeeper()
	var wg sync.WaitGroup
	for _, worker := range workers {
		wg.Add(1)
	}
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

}
