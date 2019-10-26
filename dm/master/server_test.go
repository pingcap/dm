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
	"io/ioutil"
	"net/http"

	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"

	"github.com/pingcap/dm/checker"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/master/workerrpc"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/pbmock"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
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
    black-white-list:  "instance"
    route-rules: ["sharding-route-rules-table", "sharding-route-rules-schema"]
    column-mapping-rules: ["instance-1"]
    mydumper-config-name: "global"
    loader-config-name: "global"
    syncer-config-name: "global"

  - source-id: "mysql-replica-02"
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
`

var (
	errGRPCFailed         = "test grpc request failed"
	errGRPCFailedReg      = fmt.Sprintf("(?m).*%s.*", errGRPCFailed)
	errExecDDLFailed      = "dm-worker exec ddl failed"
	msgNoSubTask          = "no sub task started"
	msgNoSubTaskReg       = fmt.Sprintf(".*%s", msgNoSubTask)
	errCheckSyncConfig    = "(?m).*check sync config with error.*"
	errCheckSyncConfigReg = fmt.Sprintf("(?m).*%s.*", errCheckSyncConfig)
)

func TestMaster(t *testing.T) {
	check.TestingT(t)
}

type testMaster struct {
}

var _ = check.Suite(&testMaster{})

func newMockRPCClient(client pb.WorkerClient) workerrpc.Client {
	c, _ := workerrpc.NewGRPCClientWrap(nil, client)
	return c
}

func testDefaultMasterServer(c *check.C) *Server {
	cfg := NewConfig()
	err := cfg.Parse([]string{"-config=./dm-master.toml"})
	c.Assert(err, check.IsNil)
	server := NewServer(cfg)
	go server.ap.Start(context.Background())

	return server
}

func testGenSubTaskConfig(c *check.C, server *Server, ctrl *gomock.Controller) map[string]*config.SubTaskConfig {
	// generate subtask configs, need to mock query worker configs RPC
	testMockWorkerConfig(c, server, ctrl, "", true)
	workerCfg := make(map[string]*config.SubTaskConfig)
	_, stCfgs, err := server.generateSubTask(context.Background(), taskConfig)
	c.Assert(err, check.IsNil)
	for _, stCfg := range stCfgs {
		worker, ok := server.cfg.DeployMap[stCfg.SourceID]
		c.Assert(ok, check.IsTrue)
		workerCfg[worker] = stCfg
	}
	return workerCfg
}

func testMockWorkerConfig(c *check.C, server *Server, ctrl *gomock.Controller, password string, result bool) {
	// mock QueryWorkerConfig API to be used in s.getWorkerConfigs
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
		server.workerClients[deploy.Worker] = newMockRPCClient(mockWorkerClient)
	}
}

func testMockStartTask(c *check.C, server *Server, ctrl *gomock.Controller, workerCfg map[string]*config.SubTaskConfig, rpcSuccess bool) {
	for idx, deploy := range server.cfg.Deploy {
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		logID := int64(idx + 1)

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
				&pb.OperateSubTaskResponse{
					Meta:  &pb.CommonWorkerResponse{Result: true, Worker: deploy.Worker},
					Op:    pb.TaskOp_Start,
					LogID: logID,
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

		if rpcSuccess {
			mockWorkerClient.EXPECT().QueryTaskOperation(
				gomock.Any(),
				&pb.QueryTaskOperationRequest{
					Name:  stCfg.Name,
					LogID: logID,
				},
			).Return(&pb.QueryTaskOperationResponse{
				Meta: &pb.CommonWorkerResponse{Result: true, Worker: deploy.Worker},
				Log:  &pb.TaskLog{Id: logID, Ts: time.Now().Unix(), Success: true},
			}, nil).MaxTimes(maxRetryNum)
		}

		server.workerClients[deploy.Worker] = newMockRPCClient(mockWorkerClient)
	}
}

func (t *testMaster) TestQueryStatus(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	server := testDefaultMasterServer(c)

	// test query all workers
	for _, workerAddr := range server.cfg.DeployMap {
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		mockWorkerClient.EXPECT().QueryStatus(
			gomock.Any(),
			&pb.QueryStatusRequest{},
		).Return(&pb.QueryStatusResponse{Result: true}, nil)
		server.workerClients[workerAddr] = newMockRPCClient(mockWorkerClient)
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
		server.workerClients[workerAddr] = newMockRPCClient(mockWorkerClient)
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
	server := testDefaultMasterServer(c)

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
			id, synced, remain, err2 := lk.TrySync(task, schema, table, workers[0], sqls, workers)
			c.Assert(err2, check.IsNil)
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
	c.Assert(resp.Locks[0].ID, check.Equals, genDDLLockID("testA", "test_db1", "test_tbl1"))

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

	server := testDefaultMasterServer(c)

	// check task successfully
	testMockWorkerConfig(c, server, ctrl, "", true)
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
	testMockWorkerConfig(c, server, ctrl, "invalid-encrypt-password", true)
	resp, err = server.CheckTask(context.Background(), &pb.CheckTaskRequest{
		Task: taskConfig,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)

	// test query worker config failed
	testMockWorkerConfig(c, server, ctrl, "", false)
	resp, err = server.CheckTask(context.Background(), &pb.CheckTaskRequest{
		Task: taskConfig,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)
}

func (t *testMaster) TestStartTask(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	server := testDefaultMasterServer(c)

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

	workerCfg := testGenSubTaskConfig(c, server, ctrl)

	// test start task successfully
	testMockStartTask(c, server, ctrl, workerCfg, true)
	resp, err = server.StartTask(context.Background(), &pb.StartTaskRequest{
		Task:    taskConfig,
		Workers: workers,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)

	// check start-task with an invalid worker
	invalidWorker := "invalid-worker"
	testMockWorkerConfig(c, server, ctrl, "", true)
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
	testMockStartTask(c, server, ctrl, workerCfg, false)
	resp, err = server.StartTask(context.Background(), &pb.StartTaskRequest{
		Task: taskConfig,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 2)
	for _, workerResp := range resp.Workers {
		c.Assert(workerResp.Result, check.IsFalse)
		c.Assert(workerResp.Msg, check.Matches, errGRPCFailedReg)
	}

	// test start task, but the first step check-task fails
	bakCheckSyncConfigFunc := checker.CheckSyncConfigFunc
	checker.CheckSyncConfigFunc = func(_ context.Context, _ []*config.SubTaskConfig) error {
		return errors.New(errCheckSyncConfig)
	}
	defer func() {
		checker.CheckSyncConfigFunc = bakCheckSyncConfigFunc
	}()
	testMockWorkerConfig(c, server, ctrl, "", true)
	resp, err = server.StartTask(context.Background(), &pb.StartTaskRequest{
		Task:    taskConfig,
		Workers: workers,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)
	c.Assert(resp.Msg, check.Matches, errCheckSyncConfigReg)
}

func (t *testMaster) TestQueryError(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()
	server := testDefaultMasterServer(c)

	// test query all workers
	for _, workerAddr := range server.cfg.DeployMap {
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		mockWorkerClient.EXPECT().QueryError(
			gomock.Any(),
			&pb.QueryErrorRequest{},
		).Return(&pb.QueryErrorResponse{Result: true}, nil)
		server.workerClients[workerAddr] = newMockRPCClient(mockWorkerClient)
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
		server.workerClients[workerAddr] = newMockRPCClient(mockWorkerClient)
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
		pauseOp  = pb.TaskOp_Pause
	)

	ctrl := gomock.NewController(c)
	defer ctrl.Finish()
	server := testDefaultMasterServer(c)
	for _, workerAddr := range server.cfg.DeployMap {
		workers = append(workers, workerAddr)
	}

	// test operate-task with invalid task name
	resp, err := server.OperateTask(context.Background(), &pb.OperateTaskRequest{
		Op:   pauseOp,
		Name: taskName,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)
	c.Assert(resp.Msg, check.Equals, fmt.Sprintf("task %s has no workers or not exist, can try `refresh-worker-tasks` cmd first", taskName))

	// test operate-task while worker clients not found
	server.taskWorkers[taskName] = workers
	resp, err = server.OperateTask(context.Background(), &pb.OperateTaskRequest{
		Op:   pauseOp,
		Name: taskName,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 2)
	for _, subtaskResp := range resp.Workers {
		c.Assert(subtaskResp.Op, check.Equals, pauseOp)
		c.Assert(subtaskResp.Meta.Msg, check.Matches, ".* relevant worker-client not found")
	}

	// test pause task successfully
	for idx, deploy := range server.cfg.Deploy {
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		logID := int64(idx + 1)
		mockWorkerClient.EXPECT().OperateSubTask(
			gomock.Any(),
			&pb.OperateSubTaskRequest{
				Op:   pauseOp,
				Name: taskName,
			},
		).Return(&pb.OperateSubTaskResponse{
			Op:    pauseOp,
			LogID: logID,
			Meta:  &pb.CommonWorkerResponse{Result: true},
		}, nil)

		mockWorkerClient.EXPECT().QueryTaskOperation(
			gomock.Any(),
			&pb.QueryTaskOperationRequest{
				Name:  taskName,
				LogID: logID,
			},
		).Return(&pb.QueryTaskOperationResponse{
			Meta: &pb.CommonWorkerResponse{Result: true, Worker: deploy.Worker},
			Log:  &pb.TaskLog{Id: logID, Ts: time.Now().Unix(), Success: true},
		}, nil)

		server.workerClients[deploy.Worker] = newMockRPCClient(mockWorkerClient)
	}
	resp, err = server.OperateTask(context.Background(), &pb.OperateTaskRequest{
		Op:   pauseOp,
		Name: taskName,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Op, check.Equals, pauseOp)
	c.Assert(resp.Workers, check.HasLen, 2)
	for _, subtaskResp := range resp.Workers {
		c.Assert(subtaskResp.Op, check.Equals, pauseOp)
		c.Assert(subtaskResp.Meta.Result, check.IsTrue)
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
		server.workerClients[workerAddr] = newMockRPCClient(mockWorkerClient)
	}
	resp, err = server.OperateTask(context.Background(), &pb.OperateTaskRequest{
		Op:      pb.TaskOp_Pause,
		Name:    taskName,
		Workers: workers,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 2)
	for _, subtaskResp := range resp.Workers {
		c.Assert(subtaskResp.Op, check.Equals, pauseOp)
		c.Assert(subtaskResp.Meta.Msg, check.Matches, errGRPCFailedReg)
	}

	// test stop task successfully, remove partial workers
	mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
	logID := int64(42)
	mockWorkerClient.EXPECT().OperateSubTask(
		gomock.Any(),
		&pb.OperateSubTaskRequest{
			Op:   pb.TaskOp_Stop,
			Name: taskName,
		},
	).Return(&pb.OperateSubTaskResponse{
		Meta:  &pb.CommonWorkerResponse{Result: true},
		LogID: logID,
		Op:    pb.TaskOp_Stop,
	}, nil)
	mockWorkerClient.EXPECT().QueryTaskOperation(
		gomock.Any(),
		&pb.QueryTaskOperationRequest{
			Name:  taskName,
			LogID: logID,
		},
	).Return(&pb.QueryTaskOperationResponse{
		Meta: &pb.CommonWorkerResponse{Result: true, Worker: workers[0]},
		Log:  &pb.TaskLog{Id: logID, Ts: time.Now().Unix(), Success: true},
	}, nil)
	server.workerClients[workers[0]] = newMockRPCClient(mockWorkerClient)
	resp, err = server.OperateTask(context.Background(), &pb.OperateTaskRequest{
		Op:      pb.TaskOp_Stop,
		Name:    taskName,
		Workers: []string{workers[0]},
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 1)
	c.Assert(resp.Workers[0].Meta.Result, check.IsTrue)
	c.Assert(server.taskWorkers, check.HasKey, taskName)
	c.Assert(server.taskWorkers[taskName], check.DeepEquals, []string{workers[1]})

	// test stop task successfully, remove all workers
	server.taskWorkers[taskName] = workers
	for idx, deploy := range server.cfg.Deploy {
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		logID := int64(idx + 100)
		mockWorkerClient.EXPECT().OperateSubTask(
			gomock.Any(),
			&pb.OperateSubTaskRequest{
				Op:   pb.TaskOp_Stop,
				Name: taskName,
			},
		).Return(&pb.OperateSubTaskResponse{
			Meta:  &pb.CommonWorkerResponse{Result: true},
			Op:    pb.TaskOp_Stop,
			LogID: logID,
		}, nil)
		mockWorkerClient.EXPECT().QueryTaskOperation(
			gomock.Any(),
			&pb.QueryTaskOperationRequest{
				Name:  taskName,
				LogID: logID,
			},
		).Return(&pb.QueryTaskOperationResponse{
			Meta: &pb.CommonWorkerResponse{Result: true, Worker: workers[0]},
			Log:  &pb.TaskLog{Id: logID, Ts: time.Now().Unix(), Success: true},
		}, nil)
		server.workerClients[deploy.Worker] = newMockRPCClient(mockWorkerClient)
	}
	resp, err = server.OperateTask(context.Background(), &pb.OperateTaskRequest{
		Op:   pb.TaskOp_Stop,
		Name: taskName,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 2)
	for _, subtaskResp := range resp.Workers {
		c.Assert(subtaskResp.Op, check.Equals, pb.TaskOp_Stop)
		c.Assert(subtaskResp.Meta.Result, check.IsTrue)
	}
	c.Assert(len(server.taskWorkers), check.Equals, 0)
}

func (t *testMaster) TestUpdateTask(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	server := testDefaultMasterServer(c)

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

	workerCfg := testGenSubTaskConfig(c, server, ctrl)

	mockUpdateTask := func(rpcSuccess bool) {
		for idx, deploy := range server.cfg.Deploy {
			mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
			logID := int64(idx + 1)

			dbCfg := &config.DBConfig{
				Host:     "127.0.0.1",
				Port:     3306 + idx,
				User:     "root",
				Password: "",
			}
			rawConfig, err2 := dbCfg.Toml()
			c.Assert(err2, check.IsNil)

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
			stCfgToml, err3 := stCfg.Toml()
			c.Assert(err3, check.IsNil)

			// mock update sub task
			rets := make([]interface{}, 0, 2)
			if rpcSuccess {
				rets = []interface{}{
					&pb.OperateSubTaskResponse{
						Meta:  &pb.CommonWorkerResponse{Result: true, Worker: deploy.Worker},
						Op:    pb.TaskOp_Start,
						LogID: logID,
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

			if rpcSuccess {
				mockWorkerClient.EXPECT().QueryTaskOperation(
					gomock.Any(),
					&pb.QueryTaskOperationRequest{
						Name:  stCfg.Name,
						LogID: logID,
					},
				).Return(&pb.QueryTaskOperationResponse{
					Meta: &pb.CommonWorkerResponse{Result: true, Worker: deploy.Worker},
					Log:  &pb.TaskLog{Id: logID, Ts: time.Now().Unix(), Success: true},
				}, nil)
			}

			server.workerClients[deploy.Worker] = newMockRPCClient(mockWorkerClient)
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
	testMockWorkerConfig(c, server, ctrl, "", true)
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
		c.Assert(workerResp.Msg, check.Matches, errGRPCFailedReg)
	}
}

func (t *testMaster) TestUnlockDDLLock(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	var (
		sqls        = []string{"stmt"}
		task        = "testA"
		schema      = "test_db"
		table       = "test_table"
		traceGIDIdx = 1
	)

	server := testDefaultMasterServer(c)

	workers := make([]string, 0, len(server.cfg.DeployMap))
	for _, workerAddr := range server.cfg.DeployMap {
		workers = append(workers, workerAddr)
	}

	mockResolveDDLLock := func(task, lockID, owner, traceGID string, ownerFail, nonOwnerFail bool) {
		for _, worker := range workers {
			mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)

			if ownerFail && worker != owner {
				continue
			}

			exec := false
			if owner == worker {
				exec = true
			}

			ret := []interface{}{
				&pb.CommonWorkerResponse{Result: true},
				nil,
			}
			if (ownerFail && worker == owner) || (nonOwnerFail && worker != owner) {
				ret[0] = &pb.CommonWorkerResponse{Result: false}
				ret[1] = errors.New(errExecDDLFailed)
			}

			mockWorkerClient.EXPECT().ExecuteDDL(
				gomock.Any(),
				&pb.ExecDDLRequest{
					Task:     task,
					LockID:   lockID,
					Exec:     exec,
					TraceGID: traceGID,
					DDLs:     sqls,
				},
			).Return(ret...)

			server.workerClients[worker] = newMockRPCClient(mockWorkerClient)
		}
	}

	prepareDDLLock := func() {
		// prepare ddl lock keeper, mainly use code from ddl_lock_test.go
		lk := NewLockKeeper()
		var wg sync.WaitGroup
		for _, w := range workers {
			wg.Add(1)
			go func(worker string) {
				defer wg.Done()
				id, _, _, err := lk.TrySync(task, schema, table, worker, sqls, workers)
				c.Assert(err, check.IsNil)
				c.Assert(lk.FindLock(id), check.NotNil)
			}(w)
		}
		wg.Wait()
		server.lockKeeper = lk
	}

	// test UnlockDDLLock successfully
	prepareDDLLock()
	lockID := genDDLLockID(task, schema, table)
	traceGID := fmt.Sprintf("resolveDDLLock.%d", traceGIDIdx)
	traceGIDIdx++
	mockResolveDDLLock(task, lockID, workers[0], traceGID, false, false)
	resp, err := server.UnlockDDLLock(context.Background(), &pb.UnlockDDLLockRequest{
		ID:           lockID,
		ReplaceOwner: workers[0],
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)

	// test UnlockDDLLock but DDL owner executed failed
	prepareDDLLock()
	lockID = genDDLLockID(task, schema, table)
	traceGID = fmt.Sprintf("resolveDDLLock.%d", traceGIDIdx)
	traceGIDIdx++
	mockResolveDDLLock(task, lockID, workers[0], traceGID, true, false)
	resp, err = server.UnlockDDLLock(context.Background(), &pb.UnlockDDLLockRequest{
		ID:           lockID,
		ReplaceOwner: workers[0],
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)
	c.Assert(server.lockKeeper.FindLock(lockID), check.NotNil)

	// retry UnlockDDLLock with force remove, but still DDL owner executed failed
	traceGID = fmt.Sprintf("resolveDDLLock.%d", traceGIDIdx)
	traceGIDIdx++
	mockResolveDDLLock(task, lockID, workers[0], traceGID, true, false)
	resp, err = server.UnlockDDLLock(context.Background(), &pb.UnlockDDLLockRequest{
		ID:           lockID,
		ReplaceOwner: workers[0],
		ForceRemove:  true,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)
	c.Assert(server.lockKeeper.FindLock(lockID), check.IsNil)

	// test UnlockDDLLock, DDL owner executed successfully but other workers failed
	prepareDDLLock()
	lockID = genDDLLockID(task, schema, table)
	traceGID = fmt.Sprintf("resolveDDLLock.%d", traceGIDIdx)
	traceGIDIdx++
	mockResolveDDLLock(task, lockID, workers[0], traceGID, false, true)
	resp, err = server.UnlockDDLLock(context.Background(), &pb.UnlockDDLLockRequest{
		ID:           lockID,
		ReplaceOwner: workers[0],
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)
	c.Assert(server.lockKeeper.FindLock(lockID), check.IsNil)

	// TODO: add SQL operator test
}

func (t *testMaster) TestBreakWorkerDDLLock(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	server := testDefaultMasterServer(c)
	var (
		task    = "test"
		schema  = "test_db"
		table   = "test_table"
		lockID  = genDDLLockID(task, schema, table)
		workers = make([]string, 0, len(server.cfg.Deploy))
	)

	for _, deploy := range server.cfg.Deploy {
		workers = append(workers, deploy.Worker)
	}

	// mock BreakDDLLock request
	mockBreakDDLLock := func(rpcSuccess bool) {
		for _, deploy := range server.cfg.Deploy {
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
			mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
			mockWorkerClient.EXPECT().BreakDDLLock(
				gomock.Any(),
				&pb.BreakDDLLockRequest{
					Task:         "test",
					RemoveLockID: lockID,
					SkipDDL:      true,
				},
			).Return(rets...)
			server.workerClients[deploy.Worker] = newMockRPCClient(mockWorkerClient)
		}
	}

	// test BreakWorkerDDLLock with invalid dm-worker[s]
	resp, err := server.BreakWorkerDDLLock(context.Background(), &pb.BreakWorkerDDLLockRequest{
		Task:         task,
		Workers:      []string{"invalid-worker1", "invalid-worker2"},
		RemoveLockID: lockID,
		SkipDDL:      true,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 2)
	for _, w := range resp.Workers {
		c.Assert(w.Result, check.IsFalse)
		c.Assert(w.Msg, check.Matches, ".*relevant worker-client not found")
	}

	// test BreakWorkerDDLLock successfully
	mockBreakDDLLock(true)
	resp, err = server.BreakWorkerDDLLock(context.Background(), &pb.BreakWorkerDDLLockRequest{
		Task:         task,
		Workers:      workers,
		RemoveLockID: lockID,
		SkipDDL:      true,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 2)
	for _, w := range resp.Workers {
		c.Assert(w.Result, check.IsTrue)
	}

	// test BreakWorkerDDLLock with error response
	mockBreakDDLLock(false)
	resp, err = server.BreakWorkerDDLLock(context.Background(), &pb.BreakWorkerDDLLockRequest{
		Task:         task,
		Workers:      workers,
		RemoveLockID: lockID,
		SkipDDL:      true,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 2)
	for _, w := range resp.Workers {
		c.Assert(w.Result, check.IsFalse)
		c.Assert(w.Msg, check.Matches, errGRPCFailedReg)
	}
}

func (t *testMaster) TestRefreshWorkerTasks(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	server := testDefaultMasterServer(c)

	workers := make([]string, 0, len(server.cfg.DeployMap))

	// mock query status, each worker has two valid tasks
	for _, deploy := range server.cfg.Deploy {
		workers = append(workers, deploy.Worker)
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		mockWorkerClient.EXPECT().QueryStatus(
			gomock.Any(),
			&pb.QueryStatusRequest{},
		).Return(&pb.QueryStatusResponse{
			Result: true,
			Worker: deploy.Worker,
			SubTaskStatus: []*pb.SubTaskStatus{
				{
					Stage: pb.Stage_Running,
					Name:  "test",
				},
				{
					Stage: pb.Stage_Running,
					Name:  "test2",
				},
				{
					Stage: pb.Stage_InvalidStage, // this will be ignored
					Name:  "test3",
				},
			},
		}, nil)
		server.workerClients[deploy.Worker] = newMockRPCClient(mockWorkerClient)
	}

	// test RefreshWorkerTasks, with two running tasks for each workers
	resp, err := server.RefreshWorkerTasks(context.Background(), &pb.RefreshWorkerTasksRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(len(server.taskWorkers), check.Equals, 2)
	for _, taskName := range []string{"test", "test2"} {
		c.Assert(server.taskWorkers, check.HasKey, taskName)
		c.Assert(server.taskWorkers[taskName], check.DeepEquals, workers)
	}

	// mock query status, each worker has no task
	for _, deploy := range server.cfg.Deploy {
		workers = append(workers, deploy.Worker)
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		mockWorkerClient.EXPECT().QueryStatus(
			gomock.Any(),
			&pb.QueryStatusRequest{},
		).Return(&pb.QueryStatusResponse{
			Result: true,
			Worker: deploy.Worker,
			Msg:    msgNoSubTask,
		}, nil)
		server.workerClients[deploy.Worker] = newMockRPCClient(mockWorkerClient)
	}

	// test RefreshWorkerTasks, with no started tasks
	resp, err = server.RefreshWorkerTasks(context.Background(), &pb.RefreshWorkerTasksRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(len(server.taskWorkers), check.Equals, 0)
	c.Assert(resp.Workers, check.HasLen, 2)
	for _, w := range resp.Workers {
		c.Assert(w.Msg, check.Matches, msgNoSubTaskReg)
	}
}

func (t *testMaster) TestPurgeWorkerRelay(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	server := testDefaultMasterServer(c)
	var (
		now      = time.Now().Unix()
		filename = "mysql-bin.000005"
		workers  = make([]string, 0, len(server.cfg.Deploy))
	)

	for _, deploy := range server.cfg.Deploy {
		workers = append(workers, deploy.Worker)
	}

	// mock PurgeRelay request
	mockPurgeRelay := func(rpcSuccess bool) {
		for _, deploy := range server.cfg.Deploy {
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
			mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
			mockWorkerClient.EXPECT().PurgeRelay(
				gomock.Any(),
				&pb.PurgeRelayRequest{
					Time:     now,
					Filename: filename,
				},
			).Return(rets...)
			server.workerClients[deploy.Worker] = newMockRPCClient(mockWorkerClient)
		}
	}

	// test PurgeWorkerRelay with invalid dm-worker[s]
	resp, err := server.PurgeWorkerRelay(context.Background(), &pb.PurgeWorkerRelayRequest{
		Workers:  []string{"invalid-worker1", "invalid-worker2"},
		Time:     now,
		Filename: filename,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 2)
	for _, w := range resp.Workers {
		c.Assert(w.Result, check.IsFalse)
		c.Assert(w.Msg, check.Matches, ".*relevant worker-client not found")
	}

	// test PurgeWorkerRelay successfully
	mockPurgeRelay(true)
	resp, err = server.PurgeWorkerRelay(context.Background(), &pb.PurgeWorkerRelayRequest{
		Workers:  workers,
		Time:     now,
		Filename: filename,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 2)
	for _, w := range resp.Workers {
		c.Assert(w.Result, check.IsTrue)
	}

	// test PurgeWorkerRelay with error response
	mockPurgeRelay(false)
	resp, err = server.PurgeWorkerRelay(context.Background(), &pb.PurgeWorkerRelayRequest{
		Workers:  workers,
		Time:     now,
		Filename: filename,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 2)
	for _, w := range resp.Workers {
		c.Assert(w.Result, check.IsFalse)
		c.Assert(w.Msg, check.Matches, errGRPCFailedReg)
	}
}

func (t *testMaster) TestSwitchWorkerRelayMaster(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	server := testDefaultMasterServer(c)
	workers := make([]string, 0, len(server.cfg.Deploy))

	for _, deploy := range server.cfg.Deploy {
		workers = append(workers, deploy.Worker)
	}

	// mock SwitchRelayMaster request
	mockSwitchRelayMaster := func(rpcSuccess bool) {
		for _, deploy := range server.cfg.Deploy {
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
			mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
			mockWorkerClient.EXPECT().SwitchRelayMaster(
				gomock.Any(),
				&pb.SwitchRelayMasterRequest{},
			).Return(rets...)
			server.workerClients[deploy.Worker] = newMockRPCClient(mockWorkerClient)
		}
	}

	// test SwitchWorkerRelayMaster with invalid dm-worker[s]
	resp, err := server.SwitchWorkerRelayMaster(context.Background(), &pb.SwitchWorkerRelayMasterRequest{
		Workers: []string{"invalid-worker1", "invalid-worker2"},
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 2)
	for _, w := range resp.Workers {
		c.Assert(w.Result, check.IsFalse)
		c.Assert(w.Msg, check.Matches, "(?m).*relevant worker-client not found.*")
	}

	// test SwitchWorkerRelayMaster successfully
	mockSwitchRelayMaster(true)
	resp, err = server.SwitchWorkerRelayMaster(context.Background(), &pb.SwitchWorkerRelayMasterRequest{
		Workers: workers,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 2)
	for _, w := range resp.Workers {
		c.Assert(w.Result, check.IsTrue)
	}

	// test SwitchWorkerRelayMaster with error response
	mockSwitchRelayMaster(false)
	resp, err = server.SwitchWorkerRelayMaster(context.Background(), &pb.SwitchWorkerRelayMasterRequest{
		Workers: workers,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 2)
	for _, w := range resp.Workers {
		c.Assert(w.Result, check.IsFalse)
		c.Assert(w.Msg, check.Matches, errGRPCFailedReg)
	}
}

func (t *testMaster) TestOperateWorkerRelayTask(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	server := testDefaultMasterServer(c)
	workers := make([]string, 0, len(server.cfg.Deploy))

	for _, deploy := range server.cfg.Deploy {
		workers = append(workers, deploy.Worker)
	}

	// mock OperateRelay request
	mockOperateRelay := func(rpcSuccess bool) {
		for _, deploy := range server.cfg.Deploy {
			rets := make([]interface{}, 0, 2)
			if rpcSuccess {
				rets = []interface{}{
					&pb.OperateRelayResponse{
						Result: true,
						Worker: deploy.Worker,
						Op:     pb.RelayOp_PauseRelay,
					},
					nil,
				}
			} else {
				rets = []interface{}{
					nil,
					errors.New(errGRPCFailed),
				}
			}
			mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
			mockWorkerClient.EXPECT().OperateRelay(
				gomock.Any(),
				&pb.OperateRelayRequest{Op: pb.RelayOp_PauseRelay},
			).Return(rets...)
			server.workerClients[deploy.Worker] = newMockRPCClient(mockWorkerClient)
		}
	}

	// test OperateWorkerRelayTask with invalid dm-worker[s]
	resp, err := server.OperateWorkerRelayTask(context.Background(), &pb.OperateWorkerRelayRequest{
		Workers: []string{"invalid-worker1", "invalid-worker2"},
		Op:      pb.RelayOp_PauseRelay,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 2)
	for _, w := range resp.Workers {
		c.Assert(w.Result, check.IsFalse)
		c.Assert(w.Msg, check.Matches, ".*relevant worker-client not found")
	}

	// test OperateWorkerRelayTask successfully
	mockOperateRelay(true)
	resp, err = server.OperateWorkerRelayTask(context.Background(), &pb.OperateWorkerRelayRequest{
		Workers: workers,
		Op:      pb.RelayOp_PauseRelay,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 2)
	for _, w := range resp.Workers {
		c.Assert(w.Result, check.IsTrue)
		c.Assert(w.Op, check.Equals, pb.RelayOp_PauseRelay)
	}

	// test OperateWorkerRelayTask with error response
	mockOperateRelay(false)
	resp, err = server.OperateWorkerRelayTask(context.Background(), &pb.OperateWorkerRelayRequest{
		Workers: workers,
		Op:      pb.RelayOp_PauseRelay,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 2)
	for _, w := range resp.Workers {
		c.Assert(w.Result, check.IsFalse)
		c.Assert(w.Msg, check.Matches, errGRPCFailedReg)
	}
}

func (t *testMaster) TestFetchWorkerDDLInfo(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	server := testDefaultMasterServer(c)
	workers := make([]string, 0, len(server.cfg.Deploy))
	for _, deploy := range server.cfg.Deploy {
		workers = append(workers, deploy.Worker)
	}
	server.taskWorkers = map[string][]string{"test": workers}
	var (
		task     = "test"
		schema   = "test_db"
		table    = "test_table"
		ddls     = []string{"stmt"}
		traceGID = fmt.Sprintf("resolveDDLLock.%d", 1)
		lockID   = genDDLLockID(task, schema, table)
		wg       sync.WaitGroup
	)

	// mock FetchDDLInfo stream API
	for _, deploy := range server.cfg.Deploy {
		stream := pbmock.NewMockWorker_FetchDDLInfoClient(ctrl)
		stream.EXPECT().Recv().Return(&pb.DDLInfo{
			Task:   task,
			Schema: schema,
			Table:  table,
			DDLs:   ddls,
		}, nil)
		// This will lead to a select on ctx.Done and time.After(fetchDDLInfoRetryTimeout),
		// so we have enough time to cancel the context.
		stream.EXPECT().Recv().Return(nil, io.EOF).MaxTimes(1)
		stream.EXPECT().Send(&pb.DDLLockInfo{Task: task, ID: lockID}).Return(nil)
		stream.EXPECT().CloseSend().Return(nil)

		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		mockWorkerClient.EXPECT().FetchDDLInfo(gomock.Any()).Return(stream, nil)
		// which worker is DDLLock owner is not determined, so we mock a exec/non-exec
		// ExecDDLRequest to each worker client, with at most one time call.
		mockWorkerClient.EXPECT().ExecuteDDL(
			gomock.Any(),
			&pb.ExecDDLRequest{
				Task:     task,
				LockID:   lockID,
				Exec:     true,
				TraceGID: traceGID,
				DDLs:     ddls,
			},
		).Return(&pb.CommonWorkerResponse{Result: true}, nil).MaxTimes(1)
		mockWorkerClient.EXPECT().ExecuteDDL(
			gomock.Any(),
			&pb.ExecDDLRequest{
				Task:     task,
				LockID:   lockID,
				Exec:     false,
				TraceGID: traceGID,
				DDLs:     ddls,
			},
		).Return(&pb.CommonWorkerResponse{Result: true}, nil).MaxTimes(1)

		server.workerClients[deploy.Worker] = newMockRPCClient(mockWorkerClient)
	}

	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		server.fetchWorkerDDLInfo(ctx)
	}()
	go func() {
		for {
			select {
			case <-time.After(time.Millisecond * 10):
				if server.lockKeeper.FindLock(lockID) == nil {
					cancel()
					return
				}
			}
		}
	}()
	wg.Wait()
}

func (t *testMaster) TestServer(c *check.C) {
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-master.toml"}), check.IsNil)

	s := NewServer(cfg)

	masterAddr := cfg.MasterAddr
	s.cfg.MasterAddr = ""
	err := s.Start()
	c.Assert(terror.ErrMasterHostPortNotValid.Equal(err), check.IsTrue)
	s.Close()
	s.cfg.MasterAddr = masterAddr

	go func() {
		err1 := s.Start()
		c.Assert(err1, check.IsNil)
	}()

	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return !s.closed.Get()
	}), check.IsTrue)

	t.testHTTPInterface(c, "status")

	dupServer := NewServer(cfg)
	err = dupServer.Start()
	c.Assert(terror.ErrMasterStartService.Equal(err), check.IsTrue)
	c.Assert(err.Error(), check.Matches, ".*bind: address already in use")

	// close
	s.Close()

	c.Assert(utils.WaitSomething(30, 10*time.Millisecond, func() bool {
		return s.closed.Get()
	}), check.IsTrue)
}

func (t *testMaster) testHTTPInterface(c *check.C, uri string) {
	resp, err := http.Get("http://127.0.0.1:8261/" + uri)
	c.Assert(err, check.IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, check.Equals, 200)
	_, err = ioutil.ReadAll(resp.Body)
	c.Assert(err, check.IsNil)
}
