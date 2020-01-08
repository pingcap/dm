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
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/pd/pkg/tempurl"
	"go.etcd.io/etcd/integration"

	"github.com/pingcap/dm/checker"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/master/coordinator"
	"github.com/pingcap/dm/dm/master/workerrpc"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/pbmock"
	"github.com/pingcap/dm/pkg/etcdutil"
	"github.com/pingcap/dm/pkg/log"
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
	testEtcdCluster       *integration.ClusterV3
)

func TestMaster(t *testing.T) {
	testEtcdCluster = integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer testEtcdCluster.Terminate(t)

	check.TestingT(t)
}

type testMaster struct {
}

var _ = check.Suite(&testMaster{})

func (t *testMaster) SetUpSuite(c *check.C) {
	err := log.InitLogger(&log.Config{})
	c.Assert(err, check.IsNil)
}

func newMockRPCClient(client pb.WorkerClient) workerrpc.Client {
	c, _ := workerrpc.NewGRPCClientWrap(nil, client)
	return c
}

func extractWorkerSource(deployMapper []*DeployMapper) ([]string, []string) {
	sources := make([]string, 0, len(deployMapper))
	workers := make([]string, 0, len(deployMapper))
	for _, deploy := range deployMapper {
		sources = append(sources, deploy.Source)
		workers = append(sources, deploy.Worker)
	}
	return sources, workers
}

func testDefaultMasterServer(c *check.C) *Server {
	cfg := NewConfig()
	err := cfg.Parse([]string{"-config=./dm-master.toml"})
	c.Assert(err, check.IsNil)
	cfg.DataDir = c.MkDir()
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

func testMockCoordinator(c *check.C, sources, workers []string, workerClients map[string]workerrpc.Client) *coordinator.Coordinator {
	coordinator2 := coordinator.NewCoordinator()
	err := coordinator2.Start(context.Background(), testEtcdCluster.RandClient())
	c.Assert(err, check.IsNil)
	for i := range workers {
		// add worker to coordinator's workers map
		coordinator2.AddWorker("worker"+string(i), workers[i], workerClients[workers[i]])
		// set this worker's status to workerFree
		coordinator2.AddWorker("worker"+string(i), workers[i], nil)
		// operate mysql config on this worker
		cfg := &config.MysqlConfig{SourceID: sources[i]}
		w, err := coordinator2.AcquireWorkerForSource(cfg.SourceID)
		c.Assert(err, check.IsNil)
		coordinator2.HandleStartedWorker(w, cfg, true)
	}
	return coordinator2
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

		server.workerClients[deploy.Worker] = newMockRPCClient(mockWorkerClient)
	}
}

func (t *testMaster) TestQueryStatus(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	server := testDefaultMasterServer(c)
	sources, workers := extractWorkerSource(server.cfg.Deploy)

	// test query all workers
	for _, deploy := range server.cfg.Deploy {
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		mockWorkerClient.EXPECT().QueryStatus(
			gomock.Any(),
			&pb.QueryStatusRequest{},
		).Return(&pb.QueryStatusResponse{Result: true}, nil)
		server.workerClients[deploy.Worker] = newMockRPCClient(mockWorkerClient)
	}
	server.coordinator = testMockCoordinator(c, sources, workers, server.workerClients)
	resp, err := server.QueryStatus(context.Background(), &pb.QueryStatusListRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)

	for _, deploy := range server.cfg.Deploy {
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		mockWorkerClient.EXPECT().QueryStatus(
			gomock.Any(),
			&pb.QueryStatusRequest{},
		).Return(&pb.QueryStatusResponse{Result: true}, nil)
		server.workerClients[deploy.Worker] = newMockRPCClient(mockWorkerClient)
	}
	server.coordinator = testMockCoordinator(c, sources, workers, server.workerClients)
	resp, err = server.QueryStatus(context.Background(), &pb.QueryStatusListRequest{
		Sources: workers,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)

	// query with invalid dm-worker[s]
	resp, err = server.QueryStatus(context.Background(), &pb.QueryStatusListRequest{
		Sources: []string{"invalid-source1", "invalid-source2"},
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
		Sources: workers,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Locks, check.HasLen, 1)
	c.Assert(resp.Locks[0].ID, check.Equals, genDDLLockID("testA", "test_db1", "test_tbl1"))

	// test specify a mismatch worker
	resp, err = server.ShowDDLLocks(context.Background(), &pb.ShowDDLLocksRequest{
		Sources: []string{"invalid-worker"},
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
	sources, workers := extractWorkerSource(server.cfg.Deploy)

	// check task successfully
	testMockWorkerConfig(c, server, ctrl, "", true)
	server.coordinator = testMockCoordinator(c, sources, workers, server.workerClients)
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
	server.coordinator = testMockCoordinator(c, sources, workers, server.workerClients)
	resp, err = server.CheckTask(context.Background(), &pb.CheckTaskRequest{
		Task: taskConfig,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)

	// test query worker config failed
	testMockWorkerConfig(c, server, ctrl, "", false)
	server.coordinator = testMockCoordinator(c, sources, workers, server.workerClients)
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
	sources, workers := extractWorkerSource(server.cfg.Deploy)

	// s.generateSubTask with error
	resp, err := server.StartTask(context.Background(), &pb.StartTaskRequest{
		Task: "invalid toml config",
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)

	workerCfg := testGenSubTaskConfig(c, server, ctrl)

	// test start task successfully
	testMockStartTask(c, server, ctrl, workerCfg, true)
	server.coordinator = testMockCoordinator(c, sources, workers, server.workerClients)
	resp, err = server.StartTask(context.Background(), &pb.StartTaskRequest{
		Task:    taskConfig,
		Sources: sources,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)

	// check start-task with an invalid worker
	invalidSource := "invalid-source"
	testMockWorkerConfig(c, server, ctrl, "", true)
	server.coordinator = testMockCoordinator(c, sources, workers, server.workerClients)
	resp, err = server.StartTask(context.Background(), &pb.StartTaskRequest{
		Task:    taskConfig,
		Sources: []string{invalidSource},
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 1)
	c.Assert(resp.Workers[0].Result, check.IsFalse)
	c.Assert(resp.Workers[0].Worker, check.Equals, invalidSource)

	// test start sub task request to worker returns error
	testMockStartTask(c, server, ctrl, workerCfg, false)
	server.coordinator = testMockCoordinator(c, sources, workers, server.workerClients)
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
	server.coordinator = testMockCoordinator(c, sources, workers, server.workerClients)
	resp, err = server.StartTask(context.Background(), &pb.StartTaskRequest{
		Task:    taskConfig,
		Sources: workers,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)
	c.Assert(resp.Msg, check.Matches, errCheckSyncConfigReg)
}

func (t *testMaster) TestQueryError(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()
	server := testDefaultMasterServer(c)
	sources, workers := extractWorkerSource(server.cfg.Deploy)

	// test query all workers
	for _, deploy := range server.cfg.Deploy {
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		mockWorkerClient.EXPECT().QueryError(
			gomock.Any(),
			&pb.QueryErrorRequest{},
		).Return(&pb.QueryErrorResponse{Result: true}, nil)
		server.workerClients[deploy.Worker] = newMockRPCClient(mockWorkerClient)
	}
	server.coordinator = testMockCoordinator(c, sources, workers, server.workerClients)
	resp, err := server.QueryError(context.Background(), &pb.QueryErrorListRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)

	// query specified dm-worker[s]
	for _, deploy := range server.cfg.Deploy {
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		mockWorkerClient.EXPECT().QueryError(
			gomock.Any(),
			&pb.QueryErrorRequest{},
		).Return(&pb.QueryErrorResponse{Result: true}, nil)
		server.workerClients[deploy.Worker] = newMockRPCClient(mockWorkerClient)
	}
	server.coordinator = testMockCoordinator(c, sources, workers, server.workerClients)
	resp, err = server.QueryError(context.Background(), &pb.QueryErrorListRequest{
		Sources: sources,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)

	// query with invalid dm-worker[s]
	resp, err = server.QueryError(context.Background(), &pb.QueryErrorListRequest{
		Sources: []string{"invalid-source1", "invalid-source2"},
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
		pauseOp  = pb.TaskOp_Pause
	)

	ctrl := gomock.NewController(c)
	defer ctrl.Finish()
	server := testDefaultMasterServer(c)
	sources, workers := extractWorkerSource(server.cfg.Deploy)

	// test operate-task with invalid task name
	resp, err := server.OperateTask(context.Background(), &pb.OperateTaskRequest{
		Op:   pauseOp,
		Name: taskName,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)
	c.Assert(resp.Msg, check.Equals, fmt.Sprintf("task %s has no workers or not exist, can try `refresh-worker-tasks` cmd first", taskName))

	// test operate-task while worker clients not found
	server.taskSources[taskName] = workers
	resp, err = server.OperateTask(context.Background(), &pb.OperateTaskRequest{
		Op:   pauseOp,
		Name: taskName,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 2)
	for _, subtaskResp := range resp.Workers {
		c.Assert(subtaskResp.Op, check.Equals, pauseOp)
		c.Assert(subtaskResp.Msg, check.Matches, ".* relevant worker-client not found")
	}

	// test pause task successfully
	for _, deploy := range server.cfg.Deploy {
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		mockWorkerClient.EXPECT().OperateSubTask(
			gomock.Any(),
			&pb.OperateSubTaskRequest{
				Op:   pauseOp,
				Name: taskName,
			},
		).Return(&pb.OperateSubTaskResponse{
			Op:     pauseOp,
			Result: true,
		}, nil)

		server.workerClients[deploy.Worker] = newMockRPCClient(mockWorkerClient)
	}
	server.coordinator = testMockCoordinator(c, sources, workers, server.workerClients)
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
		c.Assert(subtaskResp.Result, check.IsTrue)
	}

	// test operate sub task to worker returns error
	for _, deploy := range server.cfg.Deploy {
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		mockWorkerClient.EXPECT().OperateSubTask(
			gomock.Any(),
			&pb.OperateSubTaskRequest{
				Op:   pb.TaskOp_Pause,
				Name: taskName,
			},
		).Return(nil, errors.New(errGRPCFailed))
		server.workerClients[deploy.Worker] = newMockRPCClient(mockWorkerClient)
	}
	server.coordinator = testMockCoordinator(c, sources, workers, server.workerClients)
	resp, err = server.OperateTask(context.Background(), &pb.OperateTaskRequest{
		Op:      pb.TaskOp_Pause,
		Name:    taskName,
		Sources: workers,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 2)
	for _, subtaskResp := range resp.Workers {
		c.Assert(subtaskResp.Op, check.Equals, pauseOp)
		c.Assert(subtaskResp.Msg, check.Matches, errGRPCFailedReg)
	}

	// test stop task successfully, remove partial workers
	mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
	mockWorkerClient.EXPECT().OperateSubTask(
		gomock.Any(),
		&pb.OperateSubTaskRequest{
			Op:   pb.TaskOp_Stop,
			Name: taskName,
		},
	).Return(&pb.OperateSubTaskResponse{
		Op:     pb.TaskOp_Stop,
		Result: true,
	}, nil)

	server.workerClients[workers[0]] = newMockRPCClient(mockWorkerClient)
	server.coordinator = testMockCoordinator(c, sources, workers, server.workerClients)
	resp, err = server.OperateTask(context.Background(), &pb.OperateTaskRequest{
		Op:      pb.TaskOp_Stop,
		Name:    taskName,
		Sources: []string{workers[0]},
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 1)
	c.Assert(resp.Workers[0].Result, check.IsTrue)
	c.Assert(server.taskSources, check.HasKey, taskName)
	c.Assert(server.taskSources[taskName], check.DeepEquals, []string{workers[1]})

	// test stop task successfully, remove all workers
	server.taskSources[taskName] = workers
	for _, deploy := range server.cfg.Deploy {
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		mockWorkerClient.EXPECT().OperateSubTask(
			gomock.Any(),
			&pb.OperateSubTaskRequest{
				Op:   pb.TaskOp_Stop,
				Name: taskName,
			},
		).Return(&pb.OperateSubTaskResponse{
			Op:     pb.TaskOp_Stop,
			Result: true,
		}, nil)

		server.workerClients[deploy.Worker] = newMockRPCClient(mockWorkerClient)
	}
	server.coordinator = testMockCoordinator(c, sources, workers, server.workerClients)
	resp, err = server.OperateTask(context.Background(), &pb.OperateTaskRequest{
		Op:   pb.TaskOp_Stop,
		Name: taskName,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 2)
	for _, subtaskResp := range resp.Workers {
		c.Assert(subtaskResp.Op, check.Equals, pb.TaskOp_Stop)
		c.Assert(subtaskResp.Result, check.IsTrue)
	}
	c.Assert(len(server.taskSources), check.Equals, 0)
}

func (t *testMaster) TestUpdateTask(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	server := testDefaultMasterServer(c)
	sources, workers := extractWorkerSource(server.cfg.Deploy)

	// s.generateSubTask with error
	resp, err := server.UpdateTask(context.Background(), &pb.UpdateTaskRequest{
		Task: "invalid toml config",
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)

	workerCfg := testGenSubTaskConfig(c, server, ctrl)

	mockUpdateTask := func(rpcSuccess bool) {
		for idx, deploy := range server.cfg.Deploy {
			mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)

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

			server.workerClients[deploy.Worker] = newMockRPCClient(mockWorkerClient)
		}
	}

	// test update task successfully
	mockUpdateTask(true)
	server.coordinator = testMockCoordinator(c, sources, workers, server.workerClients)
	resp, err = server.UpdateTask(context.Background(), &pb.UpdateTaskRequest{
		Task:    taskConfig,
		Sources: workers,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)

	// check update-task with an invalid worker
	invalidWorker := "invalid-worker"
	testMockWorkerConfig(c, server, ctrl, "", true)
	server.coordinator = testMockCoordinator(c, sources, workers, server.workerClients)
	resp, err = server.UpdateTask(context.Background(), &pb.UpdateTaskRequest{
		Task:    taskConfig,
		Sources: []string{invalidWorker},
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 1)
	c.Assert(resp.Workers[0].Result, check.IsFalse)
	c.Assert(resp.Workers[0].Worker, check.Equals, invalidWorker)

	// test update sub task request to worker returns error
	mockUpdateTask(false)
	server.coordinator = testMockCoordinator(c, sources, workers, server.workerClients)
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
	sources, workers := extractWorkerSource(server.cfg.Deploy)

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
	server.coordinator = testMockCoordinator(c, sources, workers, server.workerClients)
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
	server.coordinator = testMockCoordinator(c, sources, workers, server.workerClients)
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
	server.coordinator = testMockCoordinator(c, sources, workers, server.workerClients)
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
	server.coordinator = testMockCoordinator(c, sources, workers, server.workerClients)
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
		task   = "test"
		schema = "test_db"
		table  = "test_table"
		lockID = genDDLLockID(task, schema, table)
	)
	sources, workers := extractWorkerSource(server.cfg.Deploy)

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
		Sources:      []string{"invalid-worker1", "invalid-worker2"},
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
	server.coordinator = testMockCoordinator(c, sources, workers, server.workerClients)

	resp, err = server.BreakWorkerDDLLock(context.Background(), &pb.BreakWorkerDDLLockRequest{
		Task:         task,
		Sources:      sources,
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
	server.coordinator = testMockCoordinator(c, sources, workers, server.workerClients)

	resp, err = server.BreakWorkerDDLLock(context.Background(), &pb.BreakWorkerDDLLockRequest{
		Task:         task,
		Sources:      sources,
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

func (t *testMaster) TestPurgeWorkerRelay(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	server := testDefaultMasterServer(c)
	sources, workers := extractWorkerSource(server.cfg.Deploy)
	var (
		now      = time.Now().Unix()
		filename = "mysql-bin.000005"
	)

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
		Sources:  []string{"invalid-worker1", "invalid-worker2"},
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
	server.coordinator = testMockCoordinator(c, sources, workers, server.workerClients)
	resp, err = server.PurgeWorkerRelay(context.Background(), &pb.PurgeWorkerRelayRequest{
		Sources:  workers,
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
	server.coordinator = testMockCoordinator(c, sources, workers, server.workerClients)
	resp, err = server.PurgeWorkerRelay(context.Background(), &pb.PurgeWorkerRelayRequest{
		Sources:  workers,
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
	sources, workers := extractWorkerSource(server.cfg.Deploy)

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
		Sources: []string{"invalid-worker1", "invalid-worker2"},
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
	server.coordinator = testMockCoordinator(c, sources, workers, server.workerClients)
	resp, err = server.SwitchWorkerRelayMaster(context.Background(), &pb.SwitchWorkerRelayMasterRequest{
		Sources: workers,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Workers, check.HasLen, 2)
	for _, w := range resp.Workers {
		c.Assert(w.Result, check.IsTrue)
	}

	// test SwitchWorkerRelayMaster with error response
	mockSwitchRelayMaster(false)
	server.coordinator = testMockCoordinator(c, sources, workers, server.workerClients)
	resp, err = server.SwitchWorkerRelayMaster(context.Background(), &pb.SwitchWorkerRelayMasterRequest{
		Sources: workers,
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
	sources, workers := extractWorkerSource(server.cfg.Deploy)

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
		Sources: []string{"invalid-worker1", "invalid-worker2"},
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
	server.coordinator = testMockCoordinator(c, sources, workers, server.workerClients)
	resp, err = server.OperateWorkerRelayTask(context.Background(), &pb.OperateWorkerRelayRequest{
		Sources: workers,
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
	server.coordinator = testMockCoordinator(c, sources, workers, server.workerClients)
	resp, err = server.OperateWorkerRelayTask(context.Background(), &pb.OperateWorkerRelayRequest{
		Sources: workers,
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
	sources, workers := extractWorkerSource(server.cfg.Deploy)
	server.taskSources = map[string][]string{"test": sources}
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

	server.coordinator = testMockCoordinator(c, sources, workers, server.workerClients)
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
	cfg.DataDir = c.MkDir()
	cfg.MasterAddr = tempurl.Alloc()[len("http://"):]

	s := NewServer(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	err1 := s.Start(ctx)
	c.Assert(err1, check.IsNil)

	t.testHTTPInterface(c, fmt.Sprintf("http://%s/status", cfg.MasterAddr), []byte(utils.GetRawInfo()))
	t.testHTTPInterface(c, fmt.Sprintf("http://%s/debug/pprof/", cfg.MasterAddr), []byte("Types of profiles available"))
	// HTTP API in this unit test is unstable, but we test it in `http_apis` in integration test.
	//t.testHTTPInterface(c, fmt.Sprintf("http://%s/apis/v1alpha1/status/test-task", cfg.MasterAddr), []byte("task test-task has no workers or not exist"))

	dupServer := NewServer(cfg)
	err := dupServer.Start(ctx)
	c.Assert(terror.ErrMasterStartEmbedEtcdFail.Equal(err), check.IsTrue)
	c.Assert(err.Error(), check.Matches, ".*bind: address already in use")

	// close
	cancel()
	s.Close()

	c.Assert(utils.WaitSomething(30, 10*time.Millisecond, func() bool {
		return s.closed.Get()
	}), check.IsTrue)
}

func (t *testMaster) testHTTPInterface(c *check.C, url string, contain []byte) {
	resp, err := http.Get(url)
	c.Assert(err, check.IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, check.Equals, 200)

	body, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, check.IsNil)
	c.Assert(bytes.Contains(body, contain), check.IsTrue)
}

func (t *testMaster) TestJoinMember(c *check.C) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	// create a new cluster
	cfg1 := NewConfig()
	c.Assert(cfg1.Parse([]string{"-config=./dm-master.toml"}), check.IsNil)
	cfg1.Name = "dm-master-1"
	cfg1.DataDir = c.MkDir()
	cfg1.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfg1.PeerUrls = tempurl.Alloc()
	cfg1.AdvertisePeerUrls = cfg1.PeerUrls
	cfg1.InitialCluster = fmt.Sprintf("%s=%s", cfg1.Name, cfg1.AdvertisePeerUrls)

	s1 := NewServer(cfg1)
	c.Assert(s1.Start(ctx), check.IsNil)
	defer s1.Close()

	// wait the first one become the leader
	c.Assert(utils.WaitSomething(30, 10*time.Millisecond, func() bool {
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

	s2 := NewServer(cfg2)
	c.Assert(s2.Start(ctx), check.IsNil)
	defer s2.Close()

	client, err := etcdutil.CreateClient(strings.Split(cfg1.AdvertisePeerUrls, ","))
	c.Assert(err, check.IsNil)
	defer client.Close()

	// verify members
	listResp, err := etcdutil.ListMembers(client)
	c.Assert(err, check.IsNil)
	c.Assert(listResp.Members, check.HasLen, 2)
	names := make(map[string]struct{}, len(listResp.Members))
	for _, m := range listResp.Members {
		names[m.Name] = struct{}{}
	}
	_, ok := names[cfg1.Name]
	c.Assert(ok, check.IsTrue)
	_, ok = names[cfg2.Name]
	c.Assert(ok, check.IsTrue)

	// s1 is still the leader
	_, leaderID, err := s2.election.LeaderInfo(ctx)
	c.Assert(err, check.IsNil)
	c.Assert(leaderID, check.Equals, cfg1.Name)

	cancel()
}

func (t *testMaster) TestOperateMysqlWorker(c *check.C) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	// create a new cluster
	cfg1 := NewConfig()
	c.Assert(cfg1.Parse([]string{"-config=./dm-master.toml"}), check.IsNil)
	cfg1.Name = "dm-master-1"
	cfg1.DataDir = c.MkDir()
	cfg1.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfg1.PeerUrls = tempurl.Alloc()
	cfg1.AdvertisePeerUrls = cfg1.PeerUrls
	cfg1.InitialCluster = fmt.Sprintf("%s=%s", cfg1.Name, cfg1.AdvertisePeerUrls)

	s1 := NewServer(cfg1)
	c.Assert(s1.Start(ctx), check.IsNil)
	defer s1.Close()
	mysqlCfg := config.NewMysqlConfig()
	mysqlCfg.LoadFromFile("./dm-mysql.toml")
	task, err := mysqlCfg.Toml()
	c.Assert(err, check.IsNil)
	// wait for coordinator to start
	waitT := 0
	for !s1.coordinator.IsStarted() && waitT < 6 {
		time.Sleep(500 * time.Millisecond)
		waitT++
	}
	c.Assert(s1.coordinator.IsStarted(), check.IsTrue)

	req := &pb.MysqlWorkerRequest{Op: pb.WorkerOp_StartWorker, Config: task}
	resp, err := s1.OperateMysqlWorker(ctx, req)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.Equals, false)
	c.Assert(resp.Msg, check.Matches, "[\\s\\S]*Acquire worker failed. no free worker could start mysql task[\\s\\S]*")
	mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
	req.Op = pb.WorkerOp_UpdateConfig
	mockWorkerClient.EXPECT().OperateMysqlWorker(
		gomock.Any(),
		req,
	).Return(&pb.MysqlWorkerResponse{
		Result: true,
		Msg:    "",
	}, nil)
	req.Op = pb.WorkerOp_StopWorker
	mockWorkerClient.EXPECT().OperateMysqlWorker(
		gomock.Any(),
		req,
	).Return(&pb.MysqlWorkerResponse{
		Result: true,
		Msg:    "",
	}, nil)
	req.Op = pb.WorkerOp_StartWorker
	mockWorkerClient.EXPECT().OperateMysqlWorker(
		gomock.Any(),
		req,
	).Return(&pb.MysqlWorkerResponse{
		Result: true,
		Msg:    "",
	}, nil)
	s1.coordinator.AddWorker("", "localhost:10099", newMockRPCClient(mockWorkerClient))
	s1.coordinator.AddWorker("", "localhost:10099", nil)
	resp, err = s1.OperateMysqlWorker(ctx, req)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.Equals, true)
	resp, err = s1.OperateMysqlWorker(ctx, req)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.Equals, false)
	c.Assert(resp.Msg, check.Matches, ".*Create worker failed. worker has been started")
	mysqlCfg.SourceID = "no-exist-source"
	task2, err := mysqlCfg.Toml()
	c.Assert(err, check.IsNil)
	req.Config = task2
	req.Op = pb.WorkerOp_UpdateConfig
	resp, err = s1.OperateMysqlWorker(ctx, req)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.Equals, false)
	c.Assert(resp.Msg, check.Matches, ".*Update worker config failed. worker has not been started")
	req.Config = task
	resp, err = s1.OperateMysqlWorker(ctx, req)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.Equals, true)
	req.Op = pb.WorkerOp_StopWorker
	resp, err = s1.OperateMysqlWorker(ctx, req)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.Equals, true)
	cancel()
}
