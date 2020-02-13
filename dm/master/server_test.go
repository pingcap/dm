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
	"io/ioutil"
	"net/http"
	"strings"
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
		workers = append(workers, deploy.Worker)
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
	sources, workers := extractWorkerSource(server.cfg.Deploy)
	nilWorkerClients := make(map[string]workerrpc.Client)
	for _, worker := range workers {
		nilWorkerClients[worker] = nil
	}
	server.coordinator = testMockCoordinator(c, sources, workers, "", nilWorkerClients)

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

func testMockCoordinator(c *check.C, sources, workers []string, password string, workerClients map[string]workerrpc.Client) *coordinator.Coordinator {
	coordinator2 := coordinator.NewCoordinator()
	err := coordinator2.Start(context.Background(), testEtcdCluster.RandClient())
	c.Assert(err, check.IsNil)
	for i := range workers {
		// add worker to coordinator's workers map
		coordinator2.AddWorker("worker"+string(i), workers[i], workerClients[workers[i]])
		// set this worker's status to workerFree
		coordinator2.AddWorker("worker"+string(i), workers[i], nil)
		// operate mysql config on this worker
		cfg := &config.MysqlConfig{SourceID: sources[i], From: config.DBConfig{Password: password}}
		w, err := coordinator2.AcquireWorkerForSource(cfg.SourceID)
		c.Assert(err, check.IsNil)
		coordinator2.HandleStartedWorker(w, cfg, true)
	}
	return coordinator2
}

func testMockStartTask(c *check.C, server *Server, ctrl *gomock.Controller, workerCfg map[string]*config.SubTaskConfig, rpcSuccess bool) {
	for _, deploy := range server.cfg.Deploy {
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)

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
					Source: deploy.Source,
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
	server.coordinator = testMockCoordinator(c, sources, workers, "", server.workerClients)
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
	server.coordinator = testMockCoordinator(c, sources, workers, "", server.workerClients)
	resp, err = server.QueryStatus(context.Background(), &pb.QueryStatusListRequest{
		Sources: sources,
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

func (t *testMaster) TestCheckTask(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	server := testDefaultMasterServer(c)
	sources, workers := extractWorkerSource(server.cfg.Deploy)

	server.coordinator = testMockCoordinator(c, sources, workers, "", server.workerClients)
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

	// simulate invalid password returned from coordinator, so cfg.SubTaskConfigs will fail
	server.coordinator = testMockCoordinator(c, sources, workers, "invalid-encrypt-password", server.workerClients)
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
	server.coordinator = testMockCoordinator(c, sources, workers, "", server.workerClients)
	resp, err = server.StartTask(context.Background(), &pb.StartTaskRequest{
		Task:    taskConfig,
		Sources: sources,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)

	// check start-task with an invalid source
	invalidSource := "invalid-source"
	resp, err = server.StartTask(context.Background(), &pb.StartTaskRequest{
		Task:    taskConfig,
		Sources: []string{invalidSource},
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Sources, check.HasLen, 1)
	c.Assert(resp.Sources[0].Result, check.IsFalse)
	c.Assert(resp.Sources[0].Source, check.Equals, invalidSource)

	// test start sub task request to worker returns error
	testMockStartTask(c, server, ctrl, workerCfg, false)
	server.coordinator = testMockCoordinator(c, sources, workers, "", server.workerClients)
	resp, err = server.StartTask(context.Background(), &pb.StartTaskRequest{
		Task: taskConfig,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Sources, check.HasLen, 2)
	for _, workerResp := range resp.Sources {
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
	resp, err = server.StartTask(context.Background(), &pb.StartTaskRequest{
		Task:    taskConfig,
		Sources: sources,
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
	server.coordinator = testMockCoordinator(c, sources, workers, "", server.workerClients)
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
	server.coordinator = testMockCoordinator(c, sources, workers, "", server.workerClients)
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
	c.Assert(resp.Msg, check.Equals, fmt.Sprintf("task %s has no source or not exist, please check the task name and status", taskName))

	// test operate-task while worker clients not found
	server.taskSources[taskName] = sources
	resp, err = server.OperateTask(context.Background(), &pb.OperateTaskRequest{
		Op:   pauseOp,
		Name: taskName,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Sources, check.HasLen, 2)
	for _, subtaskResp := range resp.Sources {
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
	server.coordinator = testMockCoordinator(c, sources, workers, "", server.workerClients)
	resp, err = server.OperateTask(context.Background(), &pb.OperateTaskRequest{
		Op:   pauseOp,
		Name: taskName,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Op, check.Equals, pauseOp)
	c.Assert(resp.Sources, check.HasLen, 2)
	for _, subtaskResp := range resp.Sources {
		c.Assert(subtaskResp.Op, check.Equals, pauseOp)
		c.Assert(subtaskResp.Result, check.IsTrue)
	}

	// test operate sub task to worker returns error
	server.taskSources[taskName] = sources
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
	server.coordinator = testMockCoordinator(c, sources, workers, "", server.workerClients)
	resp, err = server.OperateTask(context.Background(), &pb.OperateTaskRequest{
		Op:      pb.TaskOp_Pause,
		Name:    taskName,
		Sources: sources,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Sources, check.HasLen, 2)
	for _, subtaskResp := range resp.Sources {
		c.Assert(subtaskResp.Op, check.Equals, pauseOp)
		c.Assert(subtaskResp.Msg, check.Matches, errGRPCFailedReg)
	}

	// test stop task successfully, remove partial workers
	server.taskSources[taskName] = sources
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
	server.coordinator = testMockCoordinator(c, sources, workers, "", server.workerClients)
	resp, err = server.OperateTask(context.Background(), &pb.OperateTaskRequest{
		Op:      pb.TaskOp_Stop,
		Name:    taskName,
		Sources: []string{sources[0]},
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Sources, check.HasLen, 1)
	c.Assert(resp.Sources[0].Result, check.IsTrue)
	c.Assert(server.taskSources, check.HasKey, taskName)
	c.Assert(server.taskSources[taskName], check.DeepEquals, []string{sources[1]})

	// test stop task successfully, remove all workers
	server.taskSources[taskName] = sources
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
	server.coordinator = testMockCoordinator(c, sources, workers, "", server.workerClients)
	resp, err = server.OperateTask(context.Background(), &pb.OperateTaskRequest{
		Op:   pb.TaskOp_Stop,
		Name: taskName,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Sources, check.HasLen, 2)
	for _, subtaskResp := range resp.Sources {
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
		for _, deploy := range server.cfg.Deploy {
			mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)

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
						Source: deploy.Source,
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
	server.coordinator = testMockCoordinator(c, sources, workers, "", server.workerClients)
	resp, err = server.UpdateTask(context.Background(), &pb.UpdateTaskRequest{
		Task:    taskConfig,
		Sources: sources,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)

	// check update-task with an invalid source
	invalidSource := "invalid-source"
	server.coordinator = testMockCoordinator(c, sources, workers, "", server.workerClients)
	resp, err = server.UpdateTask(context.Background(), &pb.UpdateTaskRequest{
		Task:    taskConfig,
		Sources: []string{invalidSource},
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Sources, check.HasLen, 1)
	c.Assert(resp.Sources[0].Result, check.IsFalse)
	c.Assert(resp.Sources[0].Source, check.Equals, invalidSource)

	// test update sub task request to worker returns error
	mockUpdateTask(false)
	server.coordinator = testMockCoordinator(c, sources, workers, "", server.workerClients)
	resp, err = server.UpdateTask(context.Background(), &pb.UpdateTaskRequest{
		Task: taskConfig,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Sources, check.HasLen, 2)
	for _, workerResp := range resp.Sources {
		c.Assert(workerResp.Result, check.IsFalse)
		c.Assert(workerResp.Msg, check.Matches, errGRPCFailedReg)
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
						Source: deploy.Source,
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
		Sources:  []string{"invalid-source1", "invalid-source2"},
		Time:     now,
		Filename: filename,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Sources, check.HasLen, 2)
	for _, w := range resp.Sources {
		c.Assert(w.Result, check.IsFalse)
		c.Assert(w.Msg, check.Matches, ".*relevant worker-client not found")
	}

	// test PurgeWorkerRelay successfully
	mockPurgeRelay(true)
	server.coordinator = testMockCoordinator(c, sources, workers, "", server.workerClients)
	resp, err = server.PurgeWorkerRelay(context.Background(), &pb.PurgeWorkerRelayRequest{
		Sources:  sources,
		Time:     now,
		Filename: filename,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Sources, check.HasLen, 2)
	for _, w := range resp.Sources {
		c.Assert(w.Result, check.IsTrue)
	}

	// test PurgeWorkerRelay with error response
	mockPurgeRelay(false)
	server.coordinator = testMockCoordinator(c, sources, workers, "", server.workerClients)
	resp, err = server.PurgeWorkerRelay(context.Background(), &pb.PurgeWorkerRelayRequest{
		Sources:  sources,
		Time:     now,
		Filename: filename,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Sources, check.HasLen, 2)
	for _, w := range resp.Sources {
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
						Source: deploy.Source,
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
		Sources: []string{"invalid-source1", "invalid-source2"},
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Sources, check.HasLen, 2)
	for _, w := range resp.Sources {
		c.Assert(w.Result, check.IsFalse)
		c.Assert(w.Msg, check.Matches, "(?m).*relevant worker-client not found.*")
	}

	// test SwitchWorkerRelayMaster successfully
	mockSwitchRelayMaster(true)
	server.coordinator = testMockCoordinator(c, sources, workers, "", server.workerClients)
	resp, err = server.SwitchWorkerRelayMaster(context.Background(), &pb.SwitchWorkerRelayMasterRequest{
		Sources: sources,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Sources, check.HasLen, 2)
	for _, w := range resp.Sources {
		c.Assert(w.Result, check.IsTrue)
	}

	// test SwitchWorkerRelayMaster with error response
	mockSwitchRelayMaster(false)
	server.coordinator = testMockCoordinator(c, sources, workers, "", server.workerClients)
	resp, err = server.SwitchWorkerRelayMaster(context.Background(), &pb.SwitchWorkerRelayMasterRequest{
		Sources: sources,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Sources, check.HasLen, 2)
	for _, w := range resp.Sources {
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
						Source: deploy.Source,
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
		Sources: []string{"invalid-source1", "invalid-source2"},
		Op:      pb.RelayOp_PauseRelay,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Sources, check.HasLen, 2)
	for _, w := range resp.Sources {
		c.Assert(w.Result, check.IsFalse)
		c.Assert(w.Msg, check.Matches, ".*relevant worker-client not found")
	}

	// test OperateWorkerRelayTask successfully
	mockOperateRelay(true)
	server.coordinator = testMockCoordinator(c, sources, workers, "", server.workerClients)
	resp, err = server.OperateWorkerRelayTask(context.Background(), &pb.OperateWorkerRelayRequest{
		Sources: sources,
		Op:      pb.RelayOp_PauseRelay,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Sources, check.HasLen, 2)
	for _, w := range resp.Sources {
		c.Assert(w.Result, check.IsTrue)
		c.Assert(w.Op, check.Equals, pb.RelayOp_PauseRelay)
	}

	// test OperateWorkerRelayTask with error response
	mockOperateRelay(false)
	server.coordinator = testMockCoordinator(c, sources, workers, "", server.workerClients)
	resp, err = server.OperateWorkerRelayTask(context.Background(), &pb.OperateWorkerRelayRequest{
		Sources: sources,
		Op:      pb.RelayOp_PauseRelay,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Sources, check.HasLen, 2)
	for _, w := range resp.Sources {
		c.Assert(w.Result, check.IsFalse)
		c.Assert(w.Msg, check.Matches, errGRPCFailedReg)
	}
}

func (t *testMaster) TestServer(c *check.C) {
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-master.toml"}), check.IsNil)
	cfg.PeerUrls = "http://127.0.0.1:8294"
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
	_, leaderID, _, err := s2.election.LeaderInfo(ctx)
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
	c.Assert(utils.WaitSomething(6, 500*time.Millisecond, func() bool {
		return s1.coordinator.IsStarted()
	}), check.IsTrue)

	req := &pb.MysqlWorkerRequest{Op: pb.WorkerOp_StartWorker, Config: task}
	resp, err := s1.OperateMysqlWorker(ctx, req)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.Equals, false)
	c.Assert(resp.Msg, check.Matches, "[\\s\\S]*acquire worker failed. no free worker could start mysql task[\\s\\S]*")
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

func (t *testMaster) TestOfflineWorker(c *check.C) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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

	s1 := NewServer(cfg1)
	c.Assert(s1.Start(ctx), check.IsNil)
	defer s1.Close()

	time.Sleep(time.Second * 2)

	ectx, canc := context.WithTimeout(ctx, time.Second)
	defer canc()
	req1 := &pb.RegisterWorkerRequest{
		Name:    "xixi",
		Address: "127.0.0.1:1000",
	}
	resp, err := s1.RegisterWorker(ectx, req1)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)

	req2 := &pb.OfflineWorkerRequest{
		Name:    "haha",
		Address: "127.0.0.1:1000",
	}
	{
		res, err := s1.OfflineWorker(ectx, req2)
		c.Assert(err, check.IsNil)
		c.Assert(res.Result, check.IsFalse)
		c.Assert(res.Msg, check.Matches, ".*delete from etcd failed, please check whether the name and address of worker match.*")
	}
	{
		req2.Name = "xixi"
		res, err := s1.OfflineWorker(ectx, req2)
		c.Assert(err, check.IsNil)
		c.Assert(res.Result, check.IsTrue)
	}
}
