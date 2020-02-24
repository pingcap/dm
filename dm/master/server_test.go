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
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/pd/pkg/tempurl"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/integration"

	"github.com/pingcap/dm/checker"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/master/scheduler"
	"github.com/pingcap/dm/dm/master/workerrpc"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/pbmock"
	"github.com/pingcap/dm/pkg/etcdutil"
	"github.com/pingcap/dm/pkg/ha"
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
	errCheckSyncConfig    = "(?m).*check sync config with error.*"
	errCheckSyncConfigReg = fmt.Sprintf("(?m).*%s.*", errCheckSyncConfig)
	testEtcdCluster       *integration.ClusterV3
	keepAliveTTL          = int64(1)
	etcdTestCli           *clientv3.Client
)

func TestMaster(t *testing.T) {
	log.InitLogger(&log.Config{})
	testEtcdCluster = integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer testEtcdCluster.Terminate(t)

	etcdTestCli = testEtcdCluster.RandClient()

	check.TestingT(t)
}

type testMaster struct {
	workerClients   map[string]workerrpc.Client
	saveMaxRetryNum int
}

var _ = check.Suite(&testMaster{})

func (t *testMaster) SetUpSuite(c *check.C) {
	err := log.InitLogger(&log.Config{})
	c.Assert(err, check.IsNil)
	t.workerClients = make(map[string]workerrpc.Client)
	clearEtcdEnv(c)
	t.saveMaxRetryNum = maxRetryNum
	maxRetryNum = 2
}

func (t *testMaster) TearDownSuite(c *check.C) {
	maxRetryNum = t.saveMaxRetryNum
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

func clearEtcdEnv(c *check.C) {
	c.Assert(ha.ClearTestInfoOperation(etcdTestCli), check.IsNil)
}

func clearSchedulerEnv(c *check.C, cancel context.CancelFunc, wg *sync.WaitGroup) {
	cancel()
	wg.Wait()
	clearEtcdEnv(c)
}

func makeNilWorkerClients(workers []string) map[string]workerrpc.Client {
	nilWorkerClients := make(map[string]workerrpc.Client, len(workers))
	for _, worker := range workers {
		nilWorkerClients[worker] = nil
	}
	return nilWorkerClients
}

func makeWorkerClientsForHandle(ctrl *gomock.Controller, taskName string, sources []string, workers []string, reqs ...interface{}) map[string]workerrpc.Client {
	workerClients := make(map[string]workerrpc.Client, len(workers))
	for i := range workers {
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		for _, req := range reqs {
			mockRevelantWorkerClient(mockWorkerClient, taskName, sources[i], req)
		}
		workerClients[workers[i]] = newMockRPCClient(mockWorkerClient)
	}
	return workerClients
}

func testDefaultMasterServer(c *check.C) *Server {
	cfg := NewConfig()
	err := cfg.Parse([]string{"-config=./dm-master.toml"})
	c.Assert(err, check.IsNil)
	cfg.DataDir = c.MkDir()
	server := NewServer(cfg)
	server.leader = oneselfLeader
	go server.ap.Start(context.Background())

	return server
}

func testMockScheduler(ctx context.Context, wg *sync.WaitGroup, c *check.C, sources, workers []string, password string, workerClients map[string]workerrpc.Client) (*scheduler.Scheduler, []context.CancelFunc) {
	logger := log.L()
	scheduler2 := scheduler.NewScheduler(&logger)
	err := scheduler2.Start(ctx, etcdTestCli)
	c.Assert(err, check.IsNil)
	cancels := make([]context.CancelFunc, 0, 2)
	for i := range workers {
		// add worker to scheduler's workers map
		name := workers[i]
		c.Assert(scheduler2.AddWorker(name, workers[i]), check.IsNil)
		scheduler2.SetWorkerClientForTest(name, workerClients[workers[i]])
		// operate mysql config on this worker
		cfg := config.NewSourceConfig()
		cfg.SourceID = sources[i]
		cfg.From.Password = password
		c.Assert(scheduler2.AddSourceCfg(*cfg), check.IsNil)
		wg.Add(1)
		ctx1, cancel1 := context.WithCancel(ctx)
		cancels = append(cancels, cancel1)
		go func(ctx context.Context, workerName string) {
			defer wg.Done()
			c.Assert(ha.KeepAlive(ctx, etcdTestCli, workerName, keepAliveTTL), check.IsNil)
		}(ctx1, name)
		c.Assert(utils.WaitSomething(30, 10*time.Millisecond, func() bool {
			w := scheduler2.GetWorkerBySource(sources[i])
			return w != nil && w.BaseInfo().Name == name
		}), check.IsTrue)
	}
	return scheduler2, cancels
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
		).Return(&pb.QueryStatusResponse{
			Result:       true,
			SourceStatus: &pb.SourceStatus{},
		}, nil)
		t.workerClients[deploy.Worker] = newMockRPCClient(mockWorkerClient)
	}
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	server.scheduler, _ = testMockScheduler(ctx, &wg, c, sources, workers, "", t.workerClients)
	resp, err := server.QueryStatus(context.Background(), &pb.QueryStatusListRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	clearSchedulerEnv(c, cancel, &wg)

	// query specified sources
	for _, deploy := range server.cfg.Deploy {
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		mockWorkerClient.EXPECT().QueryStatus(
			gomock.Any(),
			&pb.QueryStatusRequest{},
		).Return(&pb.QueryStatusResponse{
			Result:       true,
			SourceStatus: &pb.SourceStatus{},
		}, nil)
		t.workerClients[deploy.Worker] = newMockRPCClient(mockWorkerClient)
	}
	ctx, cancel = context.WithCancel(context.Background())
	server.scheduler, _ = testMockScheduler(ctx, &wg, c, sources, workers, "", t.workerClients)
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
	c.Assert(resp.Msg, check.Matches, "task .* has no source or not exist, can try `refresh-worker-tasks` cmd first")
	clearSchedulerEnv(c, cancel, &wg)
	// TODO: test query with correct task name, this needs to add task first
}

func (t *testMaster) TestCheckTask(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	server := testDefaultMasterServer(c)
	sources, workers := extractWorkerSource(server.cfg.Deploy)

	t.workerClients = makeNilWorkerClients(workers)
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	server.scheduler, _ = testMockScheduler(ctx, &wg, c, sources, workers, "", t.workerClients)
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
	clearSchedulerEnv(c, cancel, &wg)

	// simulate invalid password returned from scheduler, so cfg.SubTaskConfigs will fail
	ctx, cancel = context.WithCancel(context.Background())
	server.scheduler, _ = testMockScheduler(ctx, &wg, c, sources, workers, "invalid-encrypt-password", t.workerClients)
	resp, err = server.CheckTask(context.Background(), &pb.CheckTaskRequest{
		Task: taskConfig,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)
	clearSchedulerEnv(c, cancel, &wg)
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

	// test start task successfully
	var wg sync.WaitGroup
	// taskName is relative to taskConfig
	taskName := "test"
	ctx, cancel := context.WithCancel(context.Background())
	req := &pb.StartTaskRequest{
		Task:    taskConfig,
		Sources: sources,
	}
	server.scheduler, _ = testMockScheduler(ctx, &wg, c, sources, workers, "",
		makeWorkerClientsForHandle(ctrl, taskName, sources, workers, req))
	resp, err = server.StartTask(context.Background(), req)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	for _, source := range sources {
		t.subTaskStageMatch(c, server.scheduler, taskName, source, pb.Stage_Running)
		tcm, _, err2 := ha.GetSubTaskCfg(etcdTestCli, source, taskName, 0)
		c.Assert(err2, check.IsNil)
		c.Assert(tcm, check.HasKey, taskName)
		c.Assert(tcm[taskName].Name, check.Equals, taskName)
		c.Assert(tcm[taskName].SourceID, check.Equals, source)
	}

	// check start-task with an invalid source
	invalidSource := "invalid-source"
	resp, err = server.StartTask(context.Background(), &pb.StartTaskRequest{
		Task:    taskConfig,
		Sources: []string{invalidSource},
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)
	c.Assert(resp.Sources, check.HasLen, 1)
	c.Assert(resp.Sources[0].Result, check.IsFalse)
	c.Assert(resp.Sources[0].Source, check.Equals, invalidSource)

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
	clearSchedulerEnv(c, cancel, &wg)
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
		).Return(&pb.QueryErrorResponse{
			Result:      true,
			SourceError: &pb.SourceError{},
		}, nil)
		t.workerClients[deploy.Worker] = newMockRPCClient(mockWorkerClient)
	}
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	server.scheduler, _ = testMockScheduler(ctx, &wg, c, sources, workers, "", t.workerClients)
	resp, err := server.QueryError(context.Background(), &pb.QueryErrorListRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	clearSchedulerEnv(c, cancel, &wg)

	// query specified dm-worker[s]
	for _, deploy := range server.cfg.Deploy {
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		mockWorkerClient.EXPECT().QueryError(
			gomock.Any(),
			&pb.QueryErrorRequest{},
		).Return(&pb.QueryErrorResponse{
			Result:      true,
			SourceError: &pb.SourceError{},
		}, nil)
		t.workerClients[deploy.Worker] = newMockRPCClient(mockWorkerClient)
	}

	ctx, cancel = context.WithCancel(context.Background())
	server.scheduler, _ = testMockScheduler(ctx, &wg, c, sources, workers, "", t.workerClients)
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
	c.Assert(resp.Msg, check.Matches, "task .* has no source or not exist, can try `refresh-worker-tasks` cmd first")
	clearSchedulerEnv(c, cancel, &wg)
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

	// 1. start task
	taskName = "test"
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	startReq := &pb.StartTaskRequest{
		Task:    taskConfig,
		Sources: sources,
	}
	pauseReq := &pb.OperateTaskRequest{
		Op:   pauseOp,
		Name: taskName,
	}
	resumeReq := &pb.OperateTaskRequest{
		Op:   pb.TaskOp_Resume,
		Name: taskName,
	}
	stopReq1 := &pb.OperateTaskRequest{
		Op:      pb.TaskOp_Stop,
		Name:    taskName,
		Sources: []string{sources[0]},
	}
	stopReq2 := &pb.OperateTaskRequest{
		Op:   pb.TaskOp_Stop,
		Name: taskName,
	}
	sourceResps := []*pb.CommonWorkerResponse{{Result: true, Source: sources[0]}, {Result: true, Source: sources[1]}}
	server.scheduler, _ = testMockScheduler(ctx, &wg, c, sources, workers, "",
		makeWorkerClientsForHandle(ctrl, taskName, sources, workers, startReq, pauseReq, resumeReq, stopReq1, stopReq2))
	stResp, err := server.StartTask(context.Background(), startReq)
	c.Assert(err, check.IsNil)
	c.Assert(stResp.Result, check.IsTrue)
	for _, source := range sources {
		t.subTaskStageMatch(c, server.scheduler, taskName, source, pb.Stage_Running)
	}
	c.Assert(stResp.Sources, check.DeepEquals, sourceResps)
	// 2. pause task
	resp, err = server.OperateTask(context.Background(), pauseReq)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	for _, source := range sources {
		t.subTaskStageMatch(c, server.scheduler, taskName, source, pb.Stage_Paused)
	}
	c.Assert(resp.Sources, check.DeepEquals, sourceResps)
	// 3. resume task
	resp, err = server.OperateTask(context.Background(), resumeReq)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	for _, source := range sources {
		t.subTaskStageMatch(c, server.scheduler, taskName, source, pb.Stage_Running)
	}
	c.Assert(resp.Sources, check.DeepEquals, sourceResps)
	// 4. test stop task successfully, remove partial sources
	resp, err = server.OperateTask(context.Background(), stopReq1)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(server.getTaskResources(taskName), check.DeepEquals, []string{sources[1]})
	c.Assert(resp.Sources, check.DeepEquals, []*pb.CommonWorkerResponse{{Result: true, Source: sources[0]}})
	// 5. test stop task successfully, remove all workers
	resp, err = server.OperateTask(context.Background(), stopReq2)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(len(server.getTaskResources(taskName)), check.Equals, 0)
	c.Assert(resp.Sources, check.DeepEquals, []*pb.CommonWorkerResponse{{Result: true, Source: sources[1]}})
	clearSchedulerEnv(c, cancel, &wg)
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
			t.workerClients[deploy.Worker] = newMockRPCClient(mockWorkerClient)
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

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	// test PurgeWorkerRelay successfully
	mockPurgeRelay(true)
	server.scheduler, _ = testMockScheduler(ctx, &wg, c, sources, workers, "", t.workerClients)
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
	clearSchedulerEnv(c, cancel, &wg)

	ctx, cancel = context.WithCancel(context.Background())
	// test PurgeWorkerRelay with error response
	mockPurgeRelay(false)
	server.scheduler, _ = testMockScheduler(ctx, &wg, c, sources, workers, "", t.workerClients)
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
	clearSchedulerEnv(c, cancel, &wg)
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
			t.workerClients[deploy.Worker] = newMockRPCClient(mockWorkerClient)
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

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	// test SwitchWorkerRelayMaster successfully
	mockSwitchRelayMaster(true)
	server.scheduler, _ = testMockScheduler(ctx, &wg, c, sources, workers, "", t.workerClients)
	resp, err = server.SwitchWorkerRelayMaster(context.Background(), &pb.SwitchWorkerRelayMasterRequest{
		Sources: sources,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Sources, check.HasLen, 2)
	for _, w := range resp.Sources {
		c.Assert(w.Result, check.IsTrue)
	}
	clearSchedulerEnv(c, cancel, &wg)

	ctx, cancel = context.WithCancel(context.Background())
	// test SwitchWorkerRelayMaster with error response
	mockSwitchRelayMaster(false)
	server.scheduler, _ = testMockScheduler(ctx, &wg, c, sources, workers, "", t.workerClients)
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
	clearSchedulerEnv(c, cancel, &wg)
}

func (t *testMaster) TestOperateWorkerRelayTask(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	server := testDefaultMasterServer(c)
	sources, workers := extractWorkerSource(server.cfg.Deploy)
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	pauseReq := &pb.OperateWorkerRelayRequest{
		Sources: sources,
		Op:      pb.RelayOp_PauseRelay,
	}
	resumeReq := &pb.OperateWorkerRelayRequest{
		Sources: sources,
		Op:      pb.RelayOp_ResumeRelay,
	}
	server.scheduler, _ = testMockScheduler(ctx, &wg, c, sources, workers, "",
		makeWorkerClientsForHandle(ctrl, "", sources, workers, pauseReq, resumeReq))

	// test OperateWorkerRelayTask with invalid dm-worker[s]
	resp, err := server.OperateWorkerRelayTask(context.Background(), &pb.OperateWorkerRelayRequest{
		Sources: []string{"invalid-source1", "invalid-source2"},
		Op:      pb.RelayOp_PauseRelay,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)
	c.Assert(resp.Msg, check.Matches, `[\s\S]*need to update expectant relay stage not exist[\s\S]*`)

	sourceResps := []*pb.CommonWorkerResponse{{Result: true, Source: sources[0]}, {Result: true, Source: sources[1]}}
	// 1. test pause-relay successfully
	resp, err = server.OperateWorkerRelayTask(context.Background(), pauseReq)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	for _, source := range sources {
		t.relayStageMatch(c, server.scheduler, source, pb.Stage_Paused)
	}
	c.Assert(resp.Sources, check.DeepEquals, sourceResps)
	// 2. test resume-relay successfully
	resp, err = server.OperateWorkerRelayTask(context.Background(), resumeReq)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	for _, source := range sources {
		t.relayStageMatch(c, server.scheduler, source, pb.Stage_Running)
	}
	c.Assert(resp.Sources, check.DeepEquals, sourceResps)
	clearSchedulerEnv(c, cancel, &wg)
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
	//t.testHTTPInterface(c, fmt.Sprintf("http://%s/apis/v1alpha1/status/test-task", cfg.MasterAddr), []byte("task test-task has no source or not exist"))

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

func (t *testMaster) TestOperateSource(c *check.C) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	// create a new cluster
	cfg1 := NewConfig()
	c.Assert(cfg1.Parse([]string{"-config=./dm-master.toml"}), check.IsNil)
	cfg1.Name = "dm-master-1"
	cfg1.DataDir = c.MkDir()
	cfg1.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfg1.AdvertiseAddr = tempurl.Alloc()[len("http://"):]
	cfg1.PeerUrls = tempurl.Alloc()
	cfg1.AdvertisePeerUrls = cfg1.PeerUrls
	cfg1.InitialCluster = fmt.Sprintf("%s=%s", cfg1.Name, cfg1.AdvertisePeerUrls)

	s1 := NewServer(cfg1)
	s1.leader = oneselfLeader
	c.Assert(s1.Start(ctx), check.IsNil)
	defer s1.Close()
	mysqlCfg := config.NewSourceConfig()
	mysqlCfg.LoadFromFile("./source.toml")
	task, err := mysqlCfg.Toml()
	c.Assert(err, check.IsNil)
	sourceID := mysqlCfg.SourceID
	// 1. wait for scheduler to start
	time.Sleep(3 * time.Second)

	// 2. try to add a new mysql source
	req := &pb.OperateSourceRequest{Op: pb.SourceOp_StartSource, Config: task}
	resp, err := s1.OperateSource(ctx, req)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.Equals, true)
	c.Assert(resp.Sources, check.DeepEquals, []*pb.CommonWorkerResponse{{
		Result: true,
		Msg:    "source is added but there is no free worker to bound",
		Source: sourceID,
	}})
	unBoundSources := s1.scheduler.UnboundSources()
	c.Assert(unBoundSources, check.HasLen, 1)
	c.Assert(unBoundSources[0], check.Equals, sourceID)

	// 3. try to stop a non-exist-source
	req.Op = pb.SourceOp_StopSource
	mysqlCfg.SourceID = "not-exist-source"
	task2, err := mysqlCfg.Toml()
	c.Assert(err, check.IsNil)
	req.Config = task2
	resp, err = s1.OperateSource(ctx, req)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.Equals, false)
	c.Assert(resp.Msg, check.Matches, `[\s\S]*source config with ID `+mysqlCfg.SourceID+` not exists[\s\S]*`)

	// 4. start a new worker, the unbounded source should be bounded
	var wg sync.WaitGroup
	ctx1, cancel1 := context.WithCancel(ctx)
	workerName := "worker1"
	defer func() {
		clearSchedulerEnv(c, cancel1, &wg)
	}()
	c.Assert(s1.scheduler.AddWorker(workerName, "172.16.10.72:8262"), check.IsNil)
	wg.Add(1)
	go func(ctx context.Context, workerName string) {
		defer wg.Done()
		c.Assert(ha.KeepAlive(ctx, s1.etcdClient, workerName, keepAliveTTL), check.IsNil)
	}(ctx1, workerName)
	c.Assert(utils.WaitSomething(30, 10*time.Millisecond, func() bool {
		w := s1.scheduler.GetWorkerBySource(sourceID)
		return w != nil && w.BaseInfo().Name == workerName
	}), check.IsTrue)

	// 5. stop this source
	req.Config = task
	req.Op = pb.SourceOp_StopSource
	mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
	mockRevelantWorkerClient(mockWorkerClient, "", sourceID, req)
	s1.scheduler.SetWorkerClientForTest(workerName, newMockRPCClient(mockWorkerClient))
	resp, err = s1.OperateSource(ctx, req)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.Equals, true)
	c.Assert(resp.Sources, check.DeepEquals, []*pb.CommonWorkerResponse{{
		Result: true,
		Source: sourceID,
	}})
	cfg, _, err := ha.GetSourceCfg(etcdTestCli, sourceID, 0)
	c.Assert(err, check.IsNil)
	var emptySourceCfg config.SourceConfig
	c.Assert(cfg, check.DeepEquals, emptySourceCfg)
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
		c.Assert(res.Msg, check.Matches, `[\s\S]*dm-worker with name `+req2.Name+` not exists[\s\S]*`)
	}
	{
		req2.Name = "xixi"
		res, err := s1.OfflineWorker(ectx, req2)
		c.Assert(err, check.IsNil)
		c.Assert(res.Result, check.IsTrue)
	}
}

func (t *testMaster) relayStageMatch(c *check.C, s *scheduler.Scheduler, source string, expectStage pb.Stage) {
	stage := ha.NewRelayStage(expectStage, source)
	c.Assert(s.GetExpectRelayStage(source), check.DeepEquals, stage)

	eStage, _, err := ha.GetRelayStage(etcdTestCli, source)
	c.Assert(err, check.IsNil)
	switch expectStage {
	case pb.Stage_Running, pb.Stage_Paused:
		c.Assert(eStage, check.DeepEquals, stage)
	}
}

func (t *testMaster) subTaskStageMatch(c *check.C, s *scheduler.Scheduler, task, source string, expectStage pb.Stage) {
	stage := ha.NewSubTaskStage(expectStage, source, task)
	c.Assert(s.GetExpectSubTaskStage(task, source), check.DeepEquals, stage)

	eStageM, _, err := ha.GetSubTaskStage(etcdTestCli, source, task)
	c.Assert(err, check.IsNil)
	switch expectStage {
	case pb.Stage_Running, pb.Stage_Paused:
		c.Assert(eStageM, check.HasLen, 1)
		c.Assert(eStageM[task], check.DeepEquals, stage)
	default:
		c.Assert(eStageM, check.HasLen, 0)
	}
}

func mockRevelantWorkerClient(mockWorkerClient *pbmock.MockWorkerClient, taskName, sourceID string, masterReq interface{}) {
	var expect pb.Stage
	switch masterReq.(type) {
	case *pb.OperateSourceRequest:
		req := masterReq.(*pb.OperateSourceRequest)
		switch req.Op {
		case pb.SourceOp_StartSource, pb.SourceOp_UpdateSource:
			expect = pb.Stage_Running
		case pb.SourceOp_StopSource:
			expect = pb.Stage_Stopped
		}
	case *pb.StartTaskRequest, *pb.UpdateTaskRequest:
		expect = pb.Stage_Running
	case *pb.OperateTaskRequest:
		req := masterReq.(*pb.OperateTaskRequest)
		switch req.Op {
		case pb.TaskOp_Resume:
			expect = pb.Stage_Running
		case pb.TaskOp_Pause:
			expect = pb.Stage_Paused
		case pb.TaskOp_Stop:
			expect = pb.Stage_Stopped
		}
	case *pb.OperateWorkerRelayRequest:
		req := masterReq.(*pb.OperateWorkerRelayRequest)
		switch req.Op {
		case pb.RelayOp_ResumeRelay:
			expect = pb.Stage_Running
		case pb.RelayOp_PauseRelay:
			expect = pb.Stage_Paused
		case pb.RelayOp_StopRelay:
			expect = pb.Stage_Stopped
		}
	}
	queryResp := &pb.QueryStatusResponse{
		Result:       true,
		SourceStatus: &pb.SourceStatus{},
	}

	switch masterReq.(type) {
	case *pb.OperateSourceRequest:
		switch expect {
		case pb.Stage_Running:
			queryResp.SourceStatus = &pb.SourceStatus{Source: sourceID}
		case pb.Stage_Stopped:
			queryResp.SourceStatus = &pb.SourceStatus{Source: ""}
		}
	case *pb.StartTaskRequest, *pb.UpdateTaskRequest, *pb.OperateTaskRequest:
		queryResp.SubTaskStatus = []*pb.SubTaskStatus{{}}
		if expect == pb.Stage_Stopped {
			queryResp.SubTaskStatus[0].Status = &pb.SubTaskStatus_Msg{
				Msg: fmt.Sprintf("no sub task with name %s has started", taskName),
			}
		} else {
			queryResp.SubTaskStatus[0].Name = taskName
			queryResp.SubTaskStatus[0].Stage = expect
		}
	case *pb.OperateWorkerRelayRequest:
		queryResp.SourceStatus = &pb.SourceStatus{RelayStatus: &pb.RelayStatus{Stage: expect}}
	}

	mockWorkerClient.EXPECT().QueryStatus(
		gomock.Any(),
		&pb.QueryStatusRequest{
			Name: taskName,
		},
	).Return(queryResp, nil).MaxTimes(maxRetryNum)
}
