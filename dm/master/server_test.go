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
	"database/sql"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	tiddl "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/sessionctx"
	tidbmock "github.com/pingcap/tidb/util/mock"
	"github.com/tikv/pd/pkg/tempurl"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/integration"

	"github.com/pingcap/dm/checker"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/master/scheduler"
	"github.com/pingcap/dm/dm/master/shardddl"
	"github.com/pingcap/dm/dm/master/workerrpc"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/pbmock"
	"github.com/pingcap/dm/pkg/conn"
	"github.com/pingcap/dm/pkg/cputil"
	"github.com/pingcap/dm/pkg/etcdutil"
	"github.com/pingcap/dm/pkg/ha"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/shardddl/optimism"
	"github.com/pingcap/dm/pkg/shardddl/pessimism"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

// use task config from integration test `sharding`
var taskConfig = `---
name: test
task-mode: all
is-sharding: true
shard-mode: ""
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

  - source-id: "mysql-replica-02"
    block-allow-list:  "instance"
    route-rules: ["sharding-route-rules-table", "sharding-route-rules-schema"]
    column-mapping-rules: ["instance-2"]
    mydumper-config-name: "global"
    loader-config-name: "global"
    syncer-config-name: "global"

block-allow-list:
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
	keepAliveTTL          = int64(10)
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
	scheduler2 := scheduler.NewScheduler(&logger, config.Security{})
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
		c.Assert(scheduler2.AddSourceCfg(*cfg), check.IsNil, check.Commentf("all sources: %v", sources))
		wg.Add(1)
		ctx1, cancel1 := context.WithCancel(ctx)
		cancels = append(cancels, cancel1)
		go func(ctx context.Context, workerName string) {
			defer wg.Done()
			c.Assert(ha.KeepAlive(ctx, etcdTestCli, workerName, keepAliveTTL), check.IsNil)
		}(ctx1, name)
		c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
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

	// simulate invalid password returned from scheduler, but config was supported plaintext mysql password, so cfg.SubTaskConfigs will success
	ctx, cancel = context.WithCancel(context.Background())
	server.scheduler, _ = testMockScheduler(ctx, &wg, c, sources, workers, "invalid-encrypt-password", t.workerClients)
	resp, err = server.CheckTask(context.Background(), &pb.CheckTaskRequest{
		Task: taskConfig,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
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

type mockDBProvider struct {
	db *sql.DB
}

// Apply will build BaseDB with DBConfig
func (d *mockDBProvider) Apply(config config.DBConfig) (*conn.BaseDB, error) {
	return conn.NewBaseDB(d.db, func() {}), nil
}

func (t *testMaster) TestStartTaskWithRemoveMeta(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	server := testDefaultMasterServer(c)
	sources, workers := extractWorkerSource(server.cfg.Deploy)
	server.etcdClient = etcdTestCli

	// test start task successfully
	var wg sync.WaitGroup
	// taskName is relative to taskConfig
	cfg := config.NewTaskConfig()
	err := cfg.Decode(taskConfig)
	c.Assert(err, check.IsNil)
	taskName := cfg.Name
	ctx, cancel := context.WithCancel(context.Background())
	logger := log.L()

	// test remove meta with pessimist
	cfg.ShardMode = config.ShardPessimistic
	req := &pb.StartTaskRequest{
		Task:       strings.ReplaceAll(taskConfig, `shard-mode: ""`, fmt.Sprintf(`shard-mode: "%s"`, cfg.ShardMode)),
		Sources:    sources,
		RemoveMeta: true,
	}
	server.scheduler, _ = testMockScheduler(ctx, &wg, c, sources, workers, "",
		makeWorkerClientsForHandle(ctrl, taskName, sources, workers, req))
	server.pessimist = shardddl.NewPessimist(&logger, func(task string) []string { return sources })
	server.optimist = shardddl.NewOptimist(&logger)

	var (
		DDLs          = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		schema, table = "foo", "bar"
		ID            = fmt.Sprintf("%s-`%s`.`%s`", taskName, schema, table)
		i11           = pessimism.NewInfo(taskName, sources[0], schema, table, DDLs)
		op2           = pessimism.NewOperation(ID, taskName, sources[0], DDLs, true, false)
	)
	_, err = pessimism.PutInfo(etcdTestCli, i11)
	c.Assert(err, check.IsNil)
	_, succ, err := pessimism.PutOperations(etcdTestCli, false, op2)
	c.Assert(succ, check.IsTrue)
	c.Assert(err, check.IsNil)

	c.Assert(server.pessimist.Start(ctx, etcdTestCli), check.IsNil)
	c.Assert(server.optimist.Start(ctx, etcdTestCli), check.IsNil)

	db, mock, err := sqlmock.New()
	c.Assert(err, check.IsNil)
	conn.DefaultDBProvider = &mockDBProvider{db: db}
	defer func() {
		conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	}()
	mock.ExpectBegin()
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.LoaderCheckpoint(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.SyncerCheckpoint(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.SyncerShardMeta(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.SyncerOnlineDDL(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	c.Assert(len(server.pessimist.Locks()), check.Greater, 0)

	resp, err := server.StartTask(context.Background(), req)
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Microsecond)
		// start another same task at the same time, should get err
		resp1, err1 := server.StartTask(context.Background(), req)
		c.Assert(err1, check.IsNil)
		c.Assert(resp1.Result, check.IsFalse)
		c.Assert(resp1.Msg, check.Equals, terror.Annotate(terror.ErrSchedulerSubTaskExist.Generate(cfg.Name, sources),
			"while remove-meta is true").Error())
	}()
	c.Assert(err, check.IsNil)
	if !resp.Result {
		c.Errorf("start task failed: %s", resp.Msg)
	}
	for _, source := range sources {
		t.subTaskStageMatch(c, server.scheduler, taskName, source, pb.Stage_Running)
		tcm, _, err2 := ha.GetSubTaskCfg(etcdTestCli, source, taskName, 0)
		c.Assert(err2, check.IsNil)
		c.Assert(tcm, check.HasKey, taskName)
		c.Assert(tcm[taskName].Name, check.Equals, taskName)
		c.Assert(tcm[taskName].SourceID, check.Equals, source)
	}

	c.Assert(server.pessimist.Locks(), check.HasLen, 0)
	if err = mock.ExpectationsWereMet(); err != nil {
		c.Errorf("db unfulfilled expectations: %s", err)
	}
	ifm, _, err := pessimism.GetAllInfo(etcdTestCli)
	c.Assert(err, check.IsNil)
	c.Assert(ifm, check.HasLen, 0)
	opm, _, err := pessimism.GetAllOperations(etcdTestCli)
	c.Assert(err, check.IsNil)
	c.Assert(opm, check.HasLen, 0)
	clearSchedulerEnv(c, cancel, &wg)

	// test remove meta with optimist
	ctx, cancel = context.WithCancel(context.Background())
	cfg.ShardMode = config.ShardOptimistic
	req = &pb.StartTaskRequest{
		Task:       strings.ReplaceAll(taskConfig, `shard-mode: ""`, fmt.Sprintf(`shard-mode: "%s"`, cfg.ShardMode)),
		Sources:    sources,
		RemoveMeta: true,
	}
	server.scheduler, _ = testMockScheduler(ctx, &wg, c, sources, workers, "",
		makeWorkerClientsForHandle(ctrl, taskName, sources, workers, req))
	server.pessimist = shardddl.NewPessimist(&logger, func(task string) []string { return sources })
	server.optimist = shardddl.NewOptimist(&logger)

	var (
		p           = parser.New()
		se          = tidbmock.NewContext()
		tblID int64 = 111

		st1      = optimism.NewSourceTables(taskName, sources[0])
		DDLs1    = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		tiBefore = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		tiAfter1 = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT)`)
		info1    = optimism.NewInfo(taskName, sources[0], "foo-1", "bar-1", schema, table, DDLs1, tiBefore, tiAfter1)
		op1      = optimism.NewOperation(ID, taskName, sources[0], info1.UpSchema, info1.UpTable, DDLs1, optimism.ConflictNone, false)
	)

	_, err = optimism.PutSourceTables(etcdTestCli, st1)
	c.Assert(err, check.IsNil)
	_, err = optimism.PutInfo(etcdTestCli, info1)
	c.Assert(err, check.IsNil)
	_, succ, err = optimism.PutOperation(etcdTestCli, false, op1)
	c.Assert(succ, check.IsTrue)
	c.Assert(err, check.IsNil)

	err = server.pessimist.Start(ctx, etcdTestCli)
	c.Assert(err, check.IsNil)
	err = server.optimist.Start(ctx, etcdTestCli)
	c.Assert(err, check.IsNil)

	db, mock, err = sqlmock.New()
	c.Assert(err, check.IsNil)
	conn.DefaultDBProvider = &mockDBProvider{db: db}
	mock.ExpectBegin()
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.LoaderCheckpoint(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.SyncerCheckpoint(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.SyncerShardMeta(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", cfg.MetaSchema, cputil.SyncerOnlineDDL(cfg.Name))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	c.Assert(len(server.optimist.Locks()), check.Greater, 0)

	resp, err = server.StartTask(context.Background(), req)
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Microsecond)
		// start another same task at the same time, should get err
		resp1, err1 := server.StartTask(context.Background(), req)
		c.Assert(err1, check.IsNil)
		c.Assert(resp1.Result, check.IsFalse)
		c.Assert(resp1.Msg, check.Equals, terror.Annotate(terror.ErrSchedulerSubTaskExist.Generate(cfg.Name, sources),
			"while remove-meta is true").Error())
	}()
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

	c.Assert(server.optimist.Locks(), check.HasLen, 0)
	if err = mock.ExpectationsWereMet(); err != nil {
		c.Errorf("db unfulfilled expectations: %s", err)
	}
	ifm2, _, err := optimism.GetAllInfo(etcdTestCli)
	c.Assert(err, check.IsNil)
	c.Assert(ifm2, check.HasLen, 0)
	opm2, _, err := optimism.GetAllOperations(etcdTestCli)
	c.Assert(err, check.IsNil)
	c.Assert(opm2, check.HasLen, 0)
	tbm, _, err := optimism.GetAllSourceTables(etcdTestCli)
	c.Assert(err, check.IsNil)
	c.Assert(tbm, check.HasLen, 0)

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
	c.Assert(err.Error(), check.Matches, ".*bind: address already in use.*")

	// close
	cancel()
	s.Close()

	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
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

	s2 := NewServer(cfg2)
	c.Assert(s2.Start(ctx), check.IsNil)
	defer s2.Close()

	client, err := etcdutil.CreateClient(strings.Split(cfg1.AdvertisePeerUrls, ","), nil)
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
	mysqlCfg.LoadFromFile("./source.yaml")
	mysqlCfg.From.Password = os.Getenv("MYSQL_PSWD")
	task, err := mysqlCfg.Yaml()
	c.Assert(err, check.IsNil)
	sourceID := mysqlCfg.SourceID
	// 1. wait for scheduler to start
	time.Sleep(3 * time.Second)

	// 2. try to add a new mysql source
	req := &pb.OperateSourceRequest{Op: pb.SourceOp_StartSource, Config: []string{task}}
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

	// 3. try to add multiple source
	// 3.1 duplicated source id
	sourceID2 := "mysql-replica-02"
	mysqlCfg.SourceID = sourceID2
	task2, err := mysqlCfg.Yaml()
	c.Assert(err, check.IsNil)
	req = &pb.OperateSourceRequest{Op: pb.SourceOp_StartSource, Config: []string{task2, task2}}
	resp, err = s1.OperateSource(ctx, req)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.Equals, false)
	c.Assert(resp.Msg, check.Matches, ".*source config with ID "+sourceID2+" already exists.*")
	// 3.2 run same command after correction
	sourceID3 := "mysql-replica-03"
	mysqlCfg.SourceID = sourceID3
	task3, err := mysqlCfg.Yaml()
	c.Assert(err, check.IsNil)
	req = &pb.OperateSourceRequest{Op: pb.SourceOp_StartSource, Config: []string{task2, task3}}
	resp, err = s1.OperateSource(ctx, req)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.Equals, true)
	sort.Slice(resp.Sources, func(i, j int) bool {
		return resp.Sources[i].Source < resp.Sources[j].Source
	})
	c.Assert(resp.Sources, check.DeepEquals, []*pb.CommonWorkerResponse{{
		Result: true,
		Msg:    "source is added but there is no free worker to bound",
		Source: sourceID2,
	}, {
		Result: true,
		Msg:    "source is added but there is no free worker to bound",
		Source: sourceID3,
	}})
	unBoundSources = s1.scheduler.UnboundSources()
	c.Assert(unBoundSources, check.HasLen, 3)
	c.Assert(unBoundSources[0], check.Equals, sourceID)
	c.Assert(unBoundSources[1], check.Equals, sourceID2)
	c.Assert(unBoundSources[2], check.Equals, sourceID3)

	// 4. try to stop a non-exist-source
	req.Op = pb.SourceOp_StopSource
	mysqlCfg.SourceID = "not-exist-source"
	task4, err := mysqlCfg.Yaml()
	c.Assert(err, check.IsNil)
	req.Config = []string{task4}
	resp, err = s1.OperateSource(ctx, req)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.Equals, false)
	c.Assert(resp.Msg, check.Matches, `[\s\S]*source config with ID `+mysqlCfg.SourceID+` not exists[\s\S]*`)

	// 5. start workers, the unbounded sources should be bounded
	var wg sync.WaitGroup
	ctx1, cancel1 := context.WithCancel(ctx)
	ctx2, cancel2 := context.WithCancel(ctx)
	ctx3, cancel3 := context.WithCancel(ctx)
	workerName1 := "worker1"
	workerName2 := "worker2"
	workerName3 := "worker3"
	defer func() {
		clearSchedulerEnv(c, cancel1, &wg)
		clearSchedulerEnv(c, cancel2, &wg)
		clearSchedulerEnv(c, cancel3, &wg)
	}()
	c.Assert(s1.scheduler.AddWorker(workerName1, "172.16.10.72:8262"), check.IsNil)
	wg.Add(1)
	go func(ctx context.Context, workerName string) {
		defer wg.Done()
		c.Assert(ha.KeepAlive(ctx, s1.etcdClient, workerName, keepAliveTTL), check.IsNil)
	}(ctx1, workerName1)
	c.Assert(s1.scheduler.AddWorker(workerName2, "172.16.10.72:8263"), check.IsNil)
	wg.Add(1)
	go func(ctx context.Context, workerName string) {
		defer wg.Done()
		c.Assert(ha.KeepAlive(ctx, s1.etcdClient, workerName, keepAliveTTL), check.IsNil)
	}(ctx2, workerName2)
	c.Assert(s1.scheduler.AddWorker(workerName3, "172.16.10.72:8264"), check.IsNil)
	wg.Add(1)
	go func(ctx context.Context, workerName string) {
		defer wg.Done()
		c.Assert(ha.KeepAlive(ctx, s1.etcdClient, workerName, keepAliveTTL), check.IsNil)
	}(ctx3, workerName3)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		w := s1.scheduler.GetWorkerBySource(sourceID)
		return w != nil
	}), check.IsTrue)

	// 6. stop sources
	req.Config = []string{task, task2, task3}
	req.Op = pb.SourceOp_StopSource

	mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
	mockRevelantWorkerClient(mockWorkerClient, "", sourceID, req)
	s1.scheduler.SetWorkerClientForTest(workerName1, newMockRPCClient(mockWorkerClient))
	mockWorkerClient2 := pbmock.NewMockWorkerClient(ctrl)
	mockRevelantWorkerClient(mockWorkerClient2, "", sourceID2, req)
	s1.scheduler.SetWorkerClientForTest(workerName2, newMockRPCClient(mockWorkerClient2))
	mockWorkerClient3 := pbmock.NewMockWorkerClient(ctrl)
	mockRevelantWorkerClient(mockWorkerClient3, "", sourceID3, req)
	s1.scheduler.SetWorkerClientForTest(workerName3, newMockRPCClient(mockWorkerClient3))
	resp, err = s1.OperateSource(ctx, req)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.Equals, true)
	c.Assert(resp.Sources, check.DeepEquals, []*pb.CommonWorkerResponse{{
		Result: true,
		Source: sourceID,
	}, {
		Result: true,
		Source: sourceID2,
	}, {
		Result: true,
		Source: sourceID3,
	}})
	scm, _, err := ha.GetSourceCfg(etcdTestCli, sourceID, 0)
	c.Assert(err, check.IsNil)
	c.Assert(scm, check.HasLen, 0)
	cancel()
}

func generateServerConfig(c *check.C, name string) *Config {
	// create a new cluster
	cfg1 := NewConfig()
	c.Assert(cfg1.Parse([]string{"-config=./dm-master.toml"}), check.IsNil)
	cfg1.Name = name
	cfg1.DataDir = c.MkDir()
	cfg1.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfg1.AdvertiseAddr = cfg1.MasterAddr
	cfg1.PeerUrls = tempurl.Alloc()
	cfg1.AdvertisePeerUrls = cfg1.PeerUrls
	cfg1.InitialCluster = fmt.Sprintf("%s=%s", cfg1.Name, cfg1.AdvertisePeerUrls)
	return cfg1
}

func (t *testMaster) TestOfflineMember(c *check.C) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cfg1 := generateServerConfig(c, "dm-master-1")
	cfg2 := generateServerConfig(c, "dm-master-2")
	cfg3 := generateServerConfig(c, "dm-master-3")

	initialCluster := fmt.Sprintf("%s=%s", cfg1.Name, cfg1.AdvertisePeerUrls) + "," +
		fmt.Sprintf("%s=%s", cfg2.Name, cfg2.AdvertisePeerUrls) + "," +
		fmt.Sprintf("%s=%s", cfg3.Name, cfg3.AdvertisePeerUrls)
	cfg1.InitialCluster = initialCluster
	cfg2.InitialCluster = initialCluster
	cfg3.InitialCluster = initialCluster

	var wg sync.WaitGroup
	s1 := NewServer(cfg1)
	defer s1.Close()
	wg.Add(1)
	go func() {
		c.Assert(s1.Start(ctx), check.IsNil)
		wg.Done()
	}()

	s2 := NewServer(cfg2)
	defer s2.Close()
	wg.Add(1)
	go func() {
		c.Assert(s2.Start(ctx), check.IsNil)
		wg.Done()
	}()

	ctx3, cancel3 := context.WithCancel(ctx)
	s3 := NewServer(cfg3)
	c.Assert(s3.Start(ctx3), check.IsNil)
	defer s3.Close()
	defer cancel3()

	wg.Wait()
	time.Sleep(time.Second)
	_, leaderID, _, err := s1.election.LeaderInfo(ctx)
	c.Assert(err, check.IsNil)

	// master related operations
	req := &pb.OfflineMemberRequest{
		Type: "masters",
		Name: "xixi",
	}
	// test offline member with wrong type
	resp, err := s2.OfflineMember(ctx, req)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)
	c.Assert(resp.Msg, check.Equals, terror.ErrMasterInvalidOfflineType.Generate(req.Type).Error())
	// test offline member with invalid master name
	req.Type = common.Master
	resp, err = s2.OfflineMember(ctx, req)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)
	c.Assert(resp.Msg, check.Matches, `[\s\S]*dm-master with name `+req.Name+` not exists[\s\S]*`)
	// test offline member with correct master name
	cli := s2.etcdClient
	listResp, err := etcdutil.ListMembers(cli)
	c.Assert(err, check.IsNil)
	c.Assert(listResp.Members, check.HasLen, 3)

	// make sure s3 is not the leader, otherwise it will take some time to campain a new leader after close s3, and it may cause timeout
	c.Assert(utils.WaitSomething(20, 500*time.Millisecond, func() bool {
		_, leaderID, _, err = s1.election.LeaderInfo(ctx)
		if err != nil {
			return false
		}

		if leaderID == s3.cfg.Name {
			_, err = s3.OperateLeader(ctx, &pb.OperateLeaderRequest{
				Op: pb.LeaderOp_EvictLeaderOp,
			})
			c.Assert(err, check.IsNil)
		}
		return leaderID != s3.cfg.Name
	}), check.IsTrue)

	cancel3()
	s3.Close()

	req.Name = s3.cfg.Name
	resp, err = s2.OfflineMember(ctx, req)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Msg, check.Equals, "")
	c.Assert(resp.Result, check.IsTrue)

	listResp, err = etcdutil.ListMembers(cli)
	c.Assert(err, check.IsNil)
	c.Assert(listResp.Members, check.HasLen, 2)
	if listResp.Members[0].Name == cfg2.Name {
		listResp.Members[0], listResp.Members[1] = listResp.Members[1], listResp.Members[0]
	}
	c.Assert(listResp.Members[0].Name, check.Equals, cfg1.Name)
	c.Assert(listResp.Members[1].Name, check.Equals, cfg2.Name)

	_, leaderID2, _, err := s1.election.LeaderInfo(ctx)
	c.Assert(err, check.IsNil)

	if leaderID == cfg3.Name {
		// s3 is leader before, leader should re-campaign
		c.Assert(leaderID != leaderID2, check.IsTrue)
	} else {
		// s3 isn't leader before, leader should keep the same
		c.Assert(leaderID, check.Equals, leaderID2)
	}

	// worker related operations
	ectx, canc := context.WithTimeout(ctx, time.Second)
	defer canc()
	req1 := &pb.RegisterWorkerRequest{
		Name:    "xixi",
		Address: "127.0.0.1:1000",
	}
	regReq, err := s1.RegisterWorker(ectx, req1)
	c.Assert(err, check.IsNil)
	c.Assert(regReq.Result, check.IsTrue)

	req2 := &pb.OfflineMemberRequest{
		Type: common.Worker,
		Name: "haha",
	}
	{
		res, err := s1.OfflineMember(ectx, req2)
		c.Assert(err, check.IsNil)
		c.Assert(res.Result, check.IsFalse)
		c.Assert(res.Msg, check.Matches, `[\s\S]*dm-worker with name `+req2.Name+` not exists[\s\S]*`)
	}
	{
		req2.Name = "xixi"
		res, err := s1.OfflineMember(ectx, req2)
		c.Assert(err, check.IsNil)
		c.Assert(res.Result, check.IsTrue)
	}
	{
		// register offline worker again. TICASE-962, 963
		resp, err := s1.RegisterWorker(ectx, req1)
		c.Assert(err, check.IsNil)
		c.Assert(resp.Result, check.IsTrue)
	}
}

func (t *testMaster) TestGetTaskCfg(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	server := testDefaultMasterServer(c)
	sources, workers := extractWorkerSource(server.cfg.Deploy)

	var wg sync.WaitGroup
	taskName := "test"
	ctx, cancel := context.WithCancel(context.Background())
	req := &pb.StartTaskRequest{
		Task:    taskConfig,
		Sources: sources,
	}
	server.scheduler, _ = testMockScheduler(ctx, &wg, c, sources, workers, "",
		makeWorkerClientsForHandle(ctrl, taskName, sources, workers, req))

	// start task
	resp, err := server.StartTask(context.Background(), req)
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)

	// get task config
	req1 := &pb.GetTaskCfgRequest{
		Name: taskName,
	}
	resp1, err := server.GetTaskCfg(context.Background(), req1)
	c.Assert(err, check.IsNil)
	c.Assert(resp1.Result, check.IsTrue)
	c.Assert(strings.Contains(resp1.Cfg, "name: test"), check.IsTrue)

	// wrong task name
	req2 := &pb.GetTaskCfgRequest{
		Name: "haha",
	}
	resp2, err := server.GetTaskCfg(context.Background(), req2)
	c.Assert(err, check.IsNil)
	c.Assert(resp2.Result, check.IsFalse)

	// test recover from etcd
	server.scheduler.Close()
	server.scheduler.Start(ctx, etcdTestCli)

	resp3, err := server.GetTaskCfg(context.Background(), req1)
	c.Assert(err, check.IsNil)
	c.Assert(resp3.Result, check.IsTrue)
	c.Assert(resp3.Cfg, check.Equals, resp1.Cfg)

	clearSchedulerEnv(c, cancel, &wg)
}

func stageDeepEqualExcludeRev(c *check.C, stage, expectStage ha.Stage) {
	expectStage.Revision = stage.Revision
	c.Assert(stage, check.DeepEquals, expectStage)
}

func (t *testMaster) relayStageMatch(c *check.C, s *scheduler.Scheduler, source string, expectStage pb.Stage) {
	stage := ha.NewRelayStage(expectStage, source)
	stageDeepEqualExcludeRev(c, s.GetExpectRelayStage(source), stage)

	eStage, _, err := ha.GetRelayStage(etcdTestCli, source)
	c.Assert(err, check.IsNil)
	switch expectStage {
	case pb.Stage_Running, pb.Stage_Paused:
		stageDeepEqualExcludeRev(c, eStage, stage)
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
		stageDeepEqualExcludeRev(c, eStageM[task], stage)
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

func createTableInfo(c *check.C, p *parser.Parser, se sessionctx.Context, tableID int64, sql string) *model.TableInfo {
	node, err := p.ParseOneStmt(sql, "utf8mb4", "utf8mb4_bin")
	if err != nil {
		c.Fatalf("fail to parse stmt, %v", err)
	}
	createStmtNode, ok := node.(*ast.CreateTableStmt)
	if !ok {
		c.Fatalf("%s is not a CREATE TABLE statement", sql)
	}
	info, err := tiddl.MockTableInfo(se, createStmtNode, tableID)
	if err != nil {
		c.Fatalf("fail to create table info, %v", err)
	}
	return info
}
