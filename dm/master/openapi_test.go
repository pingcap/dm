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
	"net/http"
	"testing"
	"time"

	"github.com/deepmap/oapi-codegen/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	filter "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/tikv/pd/pkg/tempurl"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/integration"

	"github.com/pingcap/dm/checker"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/master/workerrpc"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/pbmock"
	"github.com/pingcap/dm/openapi"
	"github.com/pingcap/dm/pkg/ha"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

var openAPITestSuite = check.SerialSuites(&openAPISuite{})

const (
	sourceSampleFile = "source.yaml"
)

// some data for test.
var (
	source1Name = "mysql-replica-01"
	source2Name = "mysql-replica-02"

	taskName   = "test"
	metaSchema = "dm_meta"

	exportThreads = 4
	importThreads = 16
	dataDir       = "./exported_data"

	replBatch   = 200
	replThreads = 32

	noShardSourceSchema = "some_db"
	noShardSourceTable  = "*"
	noShardTargetSchema = "new_name_db"
	noShardTargetTable  = ""

	shardSource1BinlogName = "mysql-bin.001"
	shardSource1BinlogPos  = 0
	shardSource1GtidSet    = ""
	shardSource2BinlogName = "mysql-bin.002"
	shardSource2BinlogPos  = 1232
	shardSource2GtidSet    = "12e57f06-f360-11eb-8235-585cc2bc66c9:1-24"

	shardSource1Schema = "db_*"
	shardSource1Table  = "tbl_1"
	shardSource2Schema = "db_*"
	shardSource2Table  = "tbl_1"

	shardTargetSchema = "db1"
	shardTargetTable  = "tbl"

	shardSource1FilterName  = "filterA"
	shardSource1FilterEvent = "drop database"
	shardSource1FilterSQL   = "^Drop"
)

type openAPISuite struct {
	testT *testing.T

	etcdTestCli     *clientv3.Client
	testEtcdCluster *integration.ClusterV3
	workerClients   map[string]workerrpc.Client
}

func (t *openAPISuite) SetUpSuite(c *check.C) {
	checkAndAdjustSourceConfigFunc = checkAndNoAdjustSourceConfigMock
}

func (t *openAPISuite) TearDownSuite(c *check.C) {
	checkAndAdjustSourceConfigFunc = checkAndAdjustSourceConfig
}

func (t *openAPISuite) SetUpTest(c *check.C) {
	t.testEtcdCluster = integration.NewClusterV3(t.testT, &integration.ClusterConfig{Size: 1})
	t.etcdTestCli = t.testEtcdCluster.RandClient()
	t.workerClients = make(map[string]workerrpc.Client)

	c.Assert(ha.ClearTestInfoOperation(t.etcdTestCli), check.IsNil)
}

func (t *openAPISuite) TestRedirectRequestToLeader(c *check.C) {
	ctx, cancel := context.WithCancel(context.Background())
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

	baseURL := "/api/v1/sources"
	// list source from leader
	result := testutil.NewRequest().Get(baseURL).Go(t.testT, s1.echo)
	// check http status code
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	var resultListSource openapi.GetSourceListResponse
	err := result.UnmarshalBodyToObject(&resultListSource)
	c.Assert(err, check.IsNil)
	c.Assert(resultListSource.Data, check.HasLen, 0)
	c.Assert(resultListSource.Total, check.Equals, 0)

	// list source not from leader will get a redirect
	result2 := testutil.NewRequest().Get(baseURL).Go(t.testT, s2.echo)
	c.Assert(result2.Code(), check.Equals, http.StatusTemporaryRedirect)
	cancel()
}

func (t *openAPISuite) TestSourceAPI(c *check.C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := setupServer(ctx, c)
	defer s.Close()

	baseURL := "/api/v1/sources"

	dbCfg := config.GetDBConfigFromEnv()
	purgeInterVal := int64(10)
	source1 := openapi.Source{
		SourceName: source1Name,
		EnableGtid: false,
		Host:       dbCfg.Host,
		Password:   dbCfg.Password,
		Port:       dbCfg.Port,
		User:       dbCfg.User,
		Purge:      &openapi.Purge{Interval: &purgeInterVal},
	}
	result := testutil.NewRequest().Post(baseURL).WithJsonBody(source1).Go(t.testT, s.echo)
	// check http status code
	c.Assert(result.Code(), check.Equals, http.StatusCreated)
	var resultSource openapi.Source
	err := result.UnmarshalBodyToObject(&resultSource)
	c.Assert(err, check.IsNil)
	c.Assert(resultSource.User, check.Equals, source1.User)
	c.Assert(resultSource.Host, check.Equals, source1.Host)
	c.Assert(resultSource.Port, check.Equals, source1.Port)
	c.Assert(resultSource.Password, check.Equals, source1.Password)
	c.Assert(resultSource.EnableGtid, check.Equals, source1.EnableGtid)
	c.Assert(resultSource.SourceName, check.Equals, source1.SourceName)
	c.Assert(*resultSource.Purge.Interval, check.Equals, *source1.Purge.Interval)

	// create source with same name will failed
	source2 := source1
	result2 := testutil.NewRequest().Post(baseURL).WithJsonBody(source2).Go(t.testT, s.echo)
	// check http status code
	c.Assert(result2.Code(), check.Equals, http.StatusBadRequest)
	var errResp openapi.ErrorWithMessage
	err = result2.UnmarshalBodyToObject(&errResp)
	c.Assert(err, check.IsNil)
	c.Assert(errResp.ErrorCode, check.Equals, int(terror.ErrSchedulerSourceCfgExist.Code()))

	// list source
	result3 := testutil.NewRequest().Get(baseURL).Go(t.testT, s.echo)
	// check http status code
	c.Assert(result3.Code(), check.Equals, http.StatusOK)
	var resultListSource openapi.GetSourceListResponse
	err = result3.UnmarshalBodyToObject(&resultListSource)
	c.Assert(err, check.IsNil)
	c.Assert(resultListSource.Data, check.HasLen, 1)
	c.Assert(resultListSource.Total, check.Equals, 1)
	c.Assert(resultListSource.Data[0].SourceName, check.Equals, source1.SourceName)

	// delete source
	result4 := testutil.NewRequest().Delete(fmt.Sprintf("%s/%s", baseURL, source1.SourceName)).Go(t.testT, s.echo)
	// check http status code
	c.Assert(result4.Code(), check.Equals, http.StatusNoContent)

	// delete again will failed
	result5 := testutil.NewRequest().Delete(fmt.Sprintf("%s/%s", baseURL, source1.SourceName)).Go(t.testT, s.echo)
	c.Assert(result5.Code(), check.Equals, http.StatusBadRequest)
	var errResp2 openapi.ErrorWithMessage
	err = result5.UnmarshalBodyToObject(&errResp2)
	c.Assert(err, check.IsNil)
	c.Assert(errResp2.ErrorCode, check.Equals, int(terror.ErrSchedulerSourceCfgNotExist.Code()))

	// list source
	result6 := testutil.NewRequest().Get(baseURL).Go(t.testT, s.echo)
	// check http status code
	c.Assert(result6.Code(), check.Equals, http.StatusOK)
	var resultListSource2 openapi.GetSourceListResponse
	err = result6.UnmarshalBodyToObject(&resultListSource2)
	c.Assert(err, check.IsNil)
	c.Assert(resultListSource2.Data, check.HasLen, 0)
	c.Assert(resultListSource2.Total, check.Equals, 0)
	cancel()
}

func (t *openAPISuite) TestRelayAPI(c *check.C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()
	s := setupServer(ctx, c)
	defer s.Close()

	baseURL := "/api/v1/sources"

	source1 := openapi.Source{
		SourceName: source1Name,
		EnableGtid: false,
		Host:       "127.0.0.1",
		Password:   "123456",
		Port:       3306,
		User:       "root",
	}
	result := testutil.NewRequest().Post(baseURL).WithJsonBody(source1).Go(t.testT, s.echo)
	// check http status code
	c.Assert(result.Code(), check.Equals, http.StatusCreated)

	// get source status
	source1StatusURL := fmt.Sprintf("%s/%s/status", baseURL, source1.SourceName)
	result2 := testutil.NewRequest().Get(source1StatusURL).Go(t.testT, s.echo)
	c.Assert(result2.Code(), check.Equals, http.StatusOK)

	var getSourceStatusResponse openapi.GetSourceStatusResponse
	err := result2.UnmarshalBodyToObject(&getSourceStatusResponse)
	c.Assert(err, check.IsNil)
	c.Assert(getSourceStatusResponse.Data[0].SourceName, check.Equals, source1.SourceName)
	c.Assert(getSourceStatusResponse.Data[0].WorkerName, check.Equals, "") // no worker bound
	c.Assert(getSourceStatusResponse.Total, check.Equals, 1)

	// add mock worker the unbounded sources should be bounded
	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	workerName1 := "worker1"
	c.Assert(s.scheduler.AddWorker(workerName1, "172.16.10.72:8262"), check.IsNil)
	go func(ctx context.Context, workerName string) {
		c.Assert(ha.KeepAlive(ctx, s.etcdClient, workerName, keepAliveTTL), check.IsNil)
	}(ctx1, workerName1)
	// wait worker ready
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		w := s.scheduler.GetWorkerBySource(source1.SourceName)
		return w != nil
	}), check.IsTrue)

	// mock worker get status relay not started
	mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
	mockRelayQueryStatus(mockWorkerClient, source1.SourceName, workerName1, pb.Stage_InvalidStage)
	s.scheduler.SetWorkerClientForTest(workerName1, newMockRPCClient(mockWorkerClient))

	// get source status again,source should be bounded by worker1,but relay not started
	result3 := testutil.NewRequest().Get(source1StatusURL).Go(t.testT, s.echo)
	c.Assert(result3.Code(), check.Equals, http.StatusOK)
	var getSourceStatusResponse2 openapi.GetSourceStatusResponse
	err = result3.UnmarshalBodyToObject(&getSourceStatusResponse2)
	c.Assert(err, check.IsNil)
	c.Assert(getSourceStatusResponse2.Data[0].SourceName, check.Equals, source1.SourceName)
	c.Assert(getSourceStatusResponse2.Data[0].WorkerName, check.Equals, workerName1) // worker1 is bound
	c.Assert(getSourceStatusResponse2.Data[0].RelayStatus, check.IsNil)              // not start relay
	c.Assert(getSourceStatusResponse2.Total, check.Equals, 1)

	// start relay
	startRelayURL := fmt.Sprintf("%s/%s/start-relay", baseURL, source1.SourceName)
	openAPIStartRelayReq := openapi.StartRelayRequest{WorkerNameList: []string{workerName1}}
	result4 := testutil.NewRequest().Patch(startRelayURL).WithJsonBody(openAPIStartRelayReq).Go(t.testT, s.echo)
	// check http status code
	c.Assert(result4.Code(), check.Equals, http.StatusOK)
	relayWorkers, err := s.scheduler.GetRelayWorkers(source1Name)
	c.Assert(err, check.IsNil)
	c.Assert(relayWorkers, check.HasLen, 1)

	// mock worker get status relay started
	mockWorkerClient = pbmock.NewMockWorkerClient(ctrl)
	mockRelayQueryStatus(mockWorkerClient, source1.SourceName, workerName1, pb.Stage_Running)
	s.scheduler.SetWorkerClientForTest(workerName1, newMockRPCClient(mockWorkerClient))
	// get source status again, relay status should not be nil
	result5 := testutil.NewRequest().Get(source1StatusURL).Go(t.testT, s.echo)
	c.Assert(result5.Code(), check.Equals, http.StatusOK)
	var getSourceStatusResponse3 openapi.GetSourceStatusResponse
	err = result5.UnmarshalBodyToObject(&getSourceStatusResponse3)
	c.Assert(err, check.IsNil)
	c.Assert(getSourceStatusResponse3.Data[0].RelayStatus.Stage, check.Equals, pb.Stage_Running.String())

	// mock worker get status meet error
	mockWorkerClient = pbmock.NewMockWorkerClient(ctrl)
	mockRelayQueryStatus(mockWorkerClient, source1.SourceName, workerName1, pb.Stage_Paused)
	s.scheduler.SetWorkerClientForTest(workerName1, newMockRPCClient(mockWorkerClient))
	// get source status again, error message should not be nil
	result6 := testutil.NewRequest().Get(source1StatusURL).Go(t.testT, s.echo)
	c.Assert(result6.Code(), check.Equals, http.StatusOK)
	var getSourceStatusResponse4 openapi.GetSourceStatusResponse
	err = result6.UnmarshalBodyToObject(&getSourceStatusResponse4)
	c.Assert(err, check.IsNil)
	c.Assert(*getSourceStatusResponse4.Data[0].ErrorMsg, check.Equals, "some error happened")
	c.Assert(getSourceStatusResponse4.Data[0].WorkerName, check.Equals, workerName1)

	// test stop relay
	stopRelayURL := fmt.Sprintf("%s/%s/stop-relay", baseURL, source1.SourceName)
	stopRelayReq := openapi.StopRelayRequest{WorkerNameList: []string{workerName1}}
	result7 := testutil.NewRequest().Patch(stopRelayURL).WithJsonBody(stopRelayReq).Go(t.testT, s.echo)
	c.Assert(result7.Code(), check.Equals, http.StatusOK)
	relayWorkers, err = s.scheduler.GetRelayWorkers(source1Name)
	c.Assert(err, check.IsNil)
	c.Assert(relayWorkers, check.HasLen, 0)

	// mock worker get status relay already stopped
	mockWorkerClient = pbmock.NewMockWorkerClient(ctrl)
	mockRelayQueryStatus(mockWorkerClient, source1.SourceName, workerName1, pb.Stage_InvalidStage)
	s.scheduler.SetWorkerClientForTest(workerName1, newMockRPCClient(mockWorkerClient))
	// get source status again,source
	result8 := testutil.NewRequest().Get(source1StatusURL).Go(t.testT, s.echo)
	c.Assert(result8.Code(), check.Equals, http.StatusOK)

	var getSourceStatusResponse5 openapi.GetSourceStatusResponse
	err = result8.UnmarshalBodyToObject(&getSourceStatusResponse5)
	c.Assert(err, check.IsNil)
	c.Assert(getSourceStatusResponse5.Data[0].SourceName, check.Equals, source1.SourceName)
	c.Assert(getSourceStatusResponse5.Data[0].WorkerName, check.Equals, workerName1) // worker1 is bound
	c.Assert(getSourceStatusResponse5.Data[0].RelayStatus, check.IsNil)              // not start relay
	c.Assert(getSourceStatusResponse5.Total, check.Equals, 1)
	cancel()
}

func (t *openAPISuite) TestTaskAPI(c *check.C) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s := setupServer(ctx, c)
	defer s.Close()
	c.Assert(failpoint.Enable("github.com/pingcap/dm/dm/master/MockSkipAdjustTargetDB", `return(true)`), check.IsNil)
	checker.CheckSyncConfigFunc = mockCheckSyncConfig
	defer func() {
		checker.CheckSyncConfigFunc = checker.CheckSyncConfig
	}()

	dbCfg := config.GetDBConfigFromEnv()
	source1 := openapi.Source{
		SourceName: source1Name,
		EnableGtid: false,
		Host:       dbCfg.Host,
		Password:   dbCfg.Password,
		Port:       dbCfg.Port,
		User:       dbCfg.User,
	}
	// create source
	sourceURL := "/api/v1/sources"
	result := testutil.NewRequest().Post(sourceURL).WithJsonBody(source1).Go(t.testT, s.echo)
	// check http status code
	c.Assert(result.Code(), check.Equals, http.StatusCreated)

	// add mock worker  start workers, the unbounded sources should be bounded
	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	workerName1 := "worker-1"
	c.Assert(s.scheduler.AddWorker(workerName1, "172.16.10.72:8262"), check.IsNil)
	go func(ctx context.Context, workerName string) {
		c.Assert(ha.KeepAlive(ctx, s.etcdClient, workerName, keepAliveTTL), check.IsNil)
	}(ctx1, workerName1)
	// wait worker ready
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		w := s.scheduler.GetWorkerBySource(source1.SourceName)
		return w != nil
	}), check.IsTrue)

	// create task
	taskURL := "/api/v1/tasks"

	task := genNoShardTask()
	// use a valid target db
	task.TargetConfig.Host = dbCfg.Host
	task.TargetConfig.Port = dbCfg.Port
	task.TargetConfig.User = dbCfg.User
	task.TargetConfig.Password = dbCfg.Password

	createTaskReq := openapi.CreateTaskRequest{RemoveMeta: false, Task: task}
	result2 := testutil.NewRequest().Post(taskURL).WithJsonBody(createTaskReq).Go(t.testT, s.echo)
	c.Assert(result2.Code(), check.Equals, http.StatusCreated)
	var createTaskResp openapi.Task
	err := result2.UnmarshalBodyToObject(&createTaskResp)
	c.Assert(err, check.IsNil)
	c.Assert(task.Name, check.Equals, createTaskResp.Name)
	subTaskM := s.scheduler.GetSubTaskCfgsByTask(taskName)
	c.Assert(len(subTaskM) == 1, check.IsTrue)
	c.Assert(subTaskM[source1Name].Name, check.Equals, taskName)

	// stop task
	result3 := testutil.NewRequest().Delete(fmt.Sprintf("%s/%s", taskURL, task.Name)).Go(t.testT, s.echo)
	c.Assert(result3.Code(), check.Equals, http.StatusNoContent)
	subTaskM = s.scheduler.GetSubTaskCfgsByTask(taskName)
	c.Assert(len(subTaskM) == 0, check.IsTrue)
	cancel()
	c.Assert(failpoint.Disable("github.com/pingcap/dm/dm/master/MockSkipAdjustTargetDB"), check.IsNil)
}

func (t *openAPISuite) TestModelToSubTaskConfigList(c *check.C) {
	testNoShardTaskToSubTaskConfig(c)
	testShardAndFilterTaskToSubTaskConfig(c)
}

func genNoShardTask() openapi.Task {
	/* no shard task
	name: test
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
	taskSourceConf := openapi.TaskSourceConf{
		SourceName: source1Name,
	}
	tableMigrateRule := openapi.TaskTableMigrateRule{
		EventFilterName: nil,
		Source: struct {
			Schema     string "json:\"schema\""
			SourceName string "json:\"source_name\""
			Table      string "json:\"table\""
		}{
			Schema:     noShardSourceSchema,
			SourceName: source1Name,
			Table:      noShardSourceTable,
		},
		Target: struct {
			Schema string "json:\"schema\""
			Table  string "json:\"table\""
		}{
			Schema: noShardTargetSchema,
			Table:  noShardTargetTable,
		},
	}

	return openapi.Task{
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
}

func genShardAndFilterTask() openapi.Task {
	/* shard and filter task
	name: test
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
	    binlog-name: "mysql-bin.001"
	    binlog-pos: 0
	    gtid: ""
	  - source-name: "mysql-replica-02"
	    binlog-name: "mysql-bin.002"
	    binlog-pos: 1232
	    gtid: "12e57f06-f360-11eb-8235-585cc2bc66c9:1-24"

	table-migrate-rule:
	  - source:
	       source-name: "mysql-replica-01"
	       schema: "db_*"
	       table: "tbl_1"
	    target:
	       schema: "db1"
	       table: "tbl"
	    event-filter: ["filterA"]
	  - source:
	       source-name: "mysql-replica-02"
	       schema: "db_*"
	       table: "tbl_1"
	    target:
	       schema: "db1"
	       table: "tbl"

	event-filter:
	   -name: "filterA"
	    ignore-event: ["drop database"]
	    ignore-sql: ["^Drop"]
	   -name: "filterB"
	    ignore-event: ["drop database"]
	    ignore-sql: ["^Create"]
	*/
	taskSource1Conf := openapi.TaskSourceConf{
		BinlogGtid: &shardSource1GtidSet,
		BinlogName: &shardSource1BinlogName,
		BinlogPos:  &shardSource1BinlogPos,
		SourceName: source1Name,
	}
	taskSource2Conf := openapi.TaskSourceConf{
		SourceName: source2Name,
		BinlogGtid: &shardSource2GtidSet,
		BinlogName: &shardSource2BinlogName,
		BinlogPos:  &shardSource2BinlogPos,
	}
	ignoreEvent := []string{shardSource1FilterEvent}
	ignoreSQL := []string{shardSource1FilterSQL}
	eventFilterRule := openapi.TaskEventFilterRule{
		IgnoreEvent: &ignoreEvent,
		IgnoreSql:   &ignoreSQL,
		RuleName:    shardSource1FilterName,
	}
	eventFilterNameList := []string{"filterA"}
	eventFilterList := []openapi.TaskEventFilterRule{eventFilterRule}
	tableMigrateRule1 := openapi.TaskTableMigrateRule{
		EventFilterName: &eventFilterNameList,
		Source: struct {
			Schema     string "json:\"schema\""
			SourceName string "json:\"source_name\""
			Table      string "json:\"table\""
		}{
			Schema:     shardSource1Schema,
			SourceName: source1Name,
			Table:      shardSource1Table,
		},
		Target: struct {
			Schema string "json:\"schema\""
			Table  string "json:\"table\""
		}{
			Schema: shardTargetSchema,
			Table:  shardTargetTable,
		},
	}
	tableMigrateRule2 := openapi.TaskTableMigrateRule{
		EventFilterName: nil,
		Source: struct {
			Schema     string "json:\"schema\""
			SourceName string "json:\"source_name\""
			Table      string "json:\"table\""
		}{
			Schema:     shardSource2Schema,
			SourceName: source2Name,
			Table:      shardSource2Table,
		},
		Target: struct {
			Schema string "json:\"schema\""
			Table  string "json:\"table\""
		}{
			Schema: shardTargetSchema,
			Table:  shardTargetTable,
		},
	}
	shardMode := openapi.TaskShardModePessimistic
	return openapi.Task{
		EnhanceOnlineSchemaChange: true,
		ShardMode:                 &shardMode,
		Name:                      taskName,
		MetaSchema:                &metaSchema,
		TaskMode:                  openapi.TaskTaskModeAll,
		OnDuplication:             openapi.TaskOnDuplicationError,
		EventFilterRule:           &eventFilterList,
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
			SourceConf: []openapi.TaskSourceConf{taskSource1Conf, taskSource2Conf},
		},
		TableMigrateRule: []openapi.TaskTableMigrateRule{tableMigrateRule1, tableMigrateRule2},
	}
}

func testNoShardTaskToSubTaskConfig(c *check.C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := setupServer(ctx, c)
	defer s.Close()

	sourceCfg1, err := config.LoadFromFile(sourceSampleFile)
	c.Assert(err, check.IsNil)
	sourceCfg1.SourceID = source1Name
	c.Assert(s.scheduler.AddSourceCfg(sourceCfg1), check.IsNil)
	task := genNoShardTask()
	toDBCfg := &config.DBConfig{
		Host:     task.TargetConfig.Host,
		Port:     task.TargetConfig.Port,
		User:     task.TargetConfig.User,
		Password: task.TargetConfig.Password,
	}

	subTaskConfigList, err := s.modelToSubTaskConfigList(toDBCfg, &task)
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
	c.Assert(rule.SchemaPattern, check.Equals, noShardSourceSchema)
	c.Assert(rule.TablePattern, check.Equals, noShardSourceTable)
	c.Assert(rule.TargetSchema, check.Equals, noShardTargetSchema)
	c.Assert(rule.TargetTable, check.Equals, noShardTargetTable)
	// check filter
	c.Assert(subTaskConfig.FilterRules, check.HasLen, 0)
	// check balist
	c.Assert(subTaskConfig.BAList, check.NotNil)
	ba := subTaskConfig.BAList
	c.Assert(ba.DoDBs, check.HasLen, 1)
	c.Assert(ba.DoDBs[0], check.Equals, noShardSourceSchema)
	c.Assert(ba.DoTables, check.HasLen, 1)
	c.Assert(ba.DoTables[0].Name, check.Equals, noShardSourceTable)
	c.Assert(ba.DoTables[0].Schema, check.Equals, noShardSourceSchema)
	cancel()
}

func testShardAndFilterTaskToSubTaskConfig(c *check.C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := setupServer(ctx, c)
	defer s.Close()

	sourceCfg1, err := config.LoadFromFile(sourceSampleFile)
	c.Assert(err, check.IsNil)
	sourceCfg1.SourceID = source1Name
	// sourceCfg1.From.m
	sourceCfg2, err := config.LoadFromFile(sourceSampleFile)
	c.Assert(err, check.IsNil)
	sourceCfg2.SourceID = source2Name
	c.Assert(s.scheduler.AddSourceCfg(sourceCfg1), check.IsNil)
	c.Assert(s.scheduler.AddSourceCfg(sourceCfg2), check.IsNil)

	task := genShardAndFilterTask()
	toDBCfg := &config.DBConfig{
		Host:     task.TargetConfig.Host,
		Port:     task.TargetConfig.Port,
		User:     task.TargetConfig.User,
		Password: task.TargetConfig.Password,
	}

	subTaskConfigList, err := s.modelToSubTaskConfigList(toDBCfg, &task)
	c.Assert(err, check.IsNil)
	c.Assert(subTaskConfigList, check.HasLen, 2)

	// check sub task 1
	subTask1Config := subTaskConfigList[0]
	// check task name and mode
	c.Assert(subTask1Config.Name, check.Equals, taskName)
	// check task meta
	c.Assert(subTask1Config.MetaSchema, check.Equals, metaSchema)
	c.Assert(subTask1Config.Meta, check.NotNil)
	c.Assert(subTask1Config.Meta.BinLogGTID, check.Equals, shardSource1GtidSet)
	c.Assert(subTask1Config.Meta.BinLogName, check.Equals, shardSource1BinlogName)
	c.Assert(subTask1Config.Meta.BinLogPos, check.Equals, uint32(shardSource1BinlogPos))
	// check shard config
	c.Assert(subTask1Config.ShardMode, check.Equals, string(openapi.TaskShardModePessimistic))
	// check online schema change
	c.Assert(subTask1Config.OnlineDDL, check.Equals, true)
	// check case sensitive
	c.Assert(subTask1Config.CaseSensitive, check.Equals, sourceCfg1.CaseSensitive)
	// check from
	c.Assert(subTask1Config.From.Host, check.Equals, sourceCfg1.From.Host)
	// check to
	c.Assert(subTask1Config.To.Host, check.Equals, toDBCfg.Host)
	// check dumpling loader syncer config
	c.Assert(subTask1Config.MydumperConfig.Threads, check.Equals, exportThreads)
	c.Assert(subTask1Config.LoaderConfig.Dir, check.Equals, fmt.Sprintf("%s.%s", dataDir, taskName))
	c.Assert(subTask1Config.LoaderConfig.PoolSize, check.Equals, importThreads)
	c.Assert(subTask1Config.SyncerConfig.WorkerCount, check.Equals, replThreads)
	c.Assert(subTask1Config.SyncerConfig.Batch, check.Equals, replBatch)
	// check route
	c.Assert(subTask1Config.RouteRules, check.HasLen, 1)
	rule := subTask1Config.RouteRules[0]
	c.Assert(rule.SchemaPattern, check.Equals, shardSource1Schema)
	c.Assert(rule.TablePattern, check.Equals, shardSource1Table)
	c.Assert(rule.TargetSchema, check.Equals, shardTargetSchema)
	c.Assert(rule.TargetTable, check.Equals, shardTargetTable)
	// check filter
	c.Assert(subTask1Config.FilterRules, check.HasLen, 1)
	c.Assert(subTask1Config.FilterRules[0].SchemaPattern, check.Equals, shardSource1Schema)
	c.Assert(subTask1Config.FilterRules[0].TablePattern, check.Equals, shardSource1Table)
	c.Assert(subTask1Config.FilterRules[0].Action, check.Equals, filter.Ignore)
	c.Assert(subTask1Config.FilterRules[0].SQLPattern, check.HasLen, 1)
	c.Assert(subTask1Config.FilterRules[0].SQLPattern[0], check.Equals, shardSource1FilterSQL)
	c.Assert(subTask1Config.FilterRules[0].Events, check.HasLen, 1)
	c.Assert(string(subTask1Config.FilterRules[0].Events[0]), check.Equals, shardSource1FilterEvent)
	// check balist
	c.Assert(subTask1Config.BAList, check.NotNil)
	ba := subTask1Config.BAList
	c.Assert(ba.DoDBs, check.HasLen, 1)
	c.Assert(ba.DoDBs[0], check.Equals, shardSource1Schema)
	c.Assert(ba.DoTables, check.HasLen, 1)
	c.Assert(ba.DoTables[0].Name, check.Equals, shardSource1Table)
	c.Assert(ba.DoTables[0].Schema, check.Equals, shardSource1Schema)

	// check sub task 2
	subTask2Config := subTaskConfigList[1]
	// check task name and mode
	c.Assert(subTask2Config.Name, check.Equals, taskName)
	// check task meta
	c.Assert(subTask2Config.MetaSchema, check.Equals, metaSchema)
	c.Assert(subTask2Config.Meta, check.NotNil)
	c.Assert(subTask2Config.Meta.BinLogGTID, check.Equals, shardSource2GtidSet)
	c.Assert(subTask2Config.Meta.BinLogName, check.Equals, shardSource2BinlogName)
	c.Assert(subTask2Config.Meta.BinLogPos, check.Equals, uint32(shardSource2BinlogPos))
	// check shard config
	c.Assert(subTask2Config.ShardMode, check.Equals, string(openapi.TaskShardModePessimistic))
	// check online schema change
	c.Assert(subTask2Config.OnlineDDL, check.Equals, true)
	// check case sensitive
	c.Assert(subTask2Config.CaseSensitive, check.Equals, sourceCfg2.CaseSensitive)
	// check from
	c.Assert(subTask2Config.From.Host, check.Equals, sourceCfg2.From.Host)
	// check to
	c.Assert(subTask2Config.To.Host, check.Equals, toDBCfg.Host)
	// check dumpling loader syncer config
	c.Assert(subTask2Config.MydumperConfig.Threads, check.Equals, exportThreads)
	c.Assert(subTask2Config.LoaderConfig.Dir, check.Equals, fmt.Sprintf("%s.%s", dataDir, taskName))
	c.Assert(subTask2Config.LoaderConfig.PoolSize, check.Equals, importThreads)
	c.Assert(subTask2Config.SyncerConfig.WorkerCount, check.Equals, replThreads)
	c.Assert(subTask2Config.SyncerConfig.Batch, check.Equals, replBatch)
	// check route
	c.Assert(subTask2Config.RouteRules, check.HasLen, 1)
	rule = subTask2Config.RouteRules[0]
	c.Assert(rule.SchemaPattern, check.Equals, shardSource2Schema)
	c.Assert(rule.TablePattern, check.Equals, shardSource2Table)
	c.Assert(rule.TargetSchema, check.Equals, shardTargetSchema)
	c.Assert(rule.TargetTable, check.Equals, shardTargetTable)
	// check filter
	c.Assert(subTask2Config.FilterRules, check.HasLen, 0)
	// check balist
	c.Assert(subTask1Config.BAList, check.NotNil)
	ba = subTask2Config.BAList
	c.Assert(ba.DoDBs, check.HasLen, 1)
	c.Assert(ba.DoDBs[0], check.Equals, shardSource2Schema)
	c.Assert(ba.DoTables, check.HasLen, 1)
	c.Assert(ba.DoTables[0].Name, check.Equals, shardSource2Table)
	c.Assert(ba.DoTables[0].Schema, check.Equals, shardSource2Schema)
	cancel()
}

func setupServer(ctx context.Context, c *check.C) *Server {
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
	// wait the first one become the leader
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return s1.election.IsLeader()
	}), check.IsTrue)
	return s1
}

// nolint:unparam
func mockRelayQueryStatus(
	mockWorkerClient *pbmock.MockWorkerClient, sourceName, workerName string, stage pb.Stage) {
	queryResp := &pb.QueryStatusResponse{
		Result: true,
		SourceStatus: &pb.SourceStatus{
			Worker: workerName,
			Source: sourceName,
		},
	}
	if stage == pb.Stage_Running {
		queryResp.SourceStatus.RelayStatus = &pb.RelayStatus{Stage: stage}
	}
	if stage == pb.Stage_Paused {
		queryResp.Result = false
		queryResp.Msg = "some error happened"
	}
	mockWorkerClient.EXPECT().QueryStatus(
		gomock.Any(),
		&pb.QueryStatusRequest{Name: ""},
	).Return(queryResp, nil).MaxTimes(maxRetryNum)
}

func mockCheckSyncConfig(ctx context.Context, cfgs []*config.SubTaskConfig, errCnt, warnCnt int64) error {
	return nil
}
