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

package worker

import (
	"context"
	"io/ioutil"
	"net/http"
	"net/url"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/pkg/tempurl"
	"github.com/siddontang/go-mysql/mysql"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"google.golang.org/grpc"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/unit"
	"github.com/pingcap/dm/pkg/ha"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

// do not forget to update this path if the file removed/renamed.
const (
	sourceSampleFile  = "./source.toml"
	subtaskSampleFile = "./subtask.toml"
	mydumperPath      = "../../bin/mydumper"
)

func TestServer(t *testing.T) {
	TestingT(t)
}

type testServer struct{}

var _ = Suite(&testServer{})

func (t *testServer) SetUpSuite(c *C) {
	err := log.InitLogger(&log.Config{})
	c.Assert(err, IsNil)

	getMinPosForSubTaskFunc = getFakePosForSubTask
}

func (t *testServer) TearDownSuite(c *C) {
	getMinPosForSubTaskFunc = getMinPosForSubTask
}

func createMockETCD(dir string, host string) (*embed.Etcd, error) {
	cfg := embed.NewConfig()
	cfg.Dir = dir
	// lpurl, _ := url.Parse(host)
	lcurl, _ := url.Parse(host)
	// cfg.LPUrls = []url.URL{*lpurl}
	cfg.LCUrls = []url.URL{*lcurl}
	cfg.ACUrls = []url.URL{*lcurl}
	cfg.Logger = "zap"
	metricsURL, _ := url.Parse(tempurl.Alloc())
	cfg.ListenMetricsUrls = []url.URL{*metricsURL}
	ETCD, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, err
	}

	select {
	case <-ETCD.Server.ReadyNotify():
	case <-time.After(5 * time.Second):
		ETCD.Server.Stop() // trigger a shutdown
	}
	// embd.client = v3client.New(embd.ETCD.Server)
	return ETCD, nil
}

func (t *testServer) TestServer(c *C) {
	var (
		masterAddr   = "127.0.0.1:8291"
		workerAddr1  = "127.0.0.1:8262"
		keepAliveTTL = int64(1)
	)
	etcdDir := c.MkDir()
	ETCD, err := createMockETCD(etcdDir, "host://"+masterAddr)
	defer ETCD.Close()
	c.Assert(err, IsNil)
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), IsNil)
	cfg.KeepAliveTTL = keepAliveTTL

	NewRelayHolder = NewDummyRelayHolder
	defer func() {
		NewRelayHolder = NewRealRelayHolder
	}()
	NewSubTask = func(cfg *config.SubTaskConfig, etcdClient *clientv3.Client) *SubTask {
		cfg.UseRelay = false
		return NewRealSubTask(cfg, etcdClient)
	}
	defer func() {
		NewSubTask = NewRealSubTask
	}()

	createUnits = func(cfg *config.SubTaskConfig, etcdClient *clientv3.Client) []unit.Unit {
		mockDumper := NewMockUnit(pb.UnitType_Dump)
		mockLoader := NewMockUnit(pb.UnitType_Load)
		mockSync := NewMockUnit(pb.UnitType_Sync)
		return []unit.Unit{mockDumper, mockLoader, mockSync}
	}
	defer func() {
		createUnits = createRealUnits
	}()

	s := NewServer(cfg)
	go func() {
		defer s.Close()
		err1 := s.Start()
		c.Assert(err1, IsNil)
	}()

	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return !s.closed.Get()
	}), IsTrue)
	dir := c.MkDir()

	t.testOperateWorker(c, s, dir, true)

	// check worker would retry connecting master rather than stop worker directly.
	ETCD = t.testRetryConnectMaster(c, s, ETCD, etcdDir, masterAddr)

	// resume contact with ETCD and start worker again
	t.testOperateWorker(c, s, dir, true)

	// test condition hub
	t.testConidtionHub(c, s)

	t.testHTTPInterface(c, "status")
	t.testHTTPInterface(c, "metrics")

	// create client
	cli := t.createClient(c, workerAddr1)

	// start task
	subtaskCfg := config.SubTaskConfig{}
	err = subtaskCfg.DecodeFile(subtaskSampleFile, true)
	c.Assert(err, IsNil)
	subtaskCfg.MydumperPath = mydumperPath

	sourceCfg := loadSourceConfigWithoutPassword(c)
	_, err = ha.PutSubTaskCfg(s.etcdClient, subtaskCfg)
	c.Assert(err, IsNil)
	_, err = ha.PutSubTaskCfgStage(s.etcdClient, []config.SubTaskConfig{subtaskCfg},
		[]ha.Stage{ha.NewSubTaskStage(pb.Stage_Running, sourceCfg.SourceID, subtaskCfg.Name)})
	c.Assert(err, IsNil)

	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return checkSubTaskStatus(cli, pb.Stage_Running)
	}), IsTrue)

	t.testSubTaskRecover(c, s, dir)

	// pause relay
	_, err = ha.PutRelayStage(s.etcdClient, ha.NewRelayStage(pb.Stage_Paused, sourceCfg.SourceID))
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return checkRelayStatus(cli, pb.Stage_Paused)
	}), IsTrue)
	// resume relay
	_, err = ha.PutRelayStage(s.etcdClient, ha.NewRelayStage(pb.Stage_Running, sourceCfg.SourceID))
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return checkRelayStatus(cli, pb.Stage_Running)
	}), IsTrue)
	// pause task
	_, err = ha.PutSubTaskStage(s.etcdClient, ha.NewSubTaskStage(pb.Stage_Paused, sourceCfg.SourceID, subtaskCfg.Name))
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return checkSubTaskStatus(cli, pb.Stage_Paused)
	}), IsTrue)
	// resume task
	_, err = ha.PutSubTaskStage(s.etcdClient, ha.NewSubTaskStage(pb.Stage_Running, sourceCfg.SourceID, subtaskCfg.Name))
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return checkSubTaskStatus(cli, pb.Stage_Running)
	}), IsTrue)
	// stop task
	_, err = ha.DeleteSubTaskStage(s.etcdClient, ha.NewSubTaskStage(pb.Stage_Stopped, sourceCfg.SourceID, subtaskCfg.Name))
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return s.getWorker(true).subTaskHolder.findSubTask(subtaskCfg.Name) == nil
	}), IsTrue)

	dupServer := NewServer(cfg)
	err = dupServer.Start()
	c.Assert(terror.ErrWorkerStartService.Equal(err), IsTrue)
	c.Assert(err.Error(), Matches, ".*bind: address already in use")

	t.testStopWorkerWhenLostConnect(c, s, ETCD)
	// close
	s.Close()

	c.Assert(utils.WaitSomething(30, 10*time.Millisecond, func() bool {
		return s.closed.Get()
	}), IsTrue)

	// test worker, just make sure testing sort
	t.testWorker(c)
}

func (t *testServer) testHTTPInterface(c *C, uri string) {
	resp, err := http.Get("http://127.0.0.1:8262/" + uri)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, 200)
	_, err = ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
}

func (t *testServer) createClient(c *C, addr string) pb.WorkerClient {
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBackoffMaxDelay(3*time.Second))
	c.Assert(err, IsNil)
	return pb.NewWorkerClient(conn)
}

func (t *testServer) testOperateWorker(c *C, s *Server, dir string, start bool) {
	// load sourceCfg
	sourceCfg := loadSourceConfigWithoutPassword(c)
	sourceCfg.EnableRelay = true
	sourceCfg.RelayDir = dir
	sourceCfg.MetaDir = c.MkDir()

	if start {
		// put mysql config into relative etcd key adapter to trigger operation event
		_, err := ha.PutSourceCfg(s.etcdClient, sourceCfg)
		c.Assert(err, IsNil)
		_, err = ha.PutRelayStageSourceBound(s.etcdClient, ha.NewRelayStage(pb.Stage_Running, sourceCfg.SourceID),
			ha.NewSourceBound(sourceCfg.SourceID, s.cfg.Name))
		c.Assert(err, IsNil)
		// worker should be started and without error
		c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
			w := s.getWorker(true)
			return w != nil && w.closed.Get() == closedFalse
		}), IsTrue)
		c.Assert(s.getSourceStatus(true).Result, IsNil)
	} else {
		// worker should be started before stopped
		w := s.getWorker(true)
		c.Assert(w, NotNil)
		c.Assert(w.closed.Get() == closedFalse, IsTrue)
		_, err := ha.DeleteSourceCfgRelayStageSourceBound(s.etcdClient, sourceCfg.SourceID, s.cfg.Name)
		c.Assert(err, IsNil)
		// worker should be started and without error
		c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
			currentWorker := s.getWorker(true)
			return currentWorker == nil && w.closed.Get() == closedTrue
		}), IsTrue)
		c.Assert(s.getSourceStatus(true).Result, IsNil)
	}
}

func (t *testServer) testRetryConnectMaster(c *C, s *Server, ETCD *embed.Etcd, dir string, hostName string) *embed.Etcd {
	ETCD.Close()
	time.Sleep(4 * time.Second)
	// When worker server fail to keepalive with etcd, sever should close its worker
	c.Assert(s.getWorker(true), IsNil)
	c.Assert(s.getSourceStatus(true).Result, IsNil)
	ETCD, err := createMockETCD(dir, "host://"+hostName)
	c.Assert(err, IsNil)
	time.Sleep(3 * time.Second)
	return ETCD
}

func (t *testServer) testSubTaskRecover(c *C, s *Server, dir string) {
	workerCli := t.createClient(c, "127.0.0.1:8262")
	t.testOperateWorker(c, s, dir, false)

	status, err := workerCli.QueryStatus(context.Background(), &pb.QueryStatusRequest{Name: "sub-task-name"})
	c.Assert(err, IsNil)
	c.Assert(status.Result, IsFalse)
	c.Assert(status.Msg, Equals, terror.ErrWorkerNoStart.Error())

	t.testOperateWorker(c, s, dir, true)
	status, err = workerCli.QueryStatus(context.Background(), &pb.QueryStatusRequest{Name: "sub-task-name"})
	c.Assert(err, IsNil)
	c.Assert(status.Result, IsTrue)
	c.Assert(status.SubTaskStatus[0].Stage, Equals, pb.Stage_Running)
}

func (t *testServer) testStopWorkerWhenLostConnect(c *C, s *Server, ETCD *embed.Etcd) {
	ETCD.Close()
	time.Sleep(retryConnectSleepTime + time.Duration(defaultKeepAliveTTL+3)*time.Second)
	c.Assert(s.getWorker(true), IsNil)
}

func (t *testServer) TestGetMinPosInAllSubTasks(c *C) {
	subTaskCfg := []*config.SubTaskConfig{
		{
			Name: "test2",
		}, {
			Name: "test3",
		}, {
			Name: "test1",
		},
	}
	minPos, err := getMinPosInAllSubTasks(context.Background(), subTaskCfg)
	c.Assert(err, IsNil)
	c.Assert(minPos.Name, Equals, "mysql-binlog.00001")
	c.Assert(minPos.Pos, Equals, uint32(12))
}

func getFakePosForSubTask(ctx context.Context, subTaskCfg *config.SubTaskConfig) (minPos *mysql.Position, err error) {
	switch subTaskCfg.Name {
	case "test1":
		return &mysql.Position{
			Name: "mysql-binlog.00001",
			Pos:  123,
		}, nil
	case "test2":
		return &mysql.Position{
			Name: "mysql-binlog.00001",
			Pos:  12,
		}, nil
	case "test3":
		return &mysql.Position{
			Name: "mysql-binlog.00003",
		}, nil
	default:
		return nil, nil
	}
}

func checkSubTaskStatus(cli pb.WorkerClient, expect pb.Stage) bool {
	status, err := cli.QueryStatus(context.Background(), &pb.QueryStatusRequest{Name: "sub-task-name"})
	if err != nil {
		return false
	}
	if status.Result == false {
		return false
	}
	return len(status.SubTaskStatus) > 0 && status.SubTaskStatus[0].Stage == expect
}

func checkRelayStatus(cli pb.WorkerClient, expect pb.Stage) bool {
	status, err := cli.QueryStatus(context.Background(), &pb.QueryStatusRequest{Name: "sub-task-name"})
	if err != nil {
		return false
	}
	if status.Result == false {
		return false
	}
	return status.SourceStatus.RelayStatus.Stage == expect
}

func loadSourceConfigWithoutPassword(c *C) config.SourceConfig {
	var sourceCfg config.SourceConfig
	err := sourceCfg.LoadFromFile(sourceSampleFile)
	c.Assert(err, IsNil)
	sourceCfg.From.Password = "" // no password set
	return sourceCfg
}
