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
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"

	. "github.com/pingcap/check"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"google.golang.org/grpc"
)

func TestServer(t *testing.T) {
	TestingT(t)
}

type testServer struct{}

var _ = Suite(&testServer{})

func createMockETCD(dir string, host string) (*embed.Etcd, error) {
	cfg := embed.NewConfig()
	cfg.Dir = dir
	// lpurl, _ := url.Parse(host)
	lcurl, _ := url.Parse(host)
	// cfg.LPUrls = []url.URL{*lpurl}
	cfg.LCUrls = []url.URL{*lcurl}
	cfg.ACUrls = []url.URL{*lcurl}
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
	hostName := "127.0.0.1:8291"
	etcdDir := c.MkDir()
	ETCD, err := createMockETCD(etcdDir, "host://"+hostName)
	c.Assert(err, IsNil)
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), IsNil)

	NewRelayHolder = NewDummyRelayHolder
	defer func() {
		NewRelayHolder = NewRealRelayHolder
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

	// check infos have be written into ETCD success.
	t.testInfosInEtcd(c, hostName, cfg.AdvertiseAddr, dir)

	// check worker would retry connecting master rather than stop worker directly.
	ETCD = t.testRetryConnectMaster(c, s, ETCD, etcdDir, hostName)

	// test condition hub
	t.testConidtionHub(c, s)

	t.testHTTPInterface(c, "status")
	t.testHTTPInterface(c, "metrics")

	// create client
	cli := t.createClient(c, "127.0.0.1:8262")

	// start task
	subtaskCfgBytes, err := ioutil.ReadFile("./subtask.toml")
	c.Assert(err, IsNil)

	resp1, err := cli.StartSubTask(context.Background(), &pb.StartSubTaskRequest{
		Task: string(subtaskCfgBytes),
	})
	c.Assert(err, IsNil)
	c.Assert(resp1.Result, IsTrue)

	status, err := cli.QueryStatus(context.Background(), &pb.QueryStatusRequest{Name: "sub-task-name"})
	c.Assert(err, IsNil)
	c.Assert(status.Result, IsTrue)
	c.Assert(status.SubTaskStatus[0].Stage, Equals, pb.Stage_Paused) //  because of `Access denied`

	t.testSubTaskRecover(c, s, dir, hostName, string(subtaskCfgBytes))

	// update task
	resp2, err := cli.UpdateSubTask(context.Background(), &pb.UpdateSubTaskRequest{
		Task: string(subtaskCfgBytes),
	})
	c.Assert(err, IsNil)
	c.Assert(resp2.Result, IsTrue)

	// operate task
	resp3, err := cli.OperateSubTask(context.Background(), &pb.OperateSubTaskRequest{
		Name: "sub-task-name",
		Op:   pb.TaskOp_Pause,
	})
	c.Assert(err, IsNil)
	c.Assert(resp3.Result, IsFalse)
	c.Assert(resp3.Msg, Matches, ".*current stage is not running.*")

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
	workerCfg := &config.MysqlConfig{}
	err := workerCfg.LoadFromFile("./dm-mysql.toml")
	c.Assert(err, IsNil)
	workerCfg.RelayDir = dir
	workerCfg.MetaDir = dir
	cli := t.createClient(c, "127.0.0.1:8262")
	task, err := workerCfg.Toml()
	c.Assert(err, IsNil)
	req := &pb.MysqlWorkerRequest{
		Op:     pb.WorkerOp_UpdateConfig,
		Config: task,
	}
	if start {
		resp, err := cli.OperateMysqlWorker(context.Background(), req)
		c.Assert(err, IsNil)
		c.Assert(resp.Result, Equals, false)
		c.Assert(resp.Msg, Matches, ".*Mysql task has not been created, please call CreateMysqlTask.*")
		req.Op = pb.WorkerOp_StartWorker
		resp, err = cli.OperateMysqlWorker(context.Background(), req)
		c.Assert(err, IsNil)
		fmt.Println(resp.Msg)
		c.Assert(resp.Result, Equals, true)

		req.Op = pb.WorkerOp_UpdateConfig
		resp, err = cli.OperateMysqlWorker(context.Background(), req)
		c.Assert(err, IsNil)
		c.Assert(resp.Result, Equals, true)
		c.Assert(s.worker, NotNil)
	} else {
		req.Op = pb.WorkerOp_StopWorker
		resp, err := cli.OperateMysqlWorker(context.Background(), req)
		c.Assert(err, IsNil)
		c.Assert(resp.Result, Equals, true)
	}
}

func (t *testServer) testInfosInEtcd(c *C, hostName string, workerAddr string, dir string) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:            GetJoinURLs(hostName),
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    keepaliveTime,
		DialKeepAliveTimeout: keepaliveTimeout,
	})
	c.Assert(err, IsNil)
	cfg := &config.MysqlConfig{}
	err = cfg.LoadFromFile("./dm-mysql.toml")
	cfg.RelayDir = dir
	cfg.MetaDir = dir

	c.Assert(err, IsNil)
	task, err := cfg.Toml()
	c.Assert(err, IsNil)

	resp, err := cli.Get(context.Background(), common.UpstreamConfigKeyAdapter.Encode(cfg.SourceID))
	c.Assert(err, IsNil)
	c.Assert(len(resp.Kvs), Equals, 1)
	c.Assert(string(resp.Kvs[0].Value), Equals, task)

	resp, err = cli.Get(context.Background(), common.UpstreamBoundWorkerKeyAdapter.Encode(workerAddr))
	c.Assert(err, IsNil)
	c.Assert(len(resp.Kvs), Equals, 1)
	c.Assert(string(resp.Kvs[0].Value), Equals, cfg.SourceID)
}

func (t *testServer) testRetryConnectMaster(c *C, s *Server, ETCD *embed.Etcd, dir string, hostName string) *embed.Etcd {
	ETCD.Close()
	time.Sleep(3 * time.Second)
	c.Assert(s.checkWorkerStart(), NotNil)
	// retryConnectMaster is false means that this worker has been tried to connect to master again.
	c.Assert(s.retryConnectMaster.Get(), IsFalse)
	ETCD, err := createMockETCD(dir, "host://"+hostName)
	c.Assert(err, IsNil)
	time.Sleep(3 * time.Second)
	return ETCD
}

func (t *testServer) testSubTaskRecover(c *C, s *Server, dir string, hostName string, subCfgStr string) {
	cfg := &config.MysqlConfig{}
	err := cfg.LoadFromFile("./dm-mysql.toml")
	c.Assert(err, IsNil)
	cfg.RelayDir = dir
	cfg.MetaDir = dir
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:            GetJoinURLs(hostName),
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    keepaliveTime,
		DialKeepAliveTimeout: keepaliveTimeout,
	})
	c.Assert(err, IsNil)

	subCfg := config.NewSubTaskConfig()
	err = subCfg.Decode(subCfgStr)
	c.Assert(err, IsNil)
	c.Assert(cfg.SourceID, Equals, subCfg.SourceID)

	{
		resp, err := cli.Get(context.Background(), common.UpstreamSubTaskKeyAdapter.Encode(cfg.SourceID), clientv3.WithPrefix())
		c.Assert(err, IsNil)
		c.Assert(len(resp.Kvs), Equals, 1)
		infos, err := common.UpstreamSubTaskKeyAdapter.Decode(string(resp.Kvs[0].Key))
		c.Assert(err, IsNil)
		c.Assert(infos[1], Equals, subCfg.Name)
		task := string(resp.Kvs[0].Value)
		c.Assert(task, Equals, subCfgStr)
	}

	workerCli := t.createClient(c, "127.0.0.1:8262")
	mysqlTask, err := cfg.Toml()
	c.Assert(err, IsNil)
	req := &pb.MysqlWorkerRequest{
		Op:     pb.WorkerOp_StopWorker,
		Config: mysqlTask,
	}

	resp, err := workerCli.OperateMysqlWorker(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.Result, Equals, true)

	status, err := workerCli.QueryStatus(context.Background(), &pb.QueryStatusRequest{Name: "sub-task-name"})
	c.Assert(err, IsNil)
	c.Assert(status.Result, IsFalse)
	c.Assert(status.Msg, Equals, terror.ErrWorkerNoStart.Error())

	req.Op = pb.WorkerOp_StartWorker
	resp, err = workerCli.OperateMysqlWorker(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.Result, Equals, true)

	status, err = workerCli.QueryStatus(context.Background(), &pb.QueryStatusRequest{Name: "sub-task-name"})
	c.Assert(err, IsNil)
	c.Assert(status.Result, IsTrue)
	c.Assert(status.SubTaskStatus[0].Stage, Equals, pb.Stage_Paused) //  because of `Access denied`
}

func (t *testServer) testStopWorkerWhenLostConnect(c *C, s *Server, ETCD *embed.Etcd) {
	c.Assert(s.retryConnectMaster.Get(), IsTrue)
	ETCD.Close()
	time.Sleep(retryConnectSleepTime + time.Duration(defaultKeepAliveTTL+3)*time.Second)
	c.Assert(s.checkWorkerStart(), IsNil)
	c.Assert(s.retryConnectMaster.Get(), IsFalse)
}
