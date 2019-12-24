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
	"github.com/pingcap/dm/dm/config"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"google.golang.org/grpc"

	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

func TestServer(t *testing.T) {
	TestingT(t)
}

type testServer struct{}

var _ = Suite(&testServer{})

func (t *testServer) TestServer(c *C) {
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), IsNil)


	NewRelayHolder = NewDummyRelayHolder
	defer func() {
		NewRelayHolder = NewRealRelayHolder
	}()

	s := NewServer(cfg)

	workerAddr := cfg.WorkerAddr
	s.cfg.WorkerAddr = ""
	err := s.Start()
	c.Assert(terror.ErrWorkerHostPortNotValid.Equal(err), IsTrue)
	s.Close()
	s.cfg.WorkerAddr = workerAddr

	go func() {
		err1 := s.Start()
		c.Assert(err1, IsNil)
	}()

	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return !s.closed.Get()
	}), IsTrue)
	t.testOperateWorker(c, s, true)

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

	t.testOperateWorker(c, s, false)
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

func (t *testServer) testOperateWorker(c *C, s *Server, start bool) {
	dir := c.MkDir()
	workerCfg := &config.MysqlConfig{}
	workerCfg.LoadFromFile("./dm-mysql.toml")
	workerCfg.RelayDir = dir
	workerCfg.MetaDir = dir
	cli := t.createClient(c, "127.0.0.1:8262")
	task, err := workerCfg.Toml()
	c.Assert(err, IsNil)
	req := &pb.MysqlTaskRequest{
		Op: pb.WorkerOp_UpdateConfig,
		Config: task,
	}
	if start {
		resp, err := cli.OperateMysqlTask(context.Background(), req)
		c.Assert(err, IsNil)
		c.Assert(resp.Result, Equals, false)
		c.Assert(resp.Msg, Matches, ".*Mysql task has not been created, please call CreateMysqlTask")
		req.Op = pb.WorkerOp_StartWorker
		resp, err = cli.OperateMysqlTask(context.Background(), req)
		c.Assert(err, IsNil)
		c.Assert(resp.Result, Equals, true)

		req.Op = pb.WorkerOp_UpdateConfig
		resp, err = cli.OperateMysqlTask(context.Background(), req)
		c.Assert(err, IsNil)
		c.Assert(resp.Result, Equals, true)
	} else {
		req.Op = pb.WorkerOp_StopWorker
		resp, err := cli.OperateMysqlTask(context.Background(), req)
		c.Assert(err, IsNil)
		c.Assert(resp.Result, Equals, true)
	}
}

