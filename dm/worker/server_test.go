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
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/dm/dm/pb"
	"google.golang.org/grpc"
)

func TestServer(t *testing.T) {
	TestingT(t)
}

type testServer struct{}

var _ = Suite(&testServer{})

func (t *testServer) TestServer(c *C) {
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), IsNil)

	dir := c.MkDir()
	cfg.RelayDir = dir
	cfg.MetaDir = dir

	NewRelayHolder = NewDummyRelayHolder
	defer func() {
		NewRelayHolder = NewRealRelayHolder
	}()

	s := NewServer(cfg)

	go func() {
		err1 := s.Start()
		c.Assert(err1, IsNil)
	}()

	c.Assert(waitSomething(100, func() bool {
		return !s.closed.Get()
	}), IsTrue)

	// test condition hub
	t.testConidtionHub(c, s)

	t.testHTTPInterface(c, "status")
	t.testHTTPInterface(c, "metrics")

	// create client
	cli := t.createClient(c)

	c.Assert(s.worker.meta.LoadTaskMeta(), HasLen, 0)
	c.Assert(s.worker.meta.PeekLog(), IsNil)

	// start task
	subtaskCfgBytes, err := ioutil.ReadFile("./subtask.toml")
	c.Assert(err, IsNil)

	resp1, err := cli.StartSubTask(context.Background(), &pb.StartSubTaskRequest{
		Task: string(subtaskCfgBytes),
	})
	c.Assert(err, IsNil)
	c.Assert(s.worker.meta.PeekLog(), NotNil)
	c.Assert(s.worker.meta.PeekLog().Id, Equals, resp1.LogID)
	c.Assert(resp1.LogID, Equals, int64(1))

	opsp, err := cli.QueryTaskOperation(context.Background(), &pb.QueryTaskOperationRequest{
		Name:  "sub-task-name",
		LogID: resp1.LogID,
	})
	c.Assert(err, IsNil)
	c.Assert(opsp.Log.Success, IsFalse)

	// update task
	resp2, err := cli.UpdateSubTask(context.Background(), &pb.UpdateSubTaskRequest{
		Task: string(subtaskCfgBytes),
	})
	c.Assert(err, IsNil)
	c.Assert(resp2.LogID, Equals, int64(2))
	c.Assert(s.worker.meta.PeekLog(), NotNil)
	c.Assert(s.worker.meta.PeekLog().Id, Equals, resp1.LogID)

	opsp, err = cli.QueryTaskOperation(context.Background(), &pb.QueryTaskOperationRequest{
		Name:  "sub-task-name",
		LogID: resp2.LogID,
	})
	c.Assert(err, IsNil)
	c.Assert(opsp.Log.Success, IsFalse)

	// operate task
	resp3, err := cli.OperateSubTask(context.Background(), &pb.OperateSubTaskRequest{
		Name: "sub-task-name",
		Op:   pb.TaskOp_Pause,
	})
	c.Assert(err, IsNil)
	c.Assert(resp3.LogID, Equals, int64(3))
	c.Assert(s.worker.meta.PeekLog(), NotNil)
	c.Assert(s.worker.meta.PeekLog().Id, Equals, resp1.LogID)

	opsp, err = cli.QueryTaskOperation(context.Background(), &pb.QueryTaskOperationRequest{
		Name:  "test",
		LogID: resp3.LogID,
	})
	c.Assert(err, IsNil)
	c.Assert(opsp.Log.Success, IsFalse)

	// close
	s.Close()

	c.Assert(waitSomething(10, func() bool {
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

func (t *testServer) createClient(c *C) pb.WorkerClient {
	conn, err := grpc.Dial("127.0.0.1:8262", grpc.WithInsecure(), grpc.WithBackoffMaxDelay(3*time.Second))
	c.Assert(err, IsNil)
	return pb.NewWorkerClient(conn)
}

func waitSomething(backoff int, fn func() bool) bool {
	for i := 0; i < backoff; i++ {
		if fn() {
			return true
		}

		time.Sleep(10 * time.Millisecond)
	}

	return false
}
