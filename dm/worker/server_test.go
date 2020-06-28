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
	"github.com/pingcap/errors"
	"google.golang.org/grpc"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

const subtaskSampleFile = "./subtask.toml"

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

	// test condition hub
	t.testConidtionHub(c, s)

	t.testHTTPInterface(c, "status")
	t.testHTTPInterface(c, "metrics")

	// create client
	cli := t.createClient(c, "127.0.0.1:8262")

	c.Assert(s.worker.meta.LoadTaskMeta(), HasLen, 0)
	c.Assert(s.worker.meta.PeekLog(), IsNil)

	// start task
	subtaskCfgBytes, err := ioutil.ReadFile(subtaskSampleFile)
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

	dupServer := NewServer(cfg)
	err = dupServer.Start()
	c.Assert(terror.ErrWorkerStartService.Equal(err), IsTrue)
	c.Assert(err.Error(), Matches, ".*bind: address already in use.*")

	// close
	s.Close()

	c.Assert(utils.WaitSomething(30, 10*time.Millisecond, func() bool {
		return s.closed.Get()
	}), IsTrue)

	// test worker, just make sure testing sort
	t.testWorker(c)
	t.testWorkerHandleTask(c)
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

func (t *testServer) TestQueryError(c *C) {
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), IsNil)

	s := NewServer(cfg)
	s.closed.Set(false)

	w := &Worker{
		cfg:         cfg,
		relayHolder: NewRelayHolder(cfg),
		// initial relay holder, the cfg's password will be decrypted in NewRelayHolder
		subTaskHolder: newSubTaskHolder(),
	}
	w.closed.Set(closedFalse)

	subtaskCfg := config.SubTaskConfig{}
	err := subtaskCfg.DecodeFile(subtaskSampleFile, true)
	c.Assert(err, IsNil)

	// subtask failed just after it is started
	st := NewSubTask(&subtaskCfg)
	st.fail(errors.New("mockSubtaskFail"))
	w.subTaskHolder.recordSubTask(st)
	s.worker = w

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := s.QueryError(ctx, &pb.QueryErrorRequest{})
	c.Assert(err, IsNil)
	c.Assert(resp, NotNil)
	c.Assert(resp.Result, IsTrue)
	c.Assert(resp.Msg, HasLen, 0)
	c.Assert(resp.SubTaskError, HasLen, 1)
	c.Assert(resp.SubTaskError[0].String(), Matches, `[\s\S]*mockSubtaskFail[\s\S]*`)
}
