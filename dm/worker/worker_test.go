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
	"strings"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/utils"
)

var emptyWorkerStatusInfoJSONLength = 25

func (t *testServer) testWorker(c *C) {
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), IsNil)

	dir := c.MkDir()
	cfg.RelayDir = dir
	cfg.MetaDir = dir

	NewRelayHolder = NewDummyRelayHolderWithInitError
	defer func() {
		NewRelayHolder = NewRealRelayHolder
	}()

	_, err := NewWorker(cfg)
	c.Assert(err, ErrorMatches, "init error")

	NewRelayHolder = NewDummyRelayHolder
	w, err := NewWorker(cfg)
	c.Assert(err, IsNil)
	c.Assert(w.StatusJSON(""), HasLen, emptyWorkerStatusInfoJSONLength)
	c.Assert(w.closed.Get(), Equals, closedFalse)

	// close twice
	w.Close()
	c.Assert(w.closed.Get(), Equals, closedTrue)
	c.Assert(w.subTaskHolder.getAllSubTasks(), HasLen, 0)
	w.Close()
	c.Assert(w.closed.Get(), Equals, closedTrue)
	c.Assert(w.subTaskHolder.getAllSubTasks(), HasLen, 0)

	_, err = w.StartSubTask(&config.SubTaskConfig{
		Name: "testStartTask",
	})
	c.Assert(err, ErrorMatches, ".*worker already closed.*")

	_, err = w.UpdateSubTask(&config.SubTaskConfig{
		Name: "testStartTask",
	})
	c.Assert(err, ErrorMatches, ".*worker already closed.*")

	_, err = w.OperateSubTask("testSubTask", pb.TaskOp_Stop)
	c.Assert(err, ErrorMatches, ".*worker already closed.*")
}

func (t *testServer) testWorkerHandleTask(c *C) {
	var (
		wg       sync.WaitGroup
		taskName = "test"
	)

	NewRelayHolder = NewDummyRelayHolder
	dir := c.MkDir()
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), IsNil)
	cfg.RelayDir = dir
	cfg.MetaDir = dir
	w, err := NewWorker(cfg)
	c.Assert(err, IsNil)

	tasks := []*pb.TaskMeta{
		{Op: pb.TaskOp_Stop, Name: taskName, Stage: pb.Stage_New},
		{Op: pb.TaskOp_Pause, Name: taskName, Stage: pb.Stage_New},
		{Op: pb.TaskOp_Resume, Name: taskName, Stage: pb.Stage_New},
	}
	for _, task := range tasks {
		_, err := w.meta.AppendOperation(task)
		c.Assert(err, IsNil)
	}
	c.Assert(len(w.meta.logs), Equals, len(tasks))

	c.Assert(failpoint.Enable("github.com/pingcap/dm/dm/worker/handleTaskInterval", `return(10)`), IsNil)
	defer failpoint.Disable("github.com/pingcap/dm/dm/worker/handleTaskInterval")
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.handleTask()
	}()

	c.Assert(utils.WaitSomething(10, 100*time.Millisecond, func() bool {
		w.meta.Lock()
		defer w.meta.Unlock()
		return len(w.meta.logs) == 0
	}), IsTrue)

	w.Close()
	wg.Wait()
}

func (t *testServer) TestTaskAutoResume(c *C) {
	var (
		taskName = "sub-task-name"
		port     = 8263
	)
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), IsNil)
	cfg.Checker.CheckInterval = duration{Duration: 40 * time.Millisecond}
	cfg.Checker.BackoffMin = duration{Duration: 20 * time.Millisecond}
	cfg.Checker.BackoffMax = duration{Duration: 1 * time.Second}
	cfg.WorkerAddr = fmt.Sprintf(":%d", port)

	dir := c.MkDir()
	cfg.RelayDir = dir
	cfg.MetaDir = dir

	NewRelayHolder = NewDummyRelayHolder
	defer func() {
		NewRelayHolder = NewRealRelayHolder
	}()

	c.Assert(failpoint.Enable("github.com/pingcap/dm/mydumper/dumpUnitProcessForever", `return(true)`), IsNil)
	defer failpoint.Disable("github.com/pingcap/dm/mydumper/dumpUnitProcessForever")
	c.Assert(failpoint.Enable("github.com/pingcap/dm/mydumper/dumpUnitProcessWithError", `2*return("test auto resume inject error")`), IsNil)
	defer failpoint.Disable("github.com/pingcap/dm/mydumper/dumpUnitProcessWithError")
	c.Assert(failpoint.Enable("github.com/pingcap/dm/dm/worker/handleTaskInterval", `return(10)`), IsNil)
	defer failpoint.Disable("github.com/pingcap/dm/dm/worker/handleTaskInterval")
	c.Assert(failpoint.Enable("github.com/pingcap/dm/dm/worker/mockCreateUnitsDumpOnly", `return(true)`), IsNil)
	defer failpoint.Disable("github.com/pingcap/dm/dm/worker/mockCreateUnitsDumpOnly")
	c.Assert(failpoint.Enable("github.com/pingcap/dm/dm/worker/DisableCreateEtcdClient", `return()`), IsNil)
	defer failpoint.Disable("github.com/pingcap/dm/dm/worker/DisableCreateEtcdClient")
	c.Assert(failpoint.Enable("github.com/pingcap/dm/dm/worker/DisableCreateElection", `return()`), IsNil)
	defer failpoint.Disable("github.com/pingcap/dm/dm/worker/DisableCreateElection")

	s := NewServer(cfg)

	go func() {
		defer s.Close()
		c.Assert(s.Start(), IsNil)
	}()
	c.Assert(utils.WaitSomething(10, 100*time.Millisecond, func() bool {
		return !s.closed.Get()
	}), IsTrue)

	// start task
	cli := t.createClient(c, fmt.Sprintf("127.0.0.1:%d", port))
	subtaskCfgBytes, err := ioutil.ReadFile("./subtask.toml")
	// strings.Replace is used here to uncomment extra-args to avoid mydumper connecting to DB and generating arg --tables-list which will cause failure
	_, err = cli.StartSubTask(context.Background(), &pb.StartSubTaskRequest{Task: strings.Replace(string(subtaskCfgBytes), "#extra-args", "extra-args", 1)})
	c.Assert(err, IsNil)

	// check task in paused state
	c.Assert(utils.WaitSomething(10, 100*time.Millisecond, func() bool {
		for _, st := range s.worker.QueryStatus(taskName) {
			if st.Name == taskName && st.Stage == pb.Stage_Paused {
				return true
			}
		}
		return false
	}), IsTrue)

	rtsc, ok := s.worker.taskStatusChecker.(*realTaskStatusChecker)
	c.Assert(ok, IsTrue)
	defer func() {
		// close multiple time
		rtsc.Close()
		rtsc.Close()
	}()

	// check task will be auto resumed
	c.Assert(utils.WaitSomething(10, 100*time.Millisecond, func() bool {
		for _, st := range s.worker.QueryStatus(taskName) {
			if st.Name == taskName && st.Stage == pb.Stage_Running {
				return true
			}
		}
		return false
	}), IsTrue)
}

func (t *testServer) TestElection(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker1 := &Worker{
		cfg: &Config{
			Name:     "dm-worker1",
			SourceID: "source-1",
		},
		ctx: ctx,
	}
	worker1.etcdClient = etcdClient
	err := worker1.createElection()
	c.Assert(err, IsNil)

	// wait the dm-worker1 become the leader
	c.Assert(utils.WaitSomething(30, 10*time.Millisecond, func() bool {
		return worker1.election.IsLeader()
	}), IsTrue)

	worker2 := &Worker{
		cfg: &Config{
			Name:     "dm-worker2",
			SourceID: "source-1",
		},
		ctx: ctx,
	}
	worker2.etcdClient = etcdClient
	err = worker2.createElection()
	c.Assert(err, IsNil)
	// the dm-worker1 is still the leader
	c.Assert(worker1.election.IsLeader(), IsTrue)
	c.Assert(worker2.election.IsLeader(), IsFalse)

	worker3 := &Worker{
		cfg: &Config{
			Name:     "dm-worker3",
			SourceID: "source-2",
		},
		ctx: ctx,
	}
	worker3.etcdClient = etcdClient
	err = worker3.createElection()
	c.Assert(err, IsNil)
	// the dm-worker3 has different source ID, so dm-worker3 is leader for source-2, dm-worker1 is still the leader for source-1
	c.Assert(utils.WaitSomething(30, 10*time.Millisecond, func() bool {
		return worker3.election.IsLeader()
	}), IsTrue)
	c.Assert(worker1.election.IsLeader(), IsTrue)

	_, leaderID, err := worker1.election.LeaderInfo(ctx)
	c.Assert(leaderID, Equals, worker1.cfg.Name)
	_, leaderID, err = worker3.election.LeaderInfo(ctx)
	c.Assert(leaderID, Equals, worker3.cfg.Name)
}
