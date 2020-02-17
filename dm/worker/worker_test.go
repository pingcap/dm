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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/utils"
)

var emptyWorkerStatusInfoJSONLength = 25

func (t *testServer) testWorker(c *C) {
	cfg := loadSourceConfigWithoutPassword(c)

	dir := c.MkDir()
	cfg.EnableRelay = true
	cfg.RelayDir = dir
	cfg.MetaDir = dir

	NewRelayHolder = NewDummyRelayHolderWithInitError
	defer func() {
		NewRelayHolder = NewRealRelayHolder
	}()

	_, err := NewWorker(&cfg, nil)
	c.Assert(err, ErrorMatches, "init error")

	NewRelayHolder = NewDummyRelayHolder
	w, err := NewWorker(&cfg, nil)
	c.Assert(err, IsNil)
	c.Assert(w.StatusJSON(""), HasLen, emptyWorkerStatusInfoJSONLength)
	//c.Assert(w.closed.Get(), Equals, closedFalse)
	//go func() {
	//	w.Start()
	//}()

	// close twice
	w.Close()
	c.Assert(w.closed.Get(), Equals, closedTrue)
	c.Assert(w.subTaskHolder.getAllSubTasks(), HasLen, 0)
	w.Close()
	c.Assert(w.closed.Get(), Equals, closedTrue)
	c.Assert(w.subTaskHolder.getAllSubTasks(), HasLen, 0)
	c.Assert(w.closed.Get(), Equals, closedTrue)

	w.StartSubTask(&config.SubTaskConfig{
		Name: "testStartTask",
	})
	task := w.subTaskHolder.findSubTask("testStartTask")
	c.Assert(task, NotNil)
	c.Assert(task.Result().String(), Matches, ".*worker already closed.*")

	err = w.UpdateSubTask(&config.SubTaskConfig{
		Name: "testStartTask",
	})
	c.Assert(err, ErrorMatches, ".*worker already closed.*")

	err = w.OperateSubTask("testSubTask", pb.TaskOp_Stop)
	c.Assert(err, ErrorMatches, ".*worker already closed.*")
}

func (t *testServer) TestTaskAutoResume(c *C) {
	var (
		taskName = "sub-task-name"
		port     = 8263
	)
	hostName := "127.0.0.1:8291"
	etcdDir := c.MkDir()
	ETCD, err := createMockETCD(etcdDir, "host://"+hostName)
	c.Assert(err, IsNil)
	defer ETCD.Close()

	cfg := NewConfig()
	sourceConfig := loadSourceConfigWithoutPassword(c)
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), IsNil)
	sourceConfig.Checker.CheckEnable = true
	sourceConfig.Checker.CheckInterval = config.Duration{Duration: 40 * time.Millisecond}
	sourceConfig.Checker.BackoffMin = config.Duration{Duration: 20 * time.Millisecond}
	sourceConfig.Checker.BackoffMax = config.Duration{Duration: 1 * time.Second}

	cfg.WorkerAddr = fmt.Sprintf(":%d", port)

	dir := c.MkDir()
	sourceConfig.RelayDir = dir
	sourceConfig.MetaDir = dir
	sourceConfig.EnableRelay = true

	NewRelayHolder = NewDummyRelayHolder
	defer func() {
		NewRelayHolder = NewRealRelayHolder
	}()

	c.Assert(failpoint.Enable("github.com/pingcap/dm/mydumper/dumpUnitProcessForever", `return(true)`), IsNil)
	defer failpoint.Disable("github.com/pingcap/dm/mydumper/dumpUnitProcessForever")
	c.Assert(failpoint.Enable("github.com/pingcap/dm/mydumper/dumpUnitProcessWithError", `2*return("test auto resume inject error")`), IsNil)
	defer failpoint.Disable("github.com/pingcap/dm/mydumper/dumpUnitProcessWithError")
	c.Assert(failpoint.Enable("github.com/pingcap/dm/dm/worker/mockCreateUnitsDumpOnly", `return(true)`), IsNil)
	defer failpoint.Disable("github.com/pingcap/dm/dm/worker/mockCreateUnitsDumpOnly")
	c.Assert(failpoint.Enable("github.com/pingcap/dm/loader/ignoreLoadCheckpointErr", `return()`), IsNil)
	defer failpoint.Disable("github.com/pingcap/dm/loader/ignoreLoadCheckpointErr")

	s := NewServer(cfg)

	go func() {
		defer s.Close()
		c.Assert(s.Start(), IsNil)
	}()
	c.Assert(utils.WaitSomething(10, 100*time.Millisecond, func() bool {
		if s.closed.Get() {
			return false
		}
		c.Assert(s.startWorker(&sourceConfig), IsNil)
		return true
	}), IsTrue)
	// start task
	cli := t.createClient(c, fmt.Sprintf("127.0.0.1:%d", port))
	subtaskCfgBytes, err := ioutil.ReadFile("./subtask.toml")
	// strings.Replace is used here to uncomment extra-args to avoid mydumper connecting to DB and generating arg --tables-list which will cause failure
	_, err = cli.StartSubTask(context.Background(), &pb.StartSubTaskRequest{Task: strings.Replace(string(subtaskCfgBytes), "#extra-args", "extra-args", 1)})
	c.Assert(err, IsNil)

	// check task in paused state
	c.Assert(utils.WaitSomething(100, 100*time.Millisecond, func() bool {
		for _, st := range s.getWorker(true).QueryStatus(taskName) {
			if st.Name == taskName && st.Stage == pb.Stage_Paused {
				return true
			}
		}
		return false
	}), IsTrue)

	rtsc, ok := s.getWorker(true).taskStatusChecker.(*realTaskStatusChecker)
	c.Assert(ok, IsTrue)
	defer func() {
		// close multiple time
		rtsc.Close()
		rtsc.Close()
	}()

	// check task will be auto resumed
	c.Assert(utils.WaitSomething(10, 100*time.Millisecond, func() bool {
		sts := s.getWorker(true).QueryStatus(taskName)
		for _, st := range sts {
			if st.Name == taskName && st.Stage == pb.Stage_Running {
				return true
			}
		}
		c.Log(sts)
		return false
	}), IsTrue)
}
