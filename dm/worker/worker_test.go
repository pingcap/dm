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
	c.Assert(w.subTasks, IsNil)
	w.Close()
	c.Assert(w.closed.Get(), Equals, closedTrue)
	c.Assert(w.subTasks, IsNil)

	_, err = w.StartSubTask(&config.SubTaskConfig{
		Name: "testStartTask",
	})
	c.Assert(err, ErrorMatches, "worker already closed.*")

	_, err = w.UpdateSubTask(&config.SubTaskConfig{
		Name: "testStartTask",
	})
	c.Assert(err, ErrorMatches, "worker already closed.*")

	_, err = w.OperateSubTask("testSubTask", pb.TaskOp_Stop)
	c.Assert(err, ErrorMatches, "worker already closed.*")
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

	c.Assert(failpoint.Enable("github.com/pingcap/dm/dm/worker/handleTaskInternal", `return(10)`), IsNil)
	defer failpoint.Disable("github.com/pingcap/dm/dm/worker/handleTaskInternal")
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.handleTask()
	}()

	c.Assert(utils.WaitSomething(5, 10*time.Millisecond, func() bool {
		w.meta.Lock()
		defer w.meta.Unlock()
		return len(w.meta.logs) == 0
	}), IsTrue)

	w.Close()
	wg.Wait()
}
