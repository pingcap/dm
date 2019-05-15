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
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
)

func TestWorker(t *testing.T) {
	TestingT(t)
}

var emptyWorkerStatusInfoJSONLength = 25

type testWorker struct{}

var _ = Suite(&testWorker{})

func (t *testWorker) TestWorker(c *C) {
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

	// start task
	c.Assert(w.meta.LoadTaskMeta(), HasLen, 0)
	c.Assert(w.meta.PeekLog(), IsNil)

	id1, err := w.StartSubTask(&config.SubTaskConfig{
		Name: "testStartTask",
	})
	c.Assert(err, IsNil)
	c.Assert(w.meta.PeekLog(), NotNil)
	c.Assert(w.meta.PeekLog().Id, Equals, id1)
	c.Assert(id1, Equals, int64(1))

	id2, err := w.UpdateSubTask(&config.SubTaskConfig{
		Name: "testStartTask",
	})
	c.Assert(err, IsNil)
	c.Assert(id2, Equals, int64(2))
	c.Assert(w.meta.PeekLog(), NotNil)
	c.Assert(w.meta.PeekLog().Id, Equals, id1)

	log2, err := w.meta.GetTaskLog(id2)
	c.Assert(err, IsNil)
	c.Assert(log2, NotNil)

	id3, err := w.OperateSubTask("testSubTask", pb.TaskOp_Stop)
	c.Assert(err, IsNil)
	c.Assert(id3, Equals, int64(3))
	c.Assert(w.meta.PeekLog(), NotNil)
	c.Assert(w.meta.PeekLog().Id, Equals, id1)

	log3, err := w.meta.GetTaskLog(id3)
	c.Assert(err, IsNil)
	c.Assert(log3, NotNil)

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
