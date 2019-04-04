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
	"io/ioutil"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
)

func TestWorker(t *testing.T) {
	TestingT(t)
}

type testWorker struct{}

var _ = Suite(&testWorker{})

func (t *testWorker) TestFileMetaDB(c *C) {
	dir := c.MkDir()

	metaDB, err := NewFileMetaDB(dir)
	c.Assert(err, IsNil)
	c.Assert(metaDB.meta.Tasks, HasLen, 0)
	c.Assert(metaDB.Close(), IsNil)

	// write old fashion meta
	oldMeta := &Meta{
		SubTasks: map[string]*config.SubTaskConfig{
			"task1": &config.SubTaskConfig{
				Name: "task1",
			},
		},
	}
	data, err := oldMeta.Toml()
	c.Assert(err, IsNil)
	err = ioutil.WriteFile(metaDB.path, []byte(data), 0644)
	c.Assert(err, IsNil)

	// recover from old fashion meta
	metaDB, err = NewFileMetaDB(dir)
	c.Assert(err, IsNil)
	c.Assert(metaDB.meta.Tasks, HasLen, 1)

	meta := metaDB.Load()
	c.Assert(meta.Tasks, HasLen, 1)

	err = metaDB.Set(&pb.TaskMeta{
		Name: "task2",
	})
	c.Assert(err, IsNil)

	// test load
	meta = metaDB.Load()
	c.Assert(meta.Tasks, HasLen, 2)
	c.Assert(meta.Tasks["task1"], NotNil)
	c.Assert(meta.Tasks["task2"], NotNil)

	// test get
	task1 := metaDB.Get("task1")
	c.Assert(task1, NotNil)
	c.Assert(task1.Name, Equals, "task1")
	task2 := metaDB.Get("task2")
	c.Assert(task2, NotNil)
	c.Assert(task2.Name, Equals, "task2")

	c.Assert(metaDB.Close(), IsNil)

	// reopen meta
	metaDB, err = NewFileMetaDB(dir)
	c.Assert(err, IsNil)
	c.Assert(metaDB.meta.Tasks, HasLen, 2)

	c.Assert(metaDB.Delete("task1"), IsNil)
	meta = metaDB.Load()
	c.Assert(meta.Tasks, HasLen, 1)

	task1 = metaDB.Get("task1")
	c.Assert(task1, IsNil)
	task2 = metaDB.Get("task2")
	c.Assert(task2, NotNil)
	c.Assert(task2.Name, Equals, "task2")

	c.Assert(metaDB.Close(), IsNil)
}
