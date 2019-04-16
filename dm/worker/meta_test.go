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
	c.Assert(metaDB.meta.SubTasks, HasLen, 0)

	meta := metaDB.Get()
	c.Assert(meta.SubTasks, HasLen, 0)

	err = metaDB.Set(&config.SubTaskConfig{
		Name:     "task1",
		SourceID: "source-1",
	})
	c.Assert(err, IsNil)

	meta = metaDB.Get()
	c.Assert(meta.SubTasks, HasLen, 1)
	c.Assert(meta.SubTasks["task1"], NotNil)

	c.Assert(metaDB.Close(), IsNil)

	metaDB, err = NewFileMetaDB(dir)
	c.Assert(err, IsNil)
	c.Assert(metaDB.meta.SubTasks, HasLen, 1)
	c.Assert(meta.SubTasks["task1"], NotNil)

	c.Assert(metaDB.Del("task1"), IsNil)
	meta = metaDB.Get()
	c.Assert(meta.SubTasks, HasLen, 0)
}
