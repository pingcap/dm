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

package master

import (
	"strconv"
	"sync"

	. "github.com/pingcap/check"
)

func (t *testMaster) TestSyncLock(c *C) {
	var total = 100
	workers := make([]string, total)
	for i := 0; i < total; i++ {
		workers[i] = strconv.Itoa(i)
	}

	l := NewLock("test_id", "test_task", "test_owner", []string{"stmt"}, workers)

	synced, remain := l.IsSync()
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, total)

	var wg sync.WaitGroup
	for i := 0; i < total-1; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			synced2, _ := l.IsSync()
			c.Assert(synced2, IsFalse)

			worker := workers[i]
			ready := l.Ready()
			c.Assert(ready[worker], IsFalse)

			synced2, remain1, err := l.TrySync(worker, workers, []string{"stmts"})
			c.Assert(err, IsNil)
			c.Assert(synced2, IsFalse)
			ready = l.Ready()
			c.Assert(ready[worker], IsTrue)

			synced2, remain2, err := l.TrySync(worker, workers, []string{"stmts"}) //re-TrySync
			c.Assert(err, IsNil)
			c.Assert(synced2, IsFalse)
			c.Assert(remain2, LessEqual, remain1)

			synced2, _ = l.IsSync()
			c.Assert(synced2, IsFalse)
		}(i)
	}
	wg.Wait()

	synced, remain = l.IsSync()
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 1)

	synced, _, err := l.TrySync(workers[len(workers)-1], workers, []string{"stmts"})
	c.Assert(err, IsNil)
	c.Assert(synced, IsTrue)

	synced, remain = l.IsSync()
	c.Assert(synced, IsTrue)
	c.Assert(remain, Equals, 0)

	// add mismatch ddls
	_, _, err = l.TrySync(workers[0], workers, []string{"mismatch ddls"})
	c.Assert(err, NotNil)
	// add new workers when in syncing
	newWorker := "new-worker"
	synced, remain, err = l.TrySync(workers[0], []string{newWorker}, []string{"stmts"})
	c.Assert(err, IsNil)
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 1)

	synced, _, err = l.TrySync(newWorker, workers, []string{"stmts"})
	c.Assert(err, IsNil)
	c.Assert(synced, IsTrue)
}
