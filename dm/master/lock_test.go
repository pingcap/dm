// Copyright 2018 PingCAP, Inc.
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

	l := NewLock("test_id", "test_task", "test_owner", workers)

	synced, remain := l.IsSync()
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, total)

	var wg sync.WaitGroup
	for i := 0; i < total-1; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			synced, _ = l.IsSync()
			c.Assert(synced, IsFalse)

			worker := workers[i]
			ready := l.Ready()
			c.Assert(ready[worker], IsFalse)

			synced2, remain1 := l.TrySync(worker, workers)
			c.Assert(synced2, IsFalse)
			ready = l.Ready()
			c.Assert(ready[worker], IsTrue)

			synced2, remain2 := l.TrySync(worker, workers) //re-TrySync
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

	synced, _ = l.TrySync(workers[len(workers)-1], workers)
	c.Assert(synced, IsTrue)

	synced, remain = l.IsSync()
	c.Assert(synced, IsTrue)
	c.Assert(remain, Equals, 0)

	// add new workers when in syncing
	newWorker := "new-worker"
	synced, remain = l.TrySync(workers[0], []string{newWorker})
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 1)

	synced, _ = l.TrySync(newWorker, workers)
	c.Assert(synced, IsTrue)
}
