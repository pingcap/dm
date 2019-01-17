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

func (t *testMaster) TestLockKeeper(c *C) {
	type Case struct {
		task   string
		schema string
		table  string
	}
	var total = 20
	workers := make([]string, total)
	for i := 0; i < total; i++ {
		workers[i] = strconv.Itoa(i)
	}

	cases := []Case{
		{"taskA", "testdb1", "testtbl1"},
		{"taskB", "testdb1", "testtbl2"},
	}

	lk := NewLockKeeper()

	c.Assert(lk.FindLock("lockID"), IsNil)
	c.Assert(lk.RemoveLock("lockID"), IsFalse)

	var wg sync.WaitGroup
	for _, cs := range cases {
		for i := 0; i < total-1; i++ {
			wg.Add(1)
			go func(cs Case, i int) {
				defer wg.Done()
				id, synced, remain, err := lk.TrySync(cs.task, cs.schema, cs.table, workers[i], []string{"stmt"}, workers)
				c.Assert(err, IsNil)
				c.Assert(synced, IsFalse)
				c.Assert(remain, Greater, 0) // multi-goroutines TrySync concurrently, can only confirm remain > 0
				c.Assert(lk.FindLock(id), NotNil)
			}(cs, i)
		}
	}
	wg.Wait()

	for _, cs := range cases {
		id, synced, remain, err := lk.TrySync(cs.task, cs.schema, cs.table, workers[len(workers)-1], []string{"stmt"}, workers)
		c.Assert(err, IsNil)
		c.Assert(synced, IsTrue)
		c.Assert(remain, Equals, 0)
		c.Assert(lk.FindLock(id), NotNil)
		c.Assert(lk.RemoveLock(id), IsTrue)
		c.Assert(lk.FindLock(id), IsNil)
	}
}
