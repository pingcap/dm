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
	. "github.com/pingcap/check"
)

func (t *testMaster) TestAgentPool(c *C) {
	t.testPool(c)
	t.testEmit(c)
}

func (t *testMaster) testPool(c *C) {
	// test limit
	agentlimit = 2
	pc := make(chan *Agent)

	go func() {
		ap := GetAgentPool()
		pc <- ap.Apply()
		pc <- ap.Apply()
		pc <- ap.Apply()

	}()

	agent1 := <-pc
	c.Assert(agent1.ID, Equals, 1)
	agent2 := <-pc
	c.Assert(agent2.ID, Equals, 2)
	select {
	case <-pc:
		c.FailNow()
	default:
	}

	GetAgentPool().Recycle(agent1)
	agent := <-pc
	c.Assert(agent.ID, Equals, 1)
	GetAgentPool().Recycle(agent2)
	GetAgentPool().Recycle(agent)
}

func (t *testMaster) testEmit(c *C) {
	type testWorkerType int

	var (
		id                    = "worker-01"
		worker testWorkerType = 1
	)

	Emit(func(args ...interface{}) {
		if len(args) != 2 {
			c.Fatalf("args count is not 2, args %v", args)
		}

		id1, ok := args[0].(string)
		if !ok {
			c.Fatalf("args[0] is not id, args %+v", args)
		}
		if id != id1 {
			c.Fatalf("args[0] is expected id, args[0] %s vs %s", id1, id)
		}

		worker1, ok := args[1].(testWorkerType)
		if !ok {
			c.Fatalf("args[1] is not worker client, args %+v", args)
		}
		if worker1 != worker {
			c.Fatalf("args[1] is not expected worker, args[1] %v vs %v", worker1, worker)
		}
	}, []interface{}{id, worker}...)

}
