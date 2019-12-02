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
)

func TestWorkerClient(t *testing.T) {
	TestingT(t)
}

type testWorkerClient struct {
}

var _ = Suite(&testWorkerClient{})

func (t *testWorkerClient) TestClientHub(c *C) {
	// static deployment
	var (
		source1   = "source-1"
		worker1   = "127.0.0.1:8262"
		worker2   = "not-exists"
		deployMap = map[string]string{
			source1: worker1,
		}
	)

	hub, err := NewClientHub(deployMap, nil)
	c.Assert(err, IsNil)
	c.Assert(hub, NotNil)

	// source with 1 dm-worker
	clis := hub.GetClientsBySourceID(source1)
	c.Assert(clis, HasLen, 1)
	cli := hub.GetClientByWorker(worker1)
	c.Assert(cli, DeepEquals, clis[0])
	workers := hub.AllWorkers()
	c.Assert(workers, HasLen, 1)
	c.Assert(workers[0], Equals, worker1)
	allClis := hub.AllClients()
	c.Assert(allClis, HasLen, 1)
	c.Assert(allClis[worker1], DeepEquals, clis[0])

	// source without dm-worker
	c.Assert(hub.GetClientsBySourceID("not-exists"), HasLen, 0)

	// not exists dm-worker
	c.Assert(hub.GetClientByWorker(worker2), IsNil)
	hub.SetClientForWorker(worker2, cli)
	c.Assert(hub.GetClientByWorker(worker2), Equals, cli)

	hub.Close()
	workers = hub.AllWorkers()
	c.Assert(workers, HasLen, 0)

	hub.Close() // try close again
}
