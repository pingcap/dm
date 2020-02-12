// Copyright 2020 PingCAP, Inc.
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

package ha

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/dm/pkg/utils"

	. "github.com/pingcap/check"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

func (t *testForEtcd) TestWorkerKeepAlive(c *C) {
	defer clearTestInfoOperation(c)
	rev, err := GetKeepAliveRev(etcdTestCli)
	c.Assert(err, IsNil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	timeout := 200 * time.Millisecond
	evCh := make(chan workerEvent, 10)
	closed := make(chan struct{})
	finished := int32(0)

	go func() {
		err1 := WatchWorkerEvent(ctx, etcdTestCli, rev, evCh)
		c.Assert(err1, IsNil)
		close(closed)
	}()

	cancels := make([]context.CancelFunc, 0, 5)
	for i := 1; i <= 5; i++ {
		worker := "worker" + strconv.Itoa(i)
		curTime := time.Now()
		ctx1, cancel1 := context.WithCancel(ctx)
		cancels = append(cancels, cancel1)
		go func(ctx context.Context) {
			err1 := KeepAlive(ctx, etcdTestCli, worker)
			c.Assert(err1, IsNil)
			atomic.AddInt32(&finished, 1)
		}(ctx1)

		select {
		case ev := <-evCh:
			c.Assert(ev.eventType, Equals, mvccpb.PUT)
			c.Assert(ev.workerName, Equals, worker)
			c.Assert(ev.joinTime.After(curTime), IsTrue)
		case <-time.After(timeout):
			c.Fatal("fail to receive put ev " + strconv.Itoa(i) + " before timeout")
		}
	}

	for i, cancel1 := range cancels {
		worker := "worker" + strconv.Itoa(i+1)
		cancel1()
		select {
		case ev := <-evCh:
			c.Assert(ev.eventType, Equals, mvccpb.DELETE)
			c.Assert(ev.workerName, Equals, worker)
		case <-time.After(5 * time.Second):
			c.Fatal("fail to receive delete ev " + strconv.Itoa(i+1) + " before timeout")
		}
	}

	waitKeepAliveQuit := utils.WaitSomething(100, timeout, func() bool {
		return atomic.LoadInt32(&finished) == 5
	})
	c.Assert(waitKeepAliveQuit, IsTrue)

	cancel()
	select {
	case <-closed:
	case <-time.After(timeout):
		c.Fatal("fail to quit WatchWorkerEvent before timeout")
	}
}
