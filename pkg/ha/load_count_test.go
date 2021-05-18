// Copyright 2021 PingCAP, Inc.
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
	"time"

	. "github.com/pingcap/check"
)

func (t *testForEtcd) TestLoadCountEtcd(c *C) {
	var (
		worker1      = "worker1"
		source1      = "source1"
		source2      = "source2"
		watchTimeout = 2 * time.Second
	)
	defer clearTestInfoOperation(c)

	// no load count exist.
	count1, ver1, err := GetWorkerSourceLoadCount(etcdTestCli, worker1, source1)
	c.Assert(err, IsNil)
	c.Assert(count1, Equals, 0)
	c.Assert(ver1, Equals, int64(0))

	// increase count
	rev1, err := IncWorkerSourceLoadCount(etcdTestCli, worker1, source1)
	c.Assert(err, IsNil)

	count2, ver2, err := GetWorkerSourceLoadCount(etcdTestCli, worker1, source1)
	c.Assert(err, IsNil)
	c.Assert(count2, Equals, 1)
	c.Assert(ver2, Equals, int64(1))

	// increase count again
	rev2, err := IncWorkerSourceLoadCount(etcdTestCli, worker1, source1)
	c.Assert(err, IsNil)
	c.Assert(rev2, Greater, rev1)

	count3, ver3, err := GetWorkerSourceLoadCount(etcdTestCli, worker1, source1)
	c.Assert(err, IsNil)
	c.Assert(count3, Equals, 2)
	c.Assert(ver3, Equals, int64(2))

	// decrease count
	rev3, err := DecWorkerSourceLoadCount(etcdTestCli, worker1, source1)
	c.Assert(err, IsNil)
	c.Assert(rev3, Greater, rev2)

	count4, ver4, err := GetWorkerSourceLoadCount(etcdTestCli, worker1, source1)
	c.Assert(err, IsNil)
	c.Assert(count4, Equals, 1)
	c.Assert(ver4, Equals, int64(3))

	// decrease count again
	rev4, err := DecWorkerSourceLoadCount(etcdTestCli, worker1, source1)
	c.Assert(err, IsNil)
	c.Assert(rev4, Greater, rev3)

	count5, ver5, err := GetWorkerSourceLoadCount(etcdTestCli, worker1, source1)
	c.Assert(err, IsNil)
	c.Assert(count5, Equals, 0)
	c.Assert(ver5, Equals, int64(0))

	// increase count for multiple sources
	rev5, err := IncWorkerSourceLoadCount(etcdTestCli, worker1, source1)
	c.Assert(err, IsNil)
	c.Assert(rev5, Greater, rev4)
	rev6, err := IncWorkerSourceLoadCount(etcdTestCli, worker1, source1)
	c.Assert(err, IsNil)
	c.Assert(rev6, Greater, rev5)
	rev7, err := IncWorkerSourceLoadCount(etcdTestCli, worker1, source2)
	c.Assert(err, IsNil)
	c.Assert(rev7, Greater, rev6)

	count6, ver6, err := GetWorkerSourceLoadCount(etcdTestCli, worker1, source1)
	c.Assert(err, IsNil)
	c.Assert(count6, Equals, 2)
	c.Assert(ver6, Equals, int64(2))
	count7, ver7, err := GetWorkerSourceLoadCount(etcdTestCli, worker1, source2)
	c.Assert(err, IsNil)
	c.Assert(count7, Equals, 1)
	c.Assert(ver7, Equals, int64(1))

	wslcm, rev8, err := GetAllWorkerSourceLoadCount(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev8, Equals, rev7)
	c.Assert(wslcm, HasLen, 1)
	c.Assert(wslcm, HasKey, worker1)
	c.Assert(wslcm[worker1], HasLen, 2)
	c.Assert(wslcm[worker1][source1], Equals, 2)
	c.Assert(wslcm[worker1][source2], Equals, 1)

	// Delete count
	rev9, succ, err := DelWorkerSourceLoadCount(etcdTestCli, worker1, source1)
	c.Assert(err, IsNil)
	c.Assert(rev9, Greater, rev8)
	c.Assert(succ, IsTrue)

	count8, ver8, err := GetWorkerSourceLoadCount(etcdTestCli, worker1, source1)
	c.Assert(err, IsNil)
	c.Assert(count8, Equals, 0)
	c.Assert(ver8, Equals, int64(0))
	count9, ver9, err := GetWorkerSourceLoadCount(etcdTestCli, worker1, source2)
	c.Assert(err, IsNil)
	c.Assert(count9, Equals, 1)
	c.Assert(ver9, Equals, int64(1))

	// Delete count by worker
	rev10, succ, err := DelWorkerSourceLoadCountByWorker(etcdTestCli, worker1)
	c.Assert(err, IsNil)
	c.Assert(rev10, Greater, rev9)
	c.Assert(succ, IsTrue)

	count10, ver10, err := GetWorkerSourceLoadCount(etcdTestCli, worker1, source2)
	c.Assert(err, IsNil)
	c.Assert(count10, Equals, 0)
	c.Assert(ver10, Equals, int64(0))

	wslcm, rev11, err := GetAllWorkerSourceLoadCount(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev11, Equals, rev10)
	c.Assert(wslcm, HasLen, 0)

	rev12, err := IncWorkerSourceLoadCount(etcdTestCli, worker1, source2)
	c.Assert(err, IsNil)
	c.Assert(rev12, Greater, rev11)

	// watch operations for the load count.
	loadCountCh := make(chan WorkerSourceLoadCount, 10)
	errCh := make(chan error, 10)
	ctx, cancel := context.WithTimeout(context.Background(), watchTimeout)
	WatchLoadCount(ctx, etcdTestCli, rev8+1, loadCountCh, errCh)
	cancel()
	close(loadCountCh)
	close(errCh)
	c.Assert(len(loadCountCh), Equals, 3)
	delLoadCount1 := <-loadCountCh
	c.Assert(delLoadCount1.Worker, Equals, worker1)
	c.Assert(delLoadCount1.Source, Equals, source1)
	c.Assert(delLoadCount1.IsDelete, IsTrue)
	delLoadCount2 := <-loadCountCh
	c.Assert(delLoadCount2.Worker, Equals, worker1)
	c.Assert(delLoadCount2.Source, Equals, source2)
	c.Assert(delLoadCount2.IsDelete, IsTrue)
	putLoadCount3 := <-loadCountCh
	c.Assert(putLoadCount3.Worker, Equals, worker1)
	c.Assert(putLoadCount3.Source, Equals, source2)
	c.Assert(putLoadCount3.IsDelete, IsFalse)
	c.Assert(putLoadCount3.Count, Equals, 1)
}
