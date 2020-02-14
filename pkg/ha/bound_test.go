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
	"time"

	. "github.com/pingcap/check"
)

func (t *testForEtcd) TestSourceBoundJSON(c *C) {
	b1 := NewSourceBound("mysql-replica-1", "dm-worker-1")

	j, err := b1.toJSON()
	c.Assert(err, IsNil)
	c.Assert(j, Equals, `{"source":"mysql-replica-1","worker":"dm-worker-1"}`)
	c.Assert(j, Equals, b1.String())

	b2, err := sourceBoundFromJSON(j)
	c.Assert(err, IsNil)
	c.Assert(b2, DeepEquals, b1)
}

func (t *testForEtcd) TestSourceBoundEtcd(c *C) {
	defer clearTestInfoOperation(c)

	var (
		watchTimeout = 500 * time.Millisecond
		worker       = "dm-worker-1"
		emptyBound   = SourceBound{}
		bound        = NewSourceBound("mysql-replica-1", worker)
	)
	c.Assert(bound.IsDeleted, IsFalse)

	// no bound exists.
	bo1, rev1, err := GetSourceBound(etcdTestCli, worker)
	c.Assert(err, IsNil)
	c.Assert(rev1, Equals, int64(0))
	c.Assert(bo1, DeepEquals, emptyBound)

	// put a bound.
	rev2, err := PutSourceBound(etcdTestCli, bound)
	c.Assert(err, IsNil)
	c.Assert(rev2, Greater, rev1)

	// watch the PUT operation for the bound.
	boundCh := make(chan SourceBound, 10)
	errCh := make(chan error, 10)
	ctx, cancel := context.WithTimeout(context.Background(), watchTimeout)
	WatchSourceBound(ctx, etcdTestCli, worker, rev2, boundCh, errCh)
	cancel()
	close(boundCh)
	close(errCh)
	c.Assert(len(boundCh), Equals, 1)
	bound.Revision = rev2
	c.Assert(<-boundCh, DeepEquals, bound)
	c.Assert(len(errCh), Equals, 0)

	// get the bound back.
	bo2, rev3, err := GetSourceBound(etcdTestCli, worker)
	c.Assert(err, IsNil)
	c.Assert(rev3, Equals, rev2)
	bound.Revision = 0
	c.Assert(bo2, DeepEquals, bound)

	// delete the bound.
	deleteOp := deleteSourceBoundOp(worker)
	resp, err := etcdTestCli.Txn(context.Background()).Then(deleteOp).Commit()
	c.Assert(err, IsNil)
	rev4 := resp.Header.Revision
	c.Assert(rev4, Greater, rev3)

	// watch the DELETE operation for the bound.
	boundCh = make(chan SourceBound, 10)
	errCh = make(chan error, 10)
	ctx, cancel = context.WithTimeout(context.Background(), watchTimeout)
	WatchSourceBound(ctx, etcdTestCli, worker, rev4, boundCh, errCh)
	cancel()
	close(boundCh)
	close(errCh)
	c.Assert(len(boundCh), Equals, 1)
	bo3 := <-boundCh
	c.Assert(bo3.IsDeleted, IsTrue)
	c.Assert(len(errCh), Equals, 0)

	// get again, not exists now.
	bo4, rev5, err := GetSourceBound(etcdTestCli, worker)
	c.Assert(err, IsNil)
	c.Assert(rev5, Equals, int64(0))
	c.Assert(bo4, DeepEquals, emptyBound)
}
