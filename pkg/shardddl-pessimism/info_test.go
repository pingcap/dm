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

package pessimism

import (
	"context"
	"sync"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/integration"
)

var (
	etcdTestCli *clientv3.Client
)

func TestInfo(t *testing.T) {
	mockCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer mockCluster.Terminate(t)

	etcdTestCli = mockCluster.RandClient()

	TestingT(t)
}

type testInfo struct{}

var _ = Suite(&testInfo{})

func (t *testInfo) TestJSON(c *C) {
	i1 := NewInfo("test", "mysql-replica-1", "foo", "bar", []string{
		"ALTER TABLE bar ADD COLUMN c1 INT",
		"ALTER TABLE bar ADD COLUMN c2 INT",
	})

	j, err := i1.toJSON()
	c.Assert(err, IsNil)
	c.Assert(j, Equals, `{"task":"test","source":"mysql-replica-1","schema":"foo","table":"bar","ddls":["ALTER TABLE bar ADD COLUMN c1 INT","ALTER TABLE bar ADD COLUMN c2 INT"]}`)

	i2, err := infoFromJSON(j)
	c.Assert(err, IsNil)
	c.Assert(i2, DeepEquals, i1)
}

func (t *testInfo) TestEtcd(c *C) {
	var (
		source1 = "mysql-replica-1"
		source2 = "mysql-replica-2"
		task1   = "task-1"
		task2   = "task-2"
		i11     = NewInfo(task1, source1, "foo", "bar", []string{
			"ALTER TABLE bar ADD COLUMN c1 INT",
		})
		i12 = NewInfo(task1, source2, "foo", "bar", []string{
			"ALTER TABLE bar ADD COLUMN c2 INT",
		})
		i21 = NewInfo(task2, source1, "foo", "bar", []string{
			"ALTER TABLE bar ADD COLUMN c3 INT",
		})
	)

	// put the same key twice.
	rev1, err := PutInfo(etcdTestCli, i11)
	c.Assert(err, IsNil)
	rev2, err := PutInfo(etcdTestCli, i11)
	c.Assert(err, IsNil)
	c.Assert(rev2, Greater, rev1)

	// get with only 1 info.
	ifm, err := GetAllInfo(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(ifm, HasLen, 1)
	c.Assert(ifm, HasKey, task1)
	c.Assert(ifm[task1], HasLen, 1)
	c.Assert(ifm[task1][source1], DeepEquals, i11)

	// put another key and get again with 2 info.
	rev3, err := PutInfo(etcdTestCli, i12)
	c.Assert(err, IsNil)
	ifm, err = GetAllInfo(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(ifm, HasLen, 1)
	c.Assert(ifm, HasKey, task1)
	c.Assert(ifm[task1], HasLen, 2)
	c.Assert(ifm[task1][source1], DeepEquals, i11)
	c.Assert(ifm[task1][source2], DeepEquals, i12)

	// start the watcher.
	wch := make(chan Info, 10)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		WatchInfoPut(ctx, etcdTestCli, rev3+1, wch) // revision+1
		close(wch)                                  // close the chan
	}()

	// put another key for a different task.
	_, err = PutInfo(etcdTestCli, i21)
	c.Assert(err, IsNil)
	wg.Wait()

	// watch should only get i21.
	c.Assert(len(wch), Equals, 1)
	c.Assert(<-wch, DeepEquals, i21)

	// get again.
	ifm, err = GetAllInfo(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(ifm, HasLen, 2)
	c.Assert(ifm, HasKey, task1)
	c.Assert(ifm, HasKey, task2)
	c.Assert(ifm[task1], HasLen, 2)
	c.Assert(ifm[task1][source1], DeepEquals, i11)
	c.Assert(ifm[task1][source2], DeepEquals, i12)
	c.Assert(ifm[task2], HasLen, 1)
	c.Assert(ifm[task2][source1], DeepEquals, i21)
}
