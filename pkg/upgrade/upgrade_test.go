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

package upgrade

import (
	"context"
	"testing"

	. "github.com/pingcap/check"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/integration"

	"github.com/pingcap/dm/dm/common"
)

var (
	etcdTestCli *clientv3.Client
)

func TestUpgrade(t *testing.T) {
	mockCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer mockCluster.Terminate(t)

	etcdTestCli = mockCluster.RandClient()

	TestingT(t)
}

func clearTestData(c *C) {
	clearVersion := clientv3.OpDelete(common.ClusterVersionKey)
	_, err := etcdTestCli.Txn(context.Background()).Then(clearVersion).Commit()
	c.Assert(err, IsNil)
}

type testForEtcd struct{}

var _ = Suite(&testForEtcd{})

func (t *testForEtcd) TestTryUpgrade(c *C) {
	defer clearTestData(c)

	// mock upgrade functions.
	oldUpgrades := upgrades
	defer func() {
		upgrades = oldUpgrades
	}()
	mockVerNo := uint64(0)
	upgrades = []func(cli *clientv3.Client) error{
		func(cli *clientv3.Client) error {
			mockVerNo = currentInternalNo + 1
			return nil
		},
	}

	// no previous version exist, new cluster.
	ver, rev1, err := GetVersion(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev1, Greater, int64(0))
	c.Assert(ver.NotSet(), IsTrue)

	// try to upgrade, but do nothing except the current version recorded.
	c.Assert(TryUpgrade(etcdTestCli), IsNil)
	ver, rev2, err := GetVersion(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev2, Greater, rev1)
	c.Assert(ver, DeepEquals, CurrentVersion)
	c.Assert(mockVerNo, Equals, uint64(0))

	// try to upgrade again, do nothing because the version is the same.
	c.Assert(TryUpgrade(etcdTestCli), IsNil)
	ver, rev3, err := GetVersion(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev3, Equals, rev2)
	c.Assert(ver, DeepEquals, CurrentVersion)
	c.Assert(mockVerNo, Equals, uint64(0))

	// mock a newer current version.
	oldCurrentVer := CurrentVersion
	defer func() {
		CurrentVersion = oldCurrentVer
	}()
	newerVer := NewVersion(currentInternalNo+1, "mock-current-ver")
	CurrentVersion = newerVer

	// try to upgrade, to a newer version, upgrade operations applied.
	c.Assert(TryUpgrade(etcdTestCli), IsNil)
	ver, rev4, err := GetVersion(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev4, Greater, rev3)
	c.Assert(ver, DeepEquals, newerVer)
	c.Assert(mockVerNo, Equals, currentInternalNo+1)

	// try to upgrade, to an older version, do nothing.
	CurrentVersion = oldCurrentVer
	c.Assert(TryUpgrade(etcdTestCli), IsNil)
	ver, rev5, err := GetVersion(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev5, Equals, rev4)
	c.Assert(ver, DeepEquals, newerVer) // not changed.
	c.Assert(mockVerNo, Equals, currentInternalNo+1)
}
