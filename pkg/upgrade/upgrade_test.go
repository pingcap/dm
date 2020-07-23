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
