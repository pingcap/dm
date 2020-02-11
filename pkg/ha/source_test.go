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

package ha

import (
	"context"
	"testing"

	. "github.com/pingcap/check"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/integration"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/dm/config"
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

// clear keys in etcd test cluster.
func clearTestInfoOperation(c *C) {
	clearSource := clientv3.OpDelete(common.UpstreamConfigKeyAdapter.Path(), clientv3.WithPrefix())
	_, err := etcdTestCli.Txn(context.Background()).Then(clearSource).Commit()
	c.Assert(err, IsNil)
}

type testForEtcd struct{}

var _ = Suite(&testForEtcd{})

func (t *testForEtcd) TestSourceEtcd(c *C) {
	defer clearTestInfoOperation(c)

	var (
		source   = "replica-mysql-1"
		emptyCfg = config.MysqlConfig{}
	)

	cfg, rev, err := GetSourceCfg(etcdTestCli, source)
	c.Assert(err, IsNil)
	c.Assert(rev, Equals, int64(0))
	c.Assert(cfg, DeepEquals, emptyCfg)
}
