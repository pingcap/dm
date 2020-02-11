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
	"testing"

	. "github.com/pingcap/check"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/integration"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/dm/config"
)

const (
	// do not forget to update this path if the file removed/renamed.
	sourceSampleFile = "../../dm/worker/dm-mysql.toml"
)

var (
	etcdTestCli *clientv3.Client
)

func TestHA(t *testing.T) {
	mockCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer mockCluster.Terminate(t)

	etcdTestCli = mockCluster.RandClient()

	TestingT(t)
}

// clear keys in etcd test cluster.
func clearTestInfoOperation(c *C) {
	clearSource := clientv3.OpDelete(common.UpstreamConfigKeyAdapter.Path(), clientv3.WithPrefix())
	clearSubTask := clientv3.OpDelete(common.UpstreamSubTaskKeyAdapter.Path(), clientv3.WithPrefix())
	clearWorkerInfo := clientv3.OpDelete(common.WorkerRegisterKeyAdapter.Path(), clientv3.WithPrefix())
	clearWorkerKeepAlive := clientv3.OpDelete(common.WorkerKeepAliveKeyAdapter.Path(), clientv3.WithPrefix())
	clearBound := clientv3.OpDelete(common.UpstreamBoundWorkerKeyAdapter.Path(), clientv3.WithPrefix())
	_, err := etcdTestCli.Txn(context.Background()).Then(clearSource, clearSubTask, clearWorkerInfo, clearWorkerKeepAlive, clearBound).Commit()
	c.Assert(err, IsNil)
}

type testForEtcd struct{}

var _ = Suite(&testForEtcd{})

func (t *testForEtcd) TestSourceEtcd(c *C) {
	defer clearTestInfoOperation(c)

	var (
		emptyCfg = config.MysqlConfig{}
		cfg      = config.MysqlConfig{}
	)
	c.Assert(cfg.LoadFromFile(sourceSampleFile), IsNil)
	source := cfg.SourceID

	// no source config exist.
	cfg1, rev1, err := GetSourceCfg(etcdTestCli, source)
	c.Assert(err, IsNil)
	c.Assert(rev1, Equals, int64(0))
	c.Assert(cfg1, DeepEquals, emptyCfg)

	// put a source config.
	rev2, err := PutSourceCfg(etcdTestCli, cfg)
	c.Assert(err, IsNil)
	c.Assert(rev2, Greater, rev1)

	// get the config back.
	cfg2, rev3, err := GetSourceCfg(etcdTestCli, source)
	c.Assert(err, IsNil)
	c.Assert(rev3, Equals, rev2)
	c.Assert(cfg2, DeepEquals, cfg)

	// delete the config.
	deleteOp := deleteSourceCfgOp(source)
	_, err = etcdTestCli.Txn(context.Background()).Then(deleteOp).Commit()
	c.Assert(err, IsNil)

	// get again, not exists now.
	cfg3, rev4, err := GetSourceCfg(etcdTestCli, source)
	c.Assert(err, IsNil)
	c.Assert(rev4, Equals, int64(0))
	c.Assert(cfg3, DeepEquals, emptyCfg)
}
