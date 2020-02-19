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

	"github.com/pingcap/dm/dm/config"
)

const (
	// do not forget to update this path if the file removed/renamed.
	sourceSampleFile = "../../dm/worker/source.toml"
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
	c.Assert(ClearTestInfoOperation(etcdTestCli), IsNil)
}

type testForEtcd struct{}

var _ = Suite(&testForEtcd{})

func (t *testForEtcd) TestSourceEtcd(c *C) {
	defer clearTestInfoOperation(c)

	var (
		emptyCfg config.SourceConfig
		cfg      config.SourceConfig
	)
	c.Assert(cfg.LoadFromFile(sourceSampleFile), IsNil)
	source := cfg.SourceID
	cfgExtra := cfg
	cfgExtra.SourceID = "mysql-replica-2"

	// no source config exist.
	cfg1, rev1, err := GetSourceCfg(etcdTestCli, source, 0)
	c.Assert(err, IsNil)
	c.Assert(rev1, Equals, int64(0))
	c.Assert(cfg1, DeepEquals, emptyCfg)
	cfgM, _, err := GetAllSourceCfg(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(cfgM, HasLen, 0)

	// put a source config.
	rev2, err := PutSourceCfg(etcdTestCli, cfg)
	c.Assert(err, IsNil)
	c.Assert(rev2, Greater, rev1)

	// get the config back.
	cfg2, rev3, err := GetSourceCfg(etcdTestCli, source, 0)
	c.Assert(err, IsNil)
	c.Assert(rev3, Equals, rev2)
	c.Assert(cfg2, DeepEquals, cfg)

	// put another source config.
	rev2, err = PutSourceCfg(etcdTestCli, cfgExtra)
	c.Assert(err, IsNil)

	// get all two config.
	cfgM, rev3, err = GetAllSourceCfg(etcdTestCli)
	c.Assert(rev3, Equals, rev2)
	c.Assert(cfgM, HasLen, 2)
	c.Assert(cfgM[source], DeepEquals, cfg)
	c.Assert(cfgM[cfgExtra.SourceID], DeepEquals, cfgExtra)

	// delete the config.
	deleteOp := deleteSourceCfgOp(source)
	_, err = etcdTestCli.Txn(context.Background()).Then(deleteOp).Commit()
	c.Assert(err, IsNil)

	// get again, not exists now.
	cfg3, rev4, err := GetSourceCfg(etcdTestCli, source, 0)
	c.Assert(err, IsNil)
	c.Assert(rev4, Equals, int64(0))
	c.Assert(cfg3, DeepEquals, emptyCfg)
}
