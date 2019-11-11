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

package master

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pingcap/check"
	"go.etcd.io/etcd/embed"

	"github.com/pingcap/dm/pkg/terror"
)

var _ = check.Suite(&testEtcdSuite{})

type testEtcdSuite struct {
}

func (t *testEtcdSuite) TestPrepareJoinEtcd(c *check.C) {
	joinCluster := "dm-master-1=http://172.100.100.100:8269"

	cfgBefore := NewConfig() // before `prepareJoinEtcd` applied
	cfgBefore.MasterAddr = ":8261"
	cfgBefore.DataDir = c.MkDir()
	c.Assert(cfgBefore.adjust(), check.IsNil)
	cfgAfter := t.cloneConfig(cfgBefore) // after `prepareJoinEtcd applied
	joinFP := filepath.Join(cfgBefore.DataDir, "join")
	memberDP := filepath.Join(cfgBefore.DataDir, "member")

	// not set `join`, do nothing
	c.Assert(prepareJoinEtcd(cfgAfter), check.IsNil)
	c.Assert(cfgAfter, check.DeepEquals, cfgBefore)

	// try to join self
	cfgAfter.Join = cfgAfter.AdvertisePeerUrls
	err := prepareJoinEtcd(cfgAfter)
	c.Assert(terror.ErrMasterJoinEmbedEtcdFail.Equal(err), check.IsTrue)
	c.Assert(err, check.ErrorMatches, ".*join self.*is forbidden.*")

	// update `join` to a valid item
	cfgBefore.Join = joinCluster

	// join with persistent data
	c.Assert(ioutil.WriteFile(joinFP, []byte(joinCluster), privateDirMode), check.IsNil)
	cfgAfter = t.cloneConfig(cfgBefore)
	c.Assert(prepareJoinEtcd(cfgAfter), check.IsNil)
	c.Assert(cfgAfter.InitialCluster, check.Equals, joinCluster)
	c.Assert(cfgAfter.InitialClusterState, check.Equals, embed.ClusterStateFlagExisting)
	c.Assert(os.Remove(joinFP), check.IsNil) // remove the persistent data

	// join with invalid persistent data
	c.Assert(os.Mkdir(joinFP, privateDirMode), check.IsNil) // use directory as invalid persistent data (file)
	cfgAfter = t.cloneConfig(cfgBefore)
	err = prepareJoinEtcd(cfgAfter)
	c.Assert(terror.ErrMasterJoinEmbedEtcdFail.Equal(err), check.IsTrue)
	c.Assert(err, check.ErrorMatches, ".*read persistent join data.*")
	c.Assert(os.Remove(joinFP), check.IsNil) // remove the persistent data

	// restart with previous data
	c.Assert(os.Mkdir(memberDP, privateDirMode), check.IsNil)
	c.Assert(os.Mkdir(filepath.Join(memberDP, "wal"), privateDirMode), check.IsNil)
	c.Assert(prepareJoinEtcd(cfgAfter), check.IsNil)
	c.Assert(cfgAfter.InitialCluster, check.Equals, "")
	c.Assert(cfgAfter.InitialClusterState, check.Equals, embed.ClusterStateFlagExisting)
	c.Assert(os.RemoveAll(memberDP), check.IsNil) // remove previous data
}

func (t *testEtcdSuite) cloneConfig(cfg *Config) *Config {
	clone := NewConfig()
	*clone = *cfg
	return clone
}

func (t *testEtcdSuite) TestIsDataExist(c *check.C) {
	d := "./directory-not-exists"
	c.Assert(isDataExist(d), check.IsFalse)

	// empty directory
	d = c.MkDir()
	c.Assert(isDataExist(d), check.IsFalse)

	// data exists in the directory
	for i := 1; i <= 3; i++ {
		fp := filepath.Join(d, fmt.Sprintf("file.%d", i))
		c.Assert(ioutil.WriteFile(fp, nil, privateDirMode), check.IsNil)
		c.Assert(isDataExist(d), check.IsTrue)
		c.Assert(isDataExist(fp), check.IsFalse) // not a directory
	}
}
