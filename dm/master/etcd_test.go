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
	"sort"
	"strings"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/pd/pkg/tempurl"
	"go.etcd.io/etcd/embed"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
)

var _ = check.Suite(&testEtcdSuite{})

type testEtcdSuite struct {
}

func (t *testEtcdSuite) SetUpSuite(c *check.C) {
	// initialized the logger to make genEmbedEtcdConfig working.
	log.InitLogger(&log.Config{})
}

func (t *testEtcdSuite) TestPrepareJoinEtcd(c *check.C) {
	cfgCluster := NewConfig() // used to start an etcd cluster
	cfgCluster.Name = "dm-master-1"
	cfgCluster.DataDir = c.MkDir()
	cfgCluster.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfgCluster.AdvertiseAddr = tempurl.Alloc()[len("http://"):]
	cfgCluster.PeerUrls = tempurl.Alloc()
	c.Assert(cfgCluster.adjust(), check.IsNil)
	cfgClusterEtcd, err := cfgCluster.genEmbedEtcdConfig()
	c.Assert(err, check.IsNil)

	cfgBefore := t.cloneConfig(cfgCluster) // before `prepareJoinEtcd` applied
	cfgBefore.DataDir = c.MkDir()          // overwrite some config items
	cfgBefore.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfgBefore.PeerUrls = tempurl.Alloc()
	cfgBefore.AdvertisePeerUrls = cfgBefore.PeerUrls
	c.Assert(cfgBefore.adjust(), check.IsNil)

	cfgAfter := t.cloneConfig(cfgBefore) // after `prepareJoinEtcd applied

	joinCluster := cfgCluster.MasterAddr
	joinFP := filepath.Join(cfgBefore.DataDir, "join")
	memberDP := filepath.Join(cfgBefore.DataDir, "member")

	// not set `join`, do nothing
	c.Assert(prepareJoinEtcd(cfgAfter), check.IsNil)
	c.Assert(cfgAfter, check.DeepEquals, cfgBefore)

	// try to join self
	cfgAfter.Join = cfgAfter.MasterAddr
	err = prepareJoinEtcd(cfgAfter)
	c.Assert(terror.ErrMasterJoinEmbedEtcdFail.Equal(err), check.IsTrue)
	c.Assert(err, check.ErrorMatches, ".*fail to join embed etcd: join self.*is forbidden.*")

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
	c.Assert(err, check.ErrorMatches, ".*fail to join embed etcd: read persistent join data.*")
	c.Assert(os.Remove(joinFP), check.IsNil)        // remove the persistent data
	c.Assert(cfgAfter, check.DeepEquals, cfgBefore) // not changed

	// restart with previous data
	c.Assert(os.Mkdir(memberDP, privateDirMode), check.IsNil)
	c.Assert(os.Mkdir(filepath.Join(memberDP, "wal"), privateDirMode), check.IsNil)
	c.Assert(prepareJoinEtcd(cfgAfter), check.IsNil)
	c.Assert(cfgAfter.InitialCluster, check.Equals, "")
	c.Assert(cfgAfter.InitialClusterState, check.Equals, embed.ClusterStateFlagExisting)
	c.Assert(os.RemoveAll(memberDP), check.IsNil) // remove previous data

	// start an etcd cluster
	e1, err := startEtcd(cfgClusterEtcd, nil, nil)
	c.Assert(err, check.IsNil)
	defer e1.Close()

	// same `name`, duplicate
	cfgAfter = t.cloneConfig(cfgBefore)
	err = prepareJoinEtcd(cfgAfter)
	c.Assert(terror.ErrMasterJoinEmbedEtcdFail.Equal(err), check.IsTrue)
	c.Assert(err, check.ErrorMatches, ".*fail to join embed etcd: missing data or joining a duplicate member.*")
	c.Assert(cfgAfter, check.DeepEquals, cfgBefore) // not changed

	// set a different name
	cfgBefore.Name = "dm-master-2"

	// add member with invalid `advertise-peer-urls`
	cfgAfter = t.cloneConfig(cfgBefore)
	cfgAfter.AdvertisePeerUrls = "invalid-advertise-peer-urls"
	err = prepareJoinEtcd(cfgAfter)
	c.Assert(terror.ErrMasterJoinEmbedEtcdFail.Equal(err), check.IsTrue)
	c.Assert(err, check.ErrorMatches, ".*fail to join embed etcd: add member.*")

	// join with existing cluster
	cfgAfter = t.cloneConfig(cfgBefore)
	c.Assert(prepareJoinEtcd(cfgAfter), check.IsNil)
	c.Assert(cfgAfter.InitialClusterState, check.Equals, embed.ClusterStateFlagExisting)
	obtainClusters := strings.Split(cfgAfter.InitialCluster, ",")
	sort.Strings(obtainClusters)
	expectedClusters := []string{
		cfgCluster.InitialCluster,
		fmt.Sprintf("%s=%s", cfgAfter.Name, cfgAfter.PeerUrls),
	}
	sort.Strings(expectedClusters)
	c.Assert(obtainClusters, check.DeepEquals, expectedClusters)

	// join data should exist now
	joinData, err := ioutil.ReadFile(joinFP)
	c.Assert(err, check.IsNil)
	c.Assert(string(joinData), check.Equals, cfgAfter.InitialCluster)

	// prepare join done, but has not start the etcd to complete the join, can not join anymore.
	cfgAfter2 := t.cloneConfig(cfgBefore)
	cfgAfter2.Name = "dm-master-3" // overwrite some items
	cfgAfter2.DataDir = c.MkDir()
	cfgAfter2.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfgAfter2.PeerUrls = tempurl.Alloc()
	cfgAfter2.AdvertisePeerUrls = cfgAfter2.PeerUrls
	err = prepareJoinEtcd(cfgAfter2)
	c.Assert(terror.ErrMasterJoinEmbedEtcdFail.Equal(err), check.IsTrue)
	c.Assert(err, check.ErrorMatches, ".*fail to join embed etcd: there is a member that has not joined successfully, continue the join or remove it.*")

	// start the joining etcd
	cfgAfterEtcd, err := cfgAfter.genEmbedEtcdConfig()
	c.Assert(err, check.IsNil)
	e2, err := startEtcd(cfgAfterEtcd, nil, nil)
	c.Assert(err, check.IsNil)
	defer e2.Close()

	// try join again
	for i := 0; i < 20; i++ {
		err = prepareJoinEtcd(cfgAfter2)
		if err == nil {
			break
		}
		// for `etcdserver: unhealthy cluster`, try again later
		c.Assert(terror.ErrMasterJoinEmbedEtcdFail.Equal(err), check.IsTrue)
		c.Assert(err, check.ErrorMatches, ".*fail to join embed etcd: add member.*: etcdserver: unhealthy cluster.*")
		time.Sleep(500 * time.Millisecond)
	}
	c.Assert(err, check.IsNil)
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
