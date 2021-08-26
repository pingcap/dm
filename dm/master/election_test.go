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

package master

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/tikv/pd/pkg/tempurl"

	"github.com/pingcap/dm/pkg/etcdutil"
	"github.com/pingcap/dm/pkg/utils"
)

var _ = check.Suite(&testElectionSuite{})

type testElectionSuite struct{}

func (t *testElectionSuite) TestFailToStartLeader(c *check.C) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// create a new cluster
	cfg1 := NewConfig()
	c.Assert(cfg1.Parse([]string{"-config=./dm-master.toml"}), check.IsNil)
	cfg1.Name = "dm-master-1"
	cfg1.DataDir = c.MkDir()
	cfg1.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfg1.PeerUrls = tempurl.Alloc()
	cfg1.AdvertisePeerUrls = cfg1.PeerUrls
	cfg1.InitialCluster = fmt.Sprintf("%s=%s", cfg1.Name, cfg1.AdvertisePeerUrls)

	s1 := NewServer(cfg1)
	c.Assert(s1.Start(ctx), check.IsNil)
	defer s1.Close()

	// wait the first one become the leader
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return s1.election.IsLeader()
	}), check.IsTrue)

	// join to an existing cluster
	cfg2 := NewConfig()
	c.Assert(cfg2.Parse([]string{"-config=./dm-master.toml"}), check.IsNil)
	cfg2.Name = "dm-master-2"
	cfg2.DataDir = c.MkDir()
	cfg2.MasterAddr = tempurl.Alloc()[len("http://"):]
	cfg2.PeerUrls = tempurl.Alloc()
	cfg2.AdvertisePeerUrls = cfg2.PeerUrls
	cfg2.Join = cfg1.MasterAddr // join to an existing cluster

	s2 := NewServer(cfg2)
	c.Assert(s2.Start(ctx), check.IsNil)
	defer s2.Close()

	client, err := etcdutil.CreateClient(strings.Split(cfg1.AdvertisePeerUrls, ","), nil)
	c.Assert(err, check.IsNil)
	defer client.Close()

	// s1 is still the leader
	_, leaderID, _, err := s2.election.LeaderInfo(ctx)
	c.Assert(err, check.IsNil)
	c.Assert(leaderID, check.Equals, cfg1.Name)

	// fail to start scheduler/pessimism/optimism
	c.Assert(failpoint.Enable("github.com/pingcap/dm/dm/master/FailToStartLeader", `return("dm-master-2")`), check.IsNil)
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/dm/dm/master/FailToStartLeader")

	s1.election.Resign()
	time.Sleep(1 * time.Second)

	// s1 is still the leader
	_, leaderID, _, err = s2.election.LeaderInfo(ctx)
	c.Assert(err, check.IsNil)
	c.Assert(leaderID, check.Equals, cfg1.Name)

	//nolint:errcheck
	failpoint.Disable("github.com/pingcap/dm/dm/master/FailToStartLeader")
	s1.election.Resign()
	time.Sleep(1 * time.Second)

	// s2 now become leader
	_, leaderID, _, err = s2.election.LeaderInfo(ctx)
	c.Assert(err, check.IsNil)
	c.Assert(leaderID, check.Equals, cfg2.Name)
}
