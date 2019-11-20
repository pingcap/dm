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

package election

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/pkg/tempurl"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/utils"
)

var _ = Suite(&testElectionSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testElectionSuite struct {
	etcd     *embed.Etcd
	endPoint string
}

func (t *testElectionSuite) SetUpTest(c *C) {
	log.InitLogger(&log.Config{})

	cfg := embed.NewConfig()
	cfg.Name = "election-test"
	cfg.Dir = c.MkDir()
	cfg.ZapLoggerBuilder = embed.NewZapCoreLoggerBuilder(log.L().Logger, log.L().Core(), log.Props().Syncer)
	cfg.Logger = "zap"
	err := cfg.Validate() // verify & trigger the builder
	c.Assert(err, IsNil)

	t.endPoint = tempurl.Alloc()
	url2, err := url.Parse(t.endPoint)
	c.Assert(err, IsNil)
	cfg.LCUrls = []url.URL{*url2}
	cfg.ACUrls = cfg.LCUrls

	url2, err = url.Parse(tempurl.Alloc())
	c.Assert(err, IsNil)
	cfg.LPUrls = []url.URL{*url2}
	cfg.APUrls = cfg.LPUrls

	cfg.InitialCluster = fmt.Sprintf("%s=%s", cfg.Name, url2)
	cfg.ClusterState = embed.ClusterStateFlagNew

	t.etcd, err = embed.StartEtcd(cfg)
	c.Assert(err, IsNil)
	select {
	case <-t.etcd.Server.ReadyNotify():
	case <-time.After(10 * time.Second):
		c.Fatal("start embed etcd timeout")
	}
}

func (t *testElectionSuite) TearDownTest(c *C) {
	t.etcd.Close()
}

func (t *testElectionSuite) TestElection(c *C) {
	var (
		sessionTTL = 60
		key        = "unit-test/election"
		ID1        = "member1"
		ID2        = "member2"
	)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{t.endPoint},
		DialTimeout: 3 * time.Second,
	})
	c.Assert(err, IsNil)
	defer cli.Close()

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	e1 := NewElection(ctx1, cli, sessionTTL, key, ID1)
	defer e1.Close()

	// e1 should become the leader
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return e1.IsLeader()
	}), IsTrue)
	leaderID, err := e1.LeaderID(ctx1)
	c.Assert(err, IsNil)
	c.Assert(leaderID, Equals, e1.ID())

	// start e2
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	e2 := NewElection(ctx2, cli, sessionTTL, key, ID2)
	defer e2.Close()
	// but the leader should still be e1
	leaderID, err = e2.LeaderID(ctx2)
	c.Assert(err, IsNil)
	c.Assert(leaderID, Equals, e1.ID())
	c.Assert(e2.IsLeader(), IsFalse)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-e1.RetireNotify(): // e1 should retire when closing
		case <-time.After(3 * time.Second):
			c.Fatal("leader campaign timeout")
		}
	}()
	e1.Close() // stop the campaign for e1
	c.Assert(e1.IsLeader(), IsFalse)
	wg.Wait()

	// e2 should become the leader
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return e2.IsLeader()
	}), IsTrue)
	leaderID, err = e2.LeaderID(ctx2)
	c.Assert(err, IsNil)
	c.Assert(leaderID, Equals, e2.ID())
}
