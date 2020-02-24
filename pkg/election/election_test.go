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
	"go.etcd.io/etcd/embed"

	"github.com/pingcap/dm/pkg/etcdutil"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
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

func (t *testElectionSuite) TestElection2After1(c *C) {
	var (
		sessionTTL = 60
		key        = "unit-test/election-2-after-1"
		ID1        = "member1"
		ID2        = "member2"
		ID3        = "member3"
		addr1      = "127.0.0.1:1"
		addr2      = "127.0.0.1:2"
		addr3      = "127.0.0.1:3"
	)
	cli, err := etcdutil.CreateClient([]string{t.endPoint})
	c.Assert(err, IsNil)
	defer cli.Close()

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	e1, err := NewElection(ctx1, cli, sessionTTL, key, ID1, addr1)
	c.Assert(err, IsNil)
	defer e1.Close()

	// e1 should become the leader
	select {
	case leader := <-e1.LeaderNotify():
		c.Assert(leader, Equals, IsLeader)
	case <-time.After(3 * time.Second):
		c.Fatal("leader campaign timeout")
	}
	c.Assert(e1.IsLeader(), IsTrue)
	_, leaderID, leaderAddr, err := e1.LeaderInfo(ctx1)
	c.Assert(err, IsNil)
	c.Assert(leaderID, Equals, e1.ID())
	c.Assert(leaderAddr, Equals, addr1)

	// start e2
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	e2, err := NewElection(ctx2, cli, sessionTTL, key, ID2, addr2)
	c.Assert(err, IsNil)
	defer e2.Close()
	select {
	case leader := <-e2.leaderCh:
		c.Assert(leader, Equals, IsNotLeader)
	case <-time.After(time.Second):
		c.Fatal("leader campaign timeout")
	}
	// but the leader should still be e1
	_, leaderID, leaderAddr, err = e2.LeaderInfo(ctx2)
	c.Assert(err, IsNil)
	c.Assert(leaderID, Equals, e1.ID())
	c.Assert(leaderAddr, Equals, addr1)
	c.Assert(e2.IsLeader(), IsFalse)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case leader := <-e1.LeaderNotify(): // e1 should retire when closing
			c.Assert(leader, Equals, RetireFromLeader)
		case <-time.After(time.Second):
			c.Fatal("leader campaign timeout")
		}
	}()
	e1.Close() // stop the campaign for e1
	c.Assert(e1.IsLeader(), IsFalse)
	wg.Wait()

	// e2 should become the leader
	select {
	case leader := <-e2.LeaderNotify():
		c.Assert(leader, Equals, IsLeader)
	case <-time.After(3 * time.Second):
		c.Fatal("leader campaign timeout")
	}
	c.Assert(e2.IsLeader(), IsTrue)
	_, leaderID, leaderAddr, err = e2.LeaderInfo(ctx2)
	c.Assert(err, IsNil)
	c.Assert(leaderID, Equals, e2.ID())
	c.Assert(leaderAddr, Equals, addr2)

	// if closing the client when campaigning, we should get an error
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case err2 := <-e2.ErrorNotify():
			c.Assert(terror.ErrElectionCampaignFail.Equal(err2), IsTrue)
			// the old session is done, but we can't create a new one.
			c.Assert(err2, ErrorMatches, ".*fail to campaign leader: create a new session.*")
		case <-time.After(time.Second):
			c.Fatal("do not receive error for e2")
		}
	}()
	cli.Close() // close the client
	wg.Wait()

	// can not elect with closed client.
	ctx3, cancel3 := context.WithCancel(context.Background())
	defer cancel3()
	_, err = NewElection(ctx3, cli, sessionTTL, key, ID3, addr3)
	c.Assert(terror.ErrElectionCampaignFail.Equal(err), IsTrue)
	c.Assert(err, ErrorMatches, ".*fail to campaign leader: create the initial session: context canceled.*")
}

func (t *testElectionSuite) TestElectionAlways1(c *C) {
	var (
		sessionTTL = 60
		key        = "unit-test/election-always-1"
		ID1        = "member1"
		ID2        = "member2"
		addr1      = "127.0.0.1:1234"
		addr2      = "127.0.0.1:2345"
	)
	cli, err := etcdutil.CreateClient([]string{t.endPoint})
	c.Assert(err, IsNil)
	defer cli.Close()

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	e1, err := NewElection(ctx1, cli, sessionTTL, key, ID1, addr1)
	c.Assert(err, IsNil)
	defer e1.Close()

	// e1 should become the leader
	select {
	case leader := <-e1.LeaderNotify():
		c.Assert(leader, Equals, IsLeader)
	case <-time.After(3 * time.Second):
		c.Fatal("leader campaign timeout")
	}
	c.Assert(e1.IsLeader(), IsTrue)
	_, leaderID, leaderAddr, err := e1.LeaderInfo(ctx1)
	c.Assert(err, IsNil)
	c.Assert(leaderID, Equals, e1.ID())
	c.Assert(leaderAddr, Equals, addr1)

	// start e2
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	e2, err := NewElection(ctx2, cli, sessionTTL, key, ID2, addr2)
	c.Assert(err, IsNil)
	defer e2.Close()
	time.Sleep(100 * time.Millisecond) // wait 100ms to start the campaign
	// but the leader should still be e1
	_, leaderID, leaderAddr, err = e2.LeaderInfo(ctx2)
	c.Assert(err, IsNil)
	c.Assert(leaderID, Equals, e1.ID())
	c.Assert(leaderAddr, Equals, addr1)
	c.Assert(e2.IsLeader(), IsFalse)

	// cancel the campaign for e2, should get no errors
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case err2 := <-e2.ErrorNotify():
			c.Fatalf("cancel the campaign should not get an error, %v", err2)
		case <-time.After(time.Second): // wait 1s
		}
	}()
	cancel2()
	wg.Wait()

	// e1 is still the leader
	c.Assert(e1.IsLeader(), IsTrue)
	_, leaderID, leaderAddr, err = e1.LeaderInfo(ctx1)
	c.Assert(err, IsNil)
	c.Assert(leaderID, Equals, e1.ID())
	c.Assert(leaderAddr, Equals, addr1)
	c.Assert(e2.IsLeader(), IsFalse)
}

func (t *testElectionSuite) TestElectionDeleteKey(c *C) {
	var (
		sessionTTL = 60
		key        = "unit-test/election-delete-key"
		ID         = "member"
		addr       = "127.0.0.1:1234"
	)
	cli, err := etcdutil.CreateClient([]string{t.endPoint})
	c.Assert(err, IsNil)
	defer cli.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e, err := NewElection(ctx, cli, sessionTTL, key, ID, addr)
	c.Assert(err, IsNil)
	defer e.Close()

	// should become the leader
	select {
	case leader := <-e.LeaderNotify():
		c.Assert(leader, Equals, IsLeader)
	case <-time.After(3 * time.Second):
		c.Fatal("leader campaign timeout")
	}
	c.Assert(e.IsLeader(), IsTrue)
	leaderKey, leaderID, leaderAddr, err := e.LeaderInfo(ctx)
	c.Assert(err, IsNil)
	c.Assert(leaderID, Equals, e.ID())
	c.Assert(leaderAddr, Equals, addr)

	// the leader retired after deleted the key
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		select {
		case err2 := <-e.ErrorNotify():
			c.Fatalf("delete the leader key should not get an error, %v", err2)
		case leader := <-e.LeaderNotify():
			c.Assert(leader, Equals, RetireFromLeader)
		}
	}()
	_, err = cli.Delete(ctx, leaderKey)
	c.Assert(err, IsNil)
	wg.Wait()
}
