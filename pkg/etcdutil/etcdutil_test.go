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

package etcdutil

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/etcdserver/etcdserverpb"

	"github.com/pingcap/dm/pkg/log"
)

var _ = Suite(&testEtcdUtilSuite{})

type testEtcdUtilSuite struct {
}

func (t *testEtcdUtilSuite) SetUpSuite(c *C) {
	// initialized the logger to make genEmbedEtcdConfig working.
	log.InitLogger(&log.Config{})
}

func TestSuite(t *testing.T) {
	TestingT(t)
}

func (t *testEtcdUtilSuite) newConfig(c *C, name string, basePort uint16, portCount int) (*embed.Config, uint16) {
	cfg := embed.NewConfig()
	cfg.Name = name
	cfg.Dir = c.MkDir()
	cfg.ZapLoggerBuilder = embed.NewZapCoreLoggerBuilder(log.L().Logger, log.L().Core(), log.Props().Syncer)
	cfg.Logger = "zap"
	err := cfg.Validate() // verify & trigger the builder
	c.Assert(err, IsNil)

	cfg.LCUrls = []url.URL{}
	for i := 0; i < portCount; i++ {
		cu, err2 := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", basePort))
		c.Assert(err2, IsNil)
		cfg.LCUrls = append(cfg.LCUrls, *cu)
		basePort++
	}
	cfg.ACUrls = cfg.LCUrls

	cfg.LPUrls = []url.URL{}
	ic := make([]string, 0, portCount)
	for i := 0; i < portCount; i++ {
		pu, err2 := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", basePort))
		c.Assert(err2, IsNil)
		cfg.LPUrls = append(cfg.LPUrls, *pu)
		ic = append(ic, fmt.Sprintf("%s=%s", cfg.Name, pu))
		basePort++
	}
	cfg.APUrls = cfg.LPUrls

	cfg.InitialCluster = strings.Join(ic, ",")
	cfg.ClusterState = embed.ClusterStateFlagNew
	return cfg, basePort
}

func (t *testEtcdUtilSuite) urlsToStrings(URLs []url.URL) []string {
	ret := make([]string, 0, len(URLs))
	for _, u := range URLs {
		ret = append(ret, u.String())
	}
	return ret
}

func (t *testEtcdUtilSuite) startEtcd(c *C, cfg *embed.Config) *embed.Etcd {
	e, err := embed.StartEtcd(cfg)
	c.Assert(err, IsNil)

	timeout := 3 * time.Second
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(timeout):
		c.Fatalf("start embed etcd timeout %v", timeout)
	}

	return e
}

func (t *testEtcdUtilSuite) createEtcdClient(c *C, cfg *embed.Config) *clientv3.Client {
	cli, err := CreateClient(t.urlsToStrings(cfg.LCUrls))
	c.Assert(err, IsNil)
	return cli
}

func (t *testEtcdUtilSuite) checkMember(c *C, mid uint64, m *etcdserverpb.Member, cfg *embed.Config) {
	if m.Name != "" { // no name exists after `member add`
		c.Assert(m.Name, Equals, cfg.Name)
	}
	c.Assert(m.ID, Equals, mid)
	c.Assert(m.ClientURLs, DeepEquals, t.urlsToStrings(cfg.ACUrls))
	c.Assert(m.PeerURLs, DeepEquals, t.urlsToStrings(cfg.APUrls))
}

func (t *testEtcdUtilSuite) TestMemberUtil(c *C) {
	for i := 1; i <= 3; i++ {
		t.testMemberUtilInternal(c, i)
	}
}

func (t *testEtcdUtilSuite) testMemberUtilInternal(c *C, portCount int) {
	var basePort uint16 = 8361

	// start a etcd
	cfg1, basePort := t.newConfig(c, "etcd1", basePort, portCount)
	etcd1 := t.startEtcd(c, cfg1)
	defer etcd1.Close()

	// list member
	cli := t.createEtcdClient(c, cfg1)
	listResp1, err := ListMembers(cli)
	c.Assert(err, IsNil)
	c.Assert(listResp1.Members, HasLen, 1)
	t.checkMember(c, uint64(etcd1.Server.ID()), listResp1.Members[0], cfg1)

	// add member
	cfg2, basePort := t.newConfig(c, "etcd2", basePort, portCount)
	cfg2.InitialCluster = cfg1.InitialCluster + "," + cfg2.InitialCluster
	cfg2.ClusterState = embed.ClusterStateFlagExisting
	addResp, err := AddMember(cli, t.urlsToStrings(cfg2.APUrls))
	c.Assert(err, IsNil)
	c.Assert(addResp.Members, HasLen, 2)

	// start the added member
	etcd2 := t.startEtcd(c, cfg2)
	defer etcd2.Close()
	c.Assert(addResp.Member.ID, Equals, uint64(etcd2.Server.ID()))

	// list member again
	listResp2, err := ListMembers(cli)
	c.Assert(err, IsNil)
	c.Assert(listResp2.Members, HasLen, 2)
	for _, m := range listResp2.Members {
		switch m.ID {
		case uint64(etcd1.Server.ID()):
			t.checkMember(c, uint64(etcd1.Server.ID()), m, cfg1)
		case uint64(etcd2.Server.ID()):
			t.checkMember(c, uint64(etcd2.Server.ID()), m, cfg2)
		default:
			c.Fatalf("unknown member %v", m)
		}
	}
}
