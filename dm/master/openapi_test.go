// Copyright 2021 PingCAP, Inc.
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

// this file implement all of the APIs of the DataMigration service.

package master

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/deepmap/oapi-codegen/pkg/testutil"
	"github.com/pingcap/check"
	"github.com/tikv/pd/pkg/tempurl"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/integration"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/master/workerrpc"
	"github.com/pingcap/dm/openapi"
	"github.com/pingcap/dm/pkg/ha"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

var openAPITestSuite = check.Suite(&openAPISuite{})

// some data for test.
var (
	source1Name = "mysql-replica-01"
)

type openAPISuite struct {
	testT *testing.T

	etcdTestCli     *clientv3.Client
	testEtcdCluster *integration.ClusterV3
	workerClients   map[string]workerrpc.Client
}

func (t *openAPISuite) SetUpTest(c *check.C) {
	t.testEtcdCluster = integration.NewClusterV3(t.testT, &integration.ClusterConfig{Size: 1})
	t.etcdTestCli = t.testEtcdCluster.RandClient()
	t.workerClients = make(map[string]workerrpc.Client)

	c.Assert(ha.ClearTestInfoOperation(t.etcdTestCli), check.IsNil)
}

func setupServer(ctx context.Context, c *check.C) *Server {
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
	// wait the first one become the leader
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return s1.election.IsLeader()
	}), check.IsTrue)

	return s1
}

func (t *openAPISuite) TestRedirectRequestToLeader(c *check.C) {
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

	baseURL := "/api/v1/sources"
	// list source from leader
	result := testutil.NewRequest().Get(baseURL).Go(t.testT, s1.echo)
	// check http status code
	c.Assert(result.Code(), check.Equals, http.StatusOK)
	var resultListSource openapi.GetSourceListResponse
	err := result.UnmarshalBodyToObject(&resultListSource)
	c.Assert(err, check.IsNil)
	c.Assert(resultListSource.Data, check.HasLen, 0)
	c.Assert(resultListSource.Total, check.Equals, 0)

	// list source not from leader will get a redirect
	result2 := testutil.NewRequest().Get(baseURL).Go(t.testT, s2.echo)
	c.Assert(result2.Code(), check.Equals, http.StatusTemporaryRedirect)
}

func (t *openAPISuite) TestSourceAPI(c *check.C) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s := setupServer(ctx, c)
	defer s.Close()

	baseURL := "/api/v1/sources"

	dbCFG := config.GetDBConfigFromEnv()
	source1 := openapi.Source{
		SourceName: source1Name,
		EnableGtid: false,
		Host:       dbCFG.Host,
		Password:   dbCFG.Password,
		Port:       dbCFG.Port,
		User:       dbCFG.User,
	}
	result := testutil.NewRequest().Post(baseURL).WithJsonBody(source1).Go(t.testT, s.echo)
	// check http status code
	c.Assert(result.Code(), check.Equals, http.StatusCreated)
	var resultSource openapi.Source
	err := result.UnmarshalBodyToObject(&resultSource)
	c.Assert(err, check.IsNil)
	c.Assert(resultSource.User, check.Equals, source1.User)
	c.Assert(resultSource.Host, check.Equals, source1.Host)
	c.Assert(resultSource.Port, check.Equals, source1.Port)
	c.Assert(resultSource.Password, check.Equals, source1.Password)
	c.Assert(resultSource.EnableGtid, check.Equals, source1.EnableGtid)
	c.Assert(resultSource.SourceName, check.Equals, source1.SourceName)

	// create source with same name will failed
	source2 := source1
	result2 := testutil.NewRequest().Post(baseURL).WithJsonBody(source2).Go(t.testT, s.echo)
	// check http status code
	c.Assert(result2.Code(), check.Equals, http.StatusBadRequest)
	var errResp openapi.ErrorWithMessage
	err = result2.UnmarshalBodyToObject(&errResp)
	c.Assert(err, check.IsNil)
	c.Assert(errResp.ErrorCode, check.Equals, int(terror.ErrSchedulerSourceCfgExist.Code()))

	// list source
	result3 := testutil.NewRequest().Get(baseURL).Go(t.testT, s.echo)
	// check http status code
	c.Assert(result3.Code(), check.Equals, http.StatusOK)
	var resultListSource openapi.GetSourceListResponse
	err = result3.UnmarshalBodyToObject(&resultListSource)
	c.Assert(err, check.IsNil)
	c.Assert(resultListSource.Data, check.HasLen, 1)
	c.Assert(resultListSource.Total, check.Equals, 1)
	c.Assert(resultListSource.Data[0].SourceName, check.Equals, source1.SourceName)

	// delete source
	result4 := testutil.NewRequest().Delete(fmt.Sprintf("%s/%s", baseURL, source1.SourceName)).Go(t.testT, s.echo)
	// check http status code
	c.Assert(result4.Code(), check.Equals, http.StatusNoContent)

	// delete again will failed
	result5 := testutil.NewRequest().Delete(fmt.Sprintf("%s/%s", baseURL, source1.SourceName)).Go(t.testT, s.echo)
	c.Assert(result5.Code(), check.Equals, http.StatusBadRequest)
	var errResp2 openapi.ErrorWithMessage
	err = result5.UnmarshalBodyToObject(&errResp2)
	c.Assert(err, check.IsNil)
	c.Assert(errResp2.ErrorCode, check.Equals, int(terror.ErrSchedulerSourceCfgNotExist.Code()))

	// list source
	result6 := testutil.NewRequest().Get(baseURL).Go(t.testT, s.echo)
	// check http status code
	c.Assert(result6.Code(), check.Equals, http.StatusOK)
	var resultListSource2 openapi.GetSourceListResponse
	err = result6.UnmarshalBodyToObject(&resultListSource2)
	c.Assert(err, check.IsNil)
	c.Assert(resultListSource2.Data, check.HasLen, 0)
	c.Assert(resultListSource2.Total, check.Equals, 0)
}
