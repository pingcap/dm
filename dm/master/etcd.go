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
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"google.golang.org/grpc"

	"github.com/pingcap/dm/pkg/etcdutil"
	"github.com/pingcap/dm/pkg/terror"
)

var (
	// privateDirMode grants owner to make/remove files inside the directory.
	privateDirMode os.FileMode = 0700
)

// startEtcd starts an embedded etcd server.
func startEtcd(masterCfg *Config,
	gRPCSvr func(*grpc.Server),
	httpHandles map[string]http.Handler) (*embed.Etcd, error) {
	cfg, err := masterCfg.genEmbedEtcdConfig()
	if err != nil {
		return nil, err
	}

	// attach extra gRPC and HTTP server
	if gRPCSvr != nil {
		cfg.ServiceRegister = gRPCSvr
	}
	if httpHandles != nil {
		cfg.UserHandlers = httpHandles
	}

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, terror.ErrMasterStartEmbedEtcdFail.Delegate(err)
	}

	timeout := time.Minute
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(timeout):
		e.Server.Stop()
		e.Close()
		return nil, terror.ErrMasterStartEmbedEtcdFail.Generatef("start embed etcd timeout %v", timeout)
	}
	return e, nil
}

// prepareJoinEtcd prepares config needed to join an existing cluster.
// learn from https://github.com/pingcap/pd/blob/37efcb05f397f26c70cda8dd44acaa3061c92159/server/join/join.go#L44.
func prepareJoinEtcd(cfg *Config) error {
	// no need to join
	if cfg.Join == "" {
		return nil
	}

	// try to join self, invalid
	if cfg.Join == cfg.AdvertisePeerUrls {
		return terror.ErrMasterJoinEmbedEtcdFail.Generatef("join self %s is forbidden", cfg.Join)
	}

	// join with persistent data
	joinFP := filepath.Join(cfg.DataDir, "join")
	if _, err := os.Stat(joinFP); !os.IsNotExist(err) {
		s, err := ioutil.ReadFile(joinFP)
		if err != nil {
			return terror.ErrMasterJoinEmbedEtcdFail.AnnotateDelegate(err, "read persistent join data")
		}
		cfg.InitialCluster = strings.TrimSpace(string(s))
		cfg.InitialClusterState = embed.ClusterStateFlagExisting
		return nil
	}

	// restart with previous data, no `InitialCluster` need to set
	if isDataExist(filepath.Join(cfg.DataDir, "member")) {
		cfg.InitialCluster = ""
		cfg.InitialClusterState = embed.ClusterStateFlagExisting
		return nil
	}

	// if without previous data, we need a client to contact with the existing cluster.
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(cfg.Join, ","),
		DialTimeout: etcdutil.DefaultDialTimeout,
	})
	if err != nil {
		return terror.ErrMasterJoinEmbedEtcdFail.Delegate(err)
	}
	defer client.Close()

	// `member list`
	listResp, err := etcdutil.ListMembers(client)
	if err != nil {
		return terror.ErrMasterJoinEmbedEtcdFail.Delegate(err)
	}

	// check members
	for _, m := range listResp.Members {
		if m.Name == "" {
			return terror.ErrMasterJoinEmbedEtcdFail.New("there is a member that has not joined successfully")
		}
		if m.Name == cfg.Name {
			// a failed DM-master re-joins the previous cluster.
			return terror.ErrMasterJoinEmbedEtcdFail.Generatef("missing data or joining a duplicated dm-master %s", m.Name)
		}
	}

	// `member add`, a new/deleted DM-master joins to an existing cluster.
	addResp, err := etcdutil.AddMember(client, strings.Split(cfg.AdvertisePeerUrls, ","))
	if err != nil {
		return terror.ErrMasterJoinEmbedEtcdFail.Delegate(err)
	}

	// generate `--initial-cluster`
	ms := make([]string, 0, len(addResp.Members))
	for _, m := range addResp.Members {
		name := m.Name
		if m.ID == addResp.Member.ID {
			name = cfg.Name
		}
		if name == "" {
			return terror.ErrMasterJoinEmbedEtcdFail.New("there is a member that has not joined successfully")
		}
		for _, url := range m.PeerURLs {
			ms = append(ms, fmt.Sprintf("%s=%s", name, url))
		}
	}
	cfg.InitialCluster = strings.Join(ms, ",")
	cfg.InitialClusterState = embed.ClusterStateFlagExisting

	// save `--initial-cluster` in persist data
	if err = os.MkdirAll(cfg.DataDir, privateDirMode); !os.IsExist(err) {
		return terror.ErrMasterJoinEmbedEtcdFail.Delegate(err)
	}
	if err = ioutil.WriteFile(joinFP, []byte(cfg.InitialCluster), privateDirMode); err != nil {
		return terror.ErrMasterJoinEmbedEtcdFail.Delegate(err)
	}

	return nil
}

// isDataExist returns whether the directory is empty (with data)
func isDataExist(d string) bool {
	dir, err := os.Open(d)
	if err != nil {
		return false
	}
	defer dir.Close()

	names, err := dir.Readdirnames(1) // read only one is enough
	if err != nil {
		return false
	}
	return len(names) != 0
}
