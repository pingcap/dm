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
	"net/http"
	"time"

	"go.etcd.io/etcd/embed"
	"google.golang.org/grpc"

	"github.com/pingcap/dm/pkg/terror"
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
