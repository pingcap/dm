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

package worker

import (
	"context"
	"go.uber.org/zap"
	"strings"
	"time"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/errors"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
)

// GetJoinURLs gets the endpoints from the join address.
func GetJoinURLs(addrs string) []string {
	// TODO: handle pm1=xxxx:1234,pm2=xxxx:1234,pm3=xxxx:1234
	return strings.Split(addrs, ",")
}

// JoinMaster let dm-worker join the cluster with the specified master endpoints.
func (s *Server) JoinMaster(endpoints []string) error {
	// TODO: grpc proxy
	var client pb.MasterClient
	for _, endpoint := range endpoints {
		conn, err := grpc.Dial(endpoint, grpc.WithInsecure(), grpc.WithBackoffMaxDelay(3*time.Second))
		if err != nil {
			return err
		}
		client = pb.NewMasterClient(conn)
		break
	}
	if client == nil {
		return errors.Errorf("cannot connect with master endpoints: %v", endpoints)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req := &pb.RegisterWorkerRequest{
		Name:    s.cfg.Name,
		Address: s.cfg.WorkerAddr,
	}
	resp, err := client.RegisterWorker(ctx, req)
	if err != nil {
		return err
	}
	if !resp.GetResult() {
		return errors.Errorf("fail to register worker: %s", resp.GetMsg())
	}
	return nil
}

var (
	defaultKeepAliveTTL = int64(3)
	revokeLeaseTimeout  = time.Second
)

// KeepAlive attempts to keep the lease of the server alive forever.
func (s *Server) KeepAlive() (bool, error) {
	// TODO: fetch the actual master endpoints, the master member maybe changed.
	cliCtx, canc := context.WithTimeout(s.ctx, revokeLeaseTimeout)
	defer canc()
	lease, err := s.etcdClient.Grant(cliCtx, defaultKeepAliveTTL)
	if err != nil {
		return false, err
	}
	k := common.WorkerKeepAliveKeyAdapter.Encode(s.cfg.WorkerAddr, s.cfg.Name)
	_, err = s.etcdClient.Put(cliCtx, k, time.Now().String(), clientv3.WithLease(lease.ID))
	if err != nil {
		return false, err
	}
	ch, err := s.etcdClient.KeepAlive(s.ctx, lease.ID)
	if err != nil {
		return false, err
	}
	log.L().Info("keepalive", zap.String("to-master", s.cfg.Join))
	// set retryConnectMaster as long as it connects success, for next retry
	s.retryConnectMaster.Set(true)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				log.L().Info("keep alive channel is closed")
				return false, nil
			}
		case <-s.ctx.Done():
			log.L().Info("server is closing, exits keepalive")
			ctx, cancel := context.WithTimeout(s.etcdClient.Ctx(), revokeLeaseTimeout)
			defer cancel()
			s.etcdClient.Revoke(ctx, lease.ID)
			return true, nil
		}
	}

}
