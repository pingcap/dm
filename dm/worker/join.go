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
	"strings"
	"time"

	"github.com/pingcap/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/ha"
	"github.com/pingcap/dm/pkg/log"
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
		Address: s.cfg.AdvertiseAddr,
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

// KeepAlive attempts to keep the lease of the server alive forever.
func (s *Server) KeepAlive() {
	for {
		log.L().Info("start to keepalive with master")
		err1 := ha.KeepAlive(s.ctx, s.etcdClient, s.cfg.Name, s.cfg.KeepAliveTTL)
		log.L().Warn("keepalive with master goroutine paused", zap.Error(err1))
		s.stopWorker("")
		select {
		case <-s.ctx.Done():
			log.L().Info("keepalive with master goroutine exited!")
			return
		case <-time.After(retryConnectSleepTime):
			// Try to connect master again
		}
	}
}
