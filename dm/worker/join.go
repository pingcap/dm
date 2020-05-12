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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := &pb.RegisterWorkerRequest{
		Name:    s.cfg.Name,
		Address: s.cfg.AdvertiseAddr,
	}

	for _, endpoint := range endpoints {
		ctx1, cancel1 := context.WithTimeout(ctx, 3*time.Second)
		conn, err := grpc.DialContext(ctx1, endpoint, grpc.WithBlock(), grpc.WithInsecure(), grpc.WithBackoffMaxDelay(3*time.Second))
		cancel1()
		if err != nil {
			if conn != nil {
				conn.Close()
			}
			log.L().Error("fail to dial dm-master", zap.Error(err))
			continue
		}
		client := pb.NewMasterClient(conn)
		ctx1, cancel1 = context.WithTimeout(ctx, 3*time.Second)
		resp, err := client.RegisterWorker(ctx1, req)
		cancel1()
		conn.Close()
		if err != nil {
			log.L().Error("fail to register worker", zap.Error(err))
			continue
		}
		if !resp.GetResult() {
			log.L().Error("fail to register worker", zap.String("error", resp.Msg))
			continue
		}
		return nil
	}
	// TODO: use terror here
	return errors.Errorf("cannot connect with master endpoints: %v", endpoints)
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
