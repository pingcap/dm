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
	"context"
	"go.uber.org/zap"
	"time"

	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/log"

	"google.golang.org/grpc"
)

const (
	oneselfLeader = "oneself"
)

func (s *Server) electionNotify(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case leader := <-s.election.LeaderNotify():
			// output the leader info.
			if leader {
				log.L().Info("current member become the leader", zap.String("current member", s.cfg.Name))
				err := s.coordinator.Start(ctx, s.etcdClient)
				if err != nil {
					log.L().Error("coordinator do not started", zap.Error(err))
				}
				if err = s.recoverSubTask(); err != nil {
					log.L().Error("recover subtask infos from coordinator fail", zap.Error(err))
				}

				s.Lock()
				s.leader = oneselfLeader
				s.Unlock()
			} else {
				leader, leaderID, err2 := s.election.LeaderInfo(ctx)
				if err2 == nil {
					log.L().Info("current member retire from the leader", zap.String("leader", leaderID), zap.String("current member", s.cfg.Name))
				} else {
					log.L().Warn("get leader info", zap.Error(err2))
				}
				s.coordinator.Stop()
				s.Lock()
				s.leader = leader
				s.createLeaderClient(leader)
				s.Unlock()
			}
		case err := <-s.election.ErrorNotify():
			// handle errors here, we do no meaningful things now.
			// but maybe:
			// 1. trigger an alert
			// 2. shutdown the DM-master process
			log.L().Error("receive error from election", zap.Error(err))
		}
	}
}

func (s *Server) createLeaderClient(leaderAddr string) {
	s.Lock()
	if s.leaderGrpcConn != nil {
		s.leaderGrpcConn.Close()
		s.leaderGrpcConn = nil
	}
	s.Unlock()

	conn, err := grpc.Dial(leaderAddr, grpc.WithInsecure(), grpc.WithBackoffMaxDelay(3*time.Second))
	if err != nil {
		log.L().Error("can't create grpc connection with leader, can't forward request to leader", zap.String("leader", leaderAddr))
		return
	}
	s.Lock()
	s.leaderGrpcConn = conn
	s.leaderClient = pb.NewMasterClient(conn)
	s.Unlock()
}

func (s *Server) isLeaderAndNeedForward() (isLeader bool, needForward bool) {
	s.RLock()
	defer s.RUnlock()
	isLeader = (s.leader == oneselfLeader)
	needForward = (s.leaderGrpcConn != nil)
	if !isLeader && !needForward {
		log.L().Error("this master is not leader, but can't forward request to leader")
	}
	return
}
