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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/check"

	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/pbmock"
)

func TestMaster(t *testing.T) {
	check.TestingT(t)
}

type testMaster struct {
}

var _ = check.Suite(&testMaster{})

func (t *testMaster) TestQueryStatus(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	cfg := NewConfig()
	err := cfg.Parse([]string{"-config=./dm-master.toml"})
	c.Assert(err, check.IsNil)
	server := NewServer(cfg)

	// test query all workers
	for _, workerAddr := range server.cfg.DeployMap {
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		mockWorkerClient.EXPECT().QueryStatus(
			gomock.Any(),
			&pb.QueryStatusRequest{},
		).Return(&pb.QueryStatusResponse{Result: true}, nil)
		server.workerClients[workerAddr] = mockWorkerClient
	}
	resp, err2 := server.QueryStatus(context.Background(), &pb.QueryStatusListRequest{})
	c.Assert(err2, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)

	// query specified dm-worker[s]
	workers := make([]string, 0, len(server.cfg.DeployMap))
	for _, workerAddr := range server.cfg.DeployMap {
		workers = append(workers, workerAddr)
		mockWorkerClient := pbmock.NewMockWorkerClient(ctrl)
		mockWorkerClient.EXPECT().QueryStatus(
			gomock.Any(),
			&pb.QueryStatusRequest{},
		).Return(&pb.QueryStatusResponse{Result: true}, nil)
		server.workerClients[workerAddr] = mockWorkerClient
	}
	resp, err2 = server.QueryStatus(context.Background(), &pb.QueryStatusListRequest{
		Workers: workers,
	})
	c.Assert(err2, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)

	// query with invalid dm-worker[s]
	resp, err2 = server.QueryStatus(context.Background(), &pb.QueryStatusListRequest{
		Workers: []string{"invalid-worker1", "invalid-worker2"},
	})
	c.Assert(err2, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)
	c.Assert(resp.Msg, check.Matches, ".*relevant worker-client not found")

	// TODO: test query with task name, this needs to add task first
}
