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
	"sync"
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

func defaultMasterServer(c *check.C) *Server {
	cfg := NewConfig()
	err := cfg.Parse([]string{"-config=./dm-master.toml"})
	c.Assert(err, check.IsNil)
	server := NewServer(cfg)

	return server
}

func (t *testMaster) TestQueryStatus(c *check.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	server := defaultMasterServer(c)

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

	// query with invalid task name
	resp, err2 = server.QueryStatus(context.Background(), &pb.QueryStatusListRequest{
		Name: "invalid-task-name",
	})
	c.Assert(err2, check.IsNil)
	c.Assert(resp.Result, check.IsFalse)
	c.Assert(resp.Msg, check.Matches, "task .* has no workers or not exist, can try `refresh-worker-tasks` cmd first")

	// TODO: test query with correct task name, this needs to add task first
}

func (t *testMaster) TestShowDDLLocks(c *check.C) {
	server := defaultMasterServer(c)

	resp, err := server.ShowDDLLocks(context.Background(), &pb.ShowDDLLocksRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Locks, check.HasLen, 0)

	workers := make([]string, 0, len(server.cfg.DeployMap))
	for _, workerAddr := range server.cfg.DeployMap {
		workers = append(workers, workerAddr)
	}

	// prepare ddl lock keeper, mainly use code from ddl_lock_test.go
	sqls := []string{"stmt"}
	cases := []struct {
		task   string
		schema string
		table  string
	}{
		{"testA", "test_db1", "test_tbl1"},
		{"testB", "test_db2", "test_tbl2"},
	}
	lk := NewLockKeeper()
	var wg sync.WaitGroup
	for _, tc := range cases {
		wg.Add(1)
		go func(task, schema, table string) {
			defer wg.Done()
			id, synced, remain, err := lk.TrySync(task, schema, table, workers[0], sqls, workers)
			c.Assert(err, check.IsNil)
			c.Assert(synced, check.IsFalse)
			c.Assert(remain, check.Greater, 0) // multi-goroutines TrySync concurrently, can only confirm remain > 0
			c.Assert(lk.FindLock(id), check.NotNil)
		}(tc.task, tc.schema, tc.table)
	}
	wg.Wait()
	server.lockKeeper = lk

	// test query with task name
	resp, err = server.ShowDDLLocks(context.Background(), &pb.ShowDDLLocksRequest{
		Task:    "testA",
		Workers: workers,
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Locks, check.HasLen, 1)

	// test specify a mismatch worker
	resp, err = server.ShowDDLLocks(context.Background(), &pb.ShowDDLLocksRequest{
		Workers: []string{"invalid-worker"},
	})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Locks, check.HasLen, 0)

	// test query all ddl locks
	resp, err = server.ShowDDLLocks(context.Background(), &pb.ShowDDLLocksRequest{})
	c.Assert(err, check.IsNil)
	c.Assert(resp.Result, check.IsTrue)
	c.Assert(resp.Locks, check.HasLen, 2)
}
