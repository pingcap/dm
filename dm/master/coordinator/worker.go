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

package coordinator

import (
	"context"
	"fmt"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"sync/atomic"
	"time"

	"github.com/pingcap/dm/dm/master/workerrpc"
)

// WorkerState the status of the worker
type WorkerState int

// the status of worker
const (
	WorkerClosed WorkerState = iota + 1
	WorkerFree
	WorkerBound
)

// Worker the proc essor that let upstream and downstream synchronization.
type Worker struct {
	name    string
	address string
	client  workerrpc.Client
	status  atomic.Value
}

// NewWorker creates a worker with specified name and address.
func NewWorker(name, address string) *Worker {
	w := &Worker{
		name:    name,
		address: address,
	}
	w.status.Store(WorkerClosed)
	return w
}

// String formats the worker.
func (w *Worker) String() string {
	return fmt.Sprintf("%s address:%s", w.name, w.address)
}

// GetClient returns the client of the worker.
func (w *Worker) GetClient() (workerrpc.Client, error) {
	if w.client == nil {
		client, err := workerrpc.NewGRPCClient(w.address)
		if err != nil {
			return nil, err
		}
		w.client = client
	}
	return w.client, nil
}

// Name returns the name of the worker.
func (w *Worker) Name() string {
	return w.name
}

// Address returns the address of the worker.
func (w *Worker) Address() string {
	return w.address
}

// State returns the state of the worker.
func (w *Worker) State() WorkerState {
	// TODO: add more jugement.
	return w.status.Load().(WorkerState)
}

// SetStatus change the status of worker
func (w *Worker) SetStatus(s WorkerState) {
	w.status.Store(s)
}

// CreateMysqlTask in a idle worker
func (w *Worker) CreateMysqlTask(ctx context.Context, c *config.WorkerConfig, d time.Duration) (*pb.MysqlTaskResponse, error) {
	content, err := c.Toml()
	if err != nil {
		return nil, err
	}
	ownerReq := &workerrpc.Request{
		Type: workerrpc.CmdCreateMysqlWorker,
		MysqlWorkerRequest: &pb.MysqlTaskRequest{
			Config: content,
		},
	}
	cli, err := w.GetClient()
	if err != nil {
		return nil, err
	}
	resp, err := cli.SendRequest(ctx, ownerReq, d)
	if err != nil {
		return nil, err
	}
	return resp.MysqlWorker, err
}

// UpdateMysqlConfig update mysql config in worker
func (w *Worker) UpdateMysqlConfig(ctx context.Context, c *config.WorkerConfig, d time.Duration) (*pb.MysqlTaskResponse, error) {
	content, err := c.Toml()
	if err != nil {
		return nil, err
	}
	ownerReq := &workerrpc.Request{
		Type: workerrpc.CmdUpdateMysqlConfig,
		MysqlWorkerRequest: &pb.MysqlTaskRequest{
			Config: content,
		},
	}
	resp, err := w.client.SendRequest(ctx, ownerReq, d)
	if err != nil {
		return nil, err
	}
	return resp.MysqlWorker, err
}

// StopMysqlTask update mysql config in worker
func (w *Worker) StopMysqlTask(ctx context.Context, sourceID string, d time.Duration) (*pb.MysqlTaskResponse, error) {
	ownerReq := &workerrpc.Request{
		Type: workerrpc.CmdStopMysqlWorker,
		StopMysqlWorker: &pb.StopMysqlTaskRequest{
			SourceID: sourceID,
		},
	}
	resp, err := w.client.SendRequest(ctx, ownerReq, d)
	if err != nil {
		return nil, err
	}
	return resp.MysqlWorker, err
}
