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
	"sync/atomic"

	"github.com/pingcap/dm/dm/master/workerrpc"
)

type WorkerStatus int

// the status of worker
const (
	WorkerClosed WorkerStatus = iota + 1
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

func NewWorker(name, address string) (*Worker, error) {
	client, err := workerrpc.NewGRPCClient(address)
	if err != nil {
		return nil, err
	}
	w := &Worker{
		name:    name,
		address: address,
		client:  client,
	}
	w.status.Store(WorkerClosed)
	return w, nil
}

// GetClient returns the client of the worker.
func (w *Worker) GetClient() workerrpc.Client {
	return w.client
}

// Name returns the name of the worker.
func (w *Worker) Name() string {
	return w.name
}

// Address returns the address of the worker.
func (w *Worker) Address() string {
	return w.address
}

// Status returns the status of the worker.
func (w *Worker) Status() WorkerStatus {
	// TODO: add more jugement.
	return w.status.Load().(WorkerStatus)
}

func (w *Worker) setStatus(s WorkerStatus) {
	w.status.Store(s)
}
