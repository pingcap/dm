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
	"sync"

	"github.com/pingcap/dm/dm/master/workerrpc"
)

// Coordinator coordinate wrokers and upstream.
type Coordinator struct {
	mu sync.RWMutex
	//address to worker
	workers map[string]*Worker
}

func NewCoordinator() *Coordinator {
	return &Coordinator{
		workers: make(map[string]*Worker),
	}
}

func (c *Coordinator) AddWorker(name string, address string) error {
	c.mu.Lock()
	defer c.mu.Lock()
	w, err := NewWorker(name, address)
	if err != nil {
		return err
	}
	c.workers[address] = w
	return nil
}

func (c *Coordinator) GetWorkerByAddress(address string) *Worker {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.workers[address]
}

func (c *Coordinator) GetWorkerClientByAddress(address string) workerrpc.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()
	w, ok := c.workers[address]
	if !ok || w.Status() == Disconnect {
		return nil
	}
	return w.GetClient()
}

func (c *Coordinator) GetAllWorkers() map[string]*Worker {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.workers
}

func (c *Coordinator) GetWorkerBySourceID(source string) *Worker {
	return nil
}

func (c *Coordinator) GetAllIdleWorkers() []*Worker {
	return nil
}

func (c *Coordinator) GetWorkersByStatus(state WorkerStatus) []*Worker {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return nil
}
