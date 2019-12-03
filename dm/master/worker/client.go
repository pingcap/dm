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
	"sync"
	"sync/atomic"

	"github.com/pingcap/dm/dm/master/workerrpc"
)

// ClientHub is used to hold DM-worker clients information.
type ClientHub struct {
	mu     sync.RWMutex
	closed int32

	// key: source-id; Value: DM-worker address list.
	workersAddr map[string][]string

	// key: DM-worker address; Value: gRPC client to the DM-worker instance.
	workersCli map[string]workerrpc.Client
}

// NewClientHub creates a new ClientHub instance.
// - deployMap: used to load static deployment map of DM-worker instances.
// - etcdEndpoints: used to get/watch dynamic register/unregister of DM-worker instances.
func NewClientHub(deployMap map[string]string, etcdEndpoints []string) (hub *ClientHub, err error) {
	// load DM-worker addresses
	workersAddr := make(map[string][]string, len(deployMap))
	for sid, addr := range deployMap {
		// only source-id:worker-address = 1:1 is supported in static deployment map now,
		// but if needed we can support comma separated list like `192.168.0.101:8262,192.168.0.102:8262`.
		workersAddr[sid] = []string{addr}
	}

	// TODO: load worker addresses from etcd.

	// create gRPC client.
	workersCli := make(map[string]workerrpc.Client, len(workersAddr))
	defer func() {
		if err != nil {
			for _, cli := range workersCli {
				cli.Close()
			}
		}
	}()

	for _, addrs := range workersAddr {
		for _, addr := range addrs {
			var cli workerrpc.Client
			cli, err = workerrpc.NewGRPCClient(addr)
			if err != nil {
				return
			}
			workersCli[addr] = cli
		}
	}

	hub = &ClientHub{
		workersAddr: workersAddr,
		workersCli:  workersCli,
	}
	return
}

// Close closes the hub and release resources.
func (h *ClientHub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !atomic.CompareAndSwapInt32(&h.closed, 0, 1) {
		return
	}

	for _, cli := range h.workersCli {
		cli.Close()
	}
	h.workersAddr = nil
	h.workersCli = nil
}

// GetClientByWorker returns the client associated with the specified DM-worker address.
func (h *ClientHub) GetClientByWorker(workerAddr string) workerrpc.Client {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.workersCli[workerAddr]
}

// SetClientForWorker sets the client associated with the specified DM-worker address.
// This method is only used for testing.
func (h *ClientHub) SetClientForWorker(workerAddr string, cli workerrpc.Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.workersCli[workerAddr] = cli
}

// GetClientsBySourceID returns a list of clients associated with the specified source-id.
func (h *ClientHub) GetClientsBySourceID(sourceID string) []workerrpc.Client {
	h.mu.RLock()
	defer h.mu.RUnlock()

	addrs, ok := h.workersAddr[sourceID]
	if !ok || len(addrs) == 0 {
		return nil
	}

	clis := make([]workerrpc.Client, 0, len(addrs))
	for _, addr := range addrs {
		cli, ok := h.workersCli[addr]
		if !ok {
			continue
		}
		clis = append(clis, cli)
	}
	return clis
}

// GetWorkersBySourceID returns a list of worker address associated with the specified source-id.
func (h *ClientHub) GetWorkersBySourceID(sourceID string) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	addrs, ok := h.workersAddr[sourceID]
	if !ok {
		return nil
	}
	ret := make([]string, len(addrs))
	copy(ret, addrs)
	return ret
}

// SetWorkersForSourceID sets the list of worker address associated with the specified source-id.
// This method is only used for testing.
func (h *ClientHub) SetWorkersForSourceID(sourceID string, workers []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.workersAddr[sourceID] = workers
}

// AllWorkers returns the address of all DM-worker instances.
// This method is use to compatible with non-HA version of DM-worker,
// and may be removed later.
func (h *ClientHub) AllWorkers() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	workers := make([]string, 0, len(h.workersCli))
	for addr := range h.workersCli {
		workers = append(workers, addr)
	}
	return workers
}

// AllClients returns all clients.
// This method is use to compatible with non-HA version of DM-worker,
// and may be removed later.
func (h *ClientHub) AllClients() map[string]workerrpc.Client {
	h.mu.RLock()
	defer h.mu.RUnlock()
	// make a copy
	clis := make(map[string]workerrpc.Client, len(h.workersCli))
	for addr, cli := range h.workersCli {
		clis[addr] = cli
	}
	return clis
}
