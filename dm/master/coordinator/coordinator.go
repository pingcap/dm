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
	"github.com/pingcap/dm/dm/config"
	"strings"
	"sync"

	"github.com/pingcap/dm/dm/master/workerrpc"
	"github.com/pingcap/dm/pkg/log"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"
)

var (
	workerKeepAlivePath = "/dm-worker/a"
)

// Coordinator coordinate wrokers and upstream.
type Coordinator struct {
	mu sync.RWMutex
	// address ->  worker
	workers map[string]*Worker
	// upstream(source-id) -> worker
	upstreams map[string]*Worker

	// upstream(address) -> config
	configs map[string]config.WorkerConfig

	// pending create taks (sourceid) --> address
	pendingtask map[string]string
}

// NewCoordinator returns a coordinate.
func NewCoordinator() *Coordinator {
	return &Coordinator{
		workers:     make(map[string]*Worker),
		configs:     make(map[string]config.WorkerConfig),
		pendingtask: make(map[string]string),
		upstreams:   make(map[string]*Worker),
	}
}

// Init would recover infomation from etcd
func (c *Coordinator) Init(etcdClient *clientv3.Client) {
	// TODO: recover upstreams and configs and workers
}

// AddWorker add the dm-worker to the coordinate.
func (c *Coordinator) AddWorker(name string, address string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.workers[address] = NewWorker(name, address)
}

// HandleStartedWorker change worker status when mysql task started
func (c *Coordinator) HandleStartedWorker(w *Worker, cfg *config.WorkerConfig, succ bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if succ {
		c.upstreams[cfg.SourceID] = w
		c.configs[w.Address()] = *cfg
	} else {
		w.SetStatus(WorkerFree)
	}
	delete(c.pendingtask, cfg.SourceID)
}

// HandleStoppedWorker change worker status when mysql task stopped
func (c *Coordinator) HandleStoppedWorker(w *Worker, cfg *config.WorkerConfig) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.upstreams, cfg.SourceID)
	delete(c.configs, w.Address())
	w.SetStatus(WorkerFree)
}

// GetFreeWorkerForSource get the free worker to create mysql delay task, and add it to pending task
// to avoid create a task in two worker
func (c *Coordinator) GetFreeWorkerForSource(source string) (*Worker, string) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if addr, ok := c.pendingtask[source]; ok {
		return nil, addr
	}
	for _, w := range c.workers {
		if w.status.Load() == WorkerFree {
			// we bound worker to avoid another task trying to get it
			w.status.Store(WorkerBound)
			c.pendingtask[source] = w.Address()
			return w, ""
		}
	}
	return nil, ""
}

// GetWorkerByAddress gets the worker through address.
func (c *Coordinator) GetWorkerByAddress(address string) *Worker {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.workers[address]
}

// GetWorkerClientByAddress gets the client of the worker through address.
func (c *Coordinator) GetWorkerClientByAddress(address string) workerrpc.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()
	w, ok := c.workers[address]
	if !ok || w.State() == WorkerClosed {
		log.L().Error("worker is not health", zap.Stringer("worker", w))
		return nil
	}
	client, err := w.GetClient()
	if err != nil {
		log.L().Error("cannot get client", zap.String("worker-name", w.Name()))
		return nil
	}
	return client
}

// GetAllWorkers gets all workers.
func (c *Coordinator) GetAllWorkers() map[string]*Worker {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.workers
}

// GetWorkerBySourceID gets the worker through source id.
func (c *Coordinator) GetWorkerBySourceID(source string) *Worker {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.upstreams[source]
}

// GetWorkersByStatus gets the workers match the specified status.
func (c *Coordinator) GetWorkersByStatus(s WorkerState) []*Worker {
	c.mu.RLock()
	defer c.mu.RUnlock()
	res := make([]*Worker, 0, len(c.workers))
	for _, w := range c.workers {
		if w.State() == s {
			res = append(res, w)
		}
	}
	return res
}

// ObserveWorkers observe the keepalive path and maintain the status of the worker.
func (c *Coordinator) ObserveWorkers(ctx context.Context, client *clientv3.Client) {
	watcher := clientv3.NewWatcher(client)
	ch := watcher.Watch(ctx, workerKeepAlivePath, clientv3.WithPrefix())

	for {
		select {
		case wresp := <-ch:
			if wresp.Canceled {
				log.L().Error("leader watcher is canceled with", zap.Error(wresp.Err()))
				return
			}

			for _, ev := range wresp.Events {
				switch ev.Type {
				case mvccpb.PUT:
					log.L().Info("putkv", zap.String("kv", string(ev.Kv.Key)))
					key := string(ev.Kv.Key)
					slice := strings.Split(string(key), ",")
					addr, name := slice[1], slice[2]
					c.mu.Lock()
					if w, ok := c.workers[addr]; ok && name == w.Name() {
						log.L().Info("worker became online, state: free", zap.String("name", w.Name()), zap.String("address", w.Address()))
						w.SetStatus(WorkerFree)
					} else {
						// TODO: how to deal with unregister worker
					}
					c.mu.Unlock()

				case mvccpb.DELETE:
					log.L().Info("deletekv", zap.String("kv", string(ev.Kv.Key)))
					key := string(ev.Kv.Key)
					slice := strings.Split(string(key), ",")
					addr, name := slice[1], slice[2]
					c.mu.Lock()
					if w, ok := c.workers[addr]; ok && name == w.Name() {
						log.L().Info("worker became offline, state: closed", zap.String("name", w.Name()), zap.String("address", w.Address()))
						w.SetStatus(WorkerClosed)
						cfg := c.configs[addr]
						delete(c.upstreams, cfg.SourceID)
						delete(c.configs, addr)
					}
					c.mu.Unlock()
				}
			}
		case <-ctx.Done():
			log.L().Info("coordinate exict due to context canceled")
			return
		}
	}
}

// Schedule schedules a free worker to a upstream.
// TODO: bind the worker the upstreams and set the status to Bound.
func (c *Coordinator) Schedule() {

}
