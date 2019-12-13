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
	if !ok || w.Status() == WorkerClosed {
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

func (c *Coordinator) GetWorkersByStatus(s WorkerStatus) []*Worker {
	c.mu.RLock()
	defer c.mu.RUnlock()
	res := make([]*Worker, 0, len(c.workers))
	for _, w := range c.workers {
		if w.Status() == s {
			res = append(res, w)
		}
	}
	return res
}

func (c *Coordinator) Maintain(ctx context.Context, client *clientv3.Client) {
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
					key := string(ev.Kv.Key)
					slice := strings.Split(string(key), ",")
					addr, name := slice[1], slice[2]
					c.mu.Lock()
					if w, ok := c.workers[addr]; ok && name == w.Name() {
						w.setStatus(WorkerFree)
					}
					c.mu.Unlock()

				case mvccpb.DELETE:
					key := string(ev.Kv.Key)
					slice := strings.Split(string(key), ",")
					addr, name := slice[1], slice[2]
					c.mu.Lock()
					if w, ok := c.workers[addr]; ok && name == w.Name() {
						w.setStatus(WorkerClosed)
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

// TODO: bind the worker the upstreams and set the status to Bound.
func (c *Coordinator) Schedule() {

}
