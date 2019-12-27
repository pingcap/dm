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
	"github.com/pingcap/dm/dm/pb"
	"sync"
	"time"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/master/workerrpc"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/errors"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"
)

var (
	etcdTimeouit = 3 * time.Second

	// ErrNotStarted coordinator does not start.
	ErrNotStarted = errors.New("coordinator does not start")
)

// Coordinator coordinate wrokers and upstream.
type Coordinator struct {
	mu sync.RWMutex
	// address ->  worker
	workers map[string]*Worker
	// upstream(source-id) -> worker
	upstreams map[string]*Worker

	// upstream(address) -> config
	configs map[string]config.MysqlConfig

	// pending create task (sourceid) --> address
	pendingtask map[string]string

	waitingTask map[string]config.MysqlConfig
	etcdCli     *clientv3.Client
	ctx         context.Context
	cancel      context.CancelFunc
	started     bool
	wg          sync.WaitGroup
}

// NewCoordinator returns a coordinate.
func NewCoordinator() *Coordinator {
	return &Coordinator{
		workers:     make(map[string]*Worker),
		configs:     make(map[string]config.MysqlConfig),
		pendingtask: make(map[string]string),
		upstreams:   make(map[string]*Worker),
		waitingTask: make(map[string]config.MysqlConfig),
	}
}

// Start starts the coordinator and would recover infomation from etcd.
func (c *Coordinator) Start(ctx context.Context, etcdClient *clientv3.Client) error {
	// TODO: recover upstreams and configs and workers
	// workers
	c.mu.Lock()
	defer c.mu.Unlock()
	c.etcdCli = etcdClient

	// recovering.
	ectx, cancel := context.WithTimeout(etcdClient.Ctx(), etcdTimeouit)
	defer cancel()
	resp, err := etcdClient.Get(ectx, common.WorkerRegisterKeyAdapter.Path(), clientv3.WithPrefix())
	if err != nil {
		return err
	}

	for _, kv := range resp.Kvs {
		addr := common.WorkerRegisterKeyAdapter.Decode(string(kv.Key))[0]
		name := string(kv.Value)
		c.workers[addr] = NewWorker(name, addr, nil)
		log.L().Info("load worker successful", zap.String("addr", addr), zap.String("name", name))
	}

	// configs
	resp, err = etcdClient.Get(ectx, common.UpstreamBoundWorkerKeyAdapter.Path(), clientv3.WithPrefix())
	if err != nil {
		return nil
	}

	for _, kv := range resp.Kvs {
		addr := common.UpstreamBoundWorkerKeyAdapter.Decode(string(kv.Key))[0]
		sourceID := string(kv.Value)
		w, ok := c.workers[addr]
		if !ok {
			log.L().Error("worker not exist but binding relationship exist", zap.String("addr", addr), zap.String("source", sourceID))
			continue
		}
		gresp, err := etcdClient.Get(ctx, common.UpstreamConfigKeyAdapter.Encode(sourceID))
		if err != nil || len(gresp.Kvs) == 0 {
			log.L().Error("cannot load config", zap.String("addr", addr), zap.String("source", sourceID), zap.Error(err))
			continue
		}
		cfgStr := string(gresp.Kvs[0].Value)
		cfg := config.NewMysqlConfig()
		err = cfg.Parse(cfgStr)
		if err != nil {
			log.L().Error("cannot parse config", zap.String("addr", addr), zap.String("source", sourceID), zap.Error(err))
			continue
		}
		c.upstreams[sourceID] = w
		log.L().Info("load config successful", zap.String("source", sourceID), zap.String("config", cfgStr))
	}
	// TODO: recover subtask

	c.started = true
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.ObserveWorkers()
	}()
	log.L().Info("coordinator is started")
	return nil
}

// IsStarted checks if the coordinator is started.
func (c *Coordinator) IsStarted() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.started
}

// Stop stops the coordinator.
func (c *Coordinator) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cancel()
	c.started = false
	c.wg.Wait()
	log.L().Info("coordinator is stoped")
}

// AddWorker add the dm-worker to the coordinate.
func (c *Coordinator) AddWorker(name string, address string, cli workerrpc.Client) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if w, ok := c.workers[address]; ok {
		w.SetStatus(WorkerFree)
		return
	}
	w := NewWorker(name, address, cli)
	c.workers[address] = w
}

// HandleStartedWorker change worker status when mysql task started
func (c *Coordinator) HandleStartedWorker(w *Worker, cfg *config.MysqlConfig, succ bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if succ {
		c.upstreams[cfg.SourceID] = w
		c.configs[w.Address()] = *cfg
	} else {
		w.SetStatus(WorkerFree)
		delete(c.pendingtask, cfg.SourceID)
	}
}

// HandleStoppedWorker change worker status when mysql task stopped
func (c *Coordinator) HandleStoppedWorker(w *Worker, cfg *config.MysqlConfig) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if w == nil {
		if _, ok := c.waitingTask[cfg.SourceID]; !ok {
			return false
		}
		// This mysqltask is waiting to be scheduled. So we just remove it from wait queue.
		delete(c.waitingTask, cfg.SourceID)
	} else {
		delete(c.upstreams, cfg.SourceID)
		delete(c.configs, w.Address())
		w.SetStatus(WorkerFree)
	}
	return true
}

// AcquireWorkerForSource get the free worker to create mysql delay task, and add it to pending task
// to avoid create a task in two worker
func (c *Coordinator) AcquireWorkerForSource(source string) (*Worker, error) {
	defer c.mu.RUnlock()
	if c.started == false {
		return nil, ErrNotStarted
	}
	if addr, ok := c.pendingtask[source]; ok {
		return nil, errors.Errorf("Acquire worker failed. the same source has been started in worker: %s", addr)
	}
	for _, w := range c.workers {
		if w.status.Load() == WorkerFree {
			// we bound worker to avoid another task trying to get it
			w.status.Store(WorkerBound)
			c.pendingtask[source] = w.Address()
			return w, nil
		}
	}
	return nil, errors.New("Acquire worker failed. no  free worker could start mysql task")
}

// GetAllWorkers gets all workers.
func (c *Coordinator) GetAllWorkers() map[string]*Worker {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.workers
}

// GetRunningMysqlSource gets all souce which is running.
func (c *Coordinator) GetRunningMysqlSource() map[string]*Worker {
	c.mu.RLock()
	defer c.mu.RUnlock()
	res := make(map[string]*Worker)
	for source, w := range c.upstreams {
		if w.State() == WorkerBound {
			res[source] = w
		}
	}
	return res
}

// GetWorkerBySourceID gets the worker through source id.
func (c *Coordinator) GetWorkerBySourceID(source string) *Worker {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.upstreams[source]
}

// GetConfigBySourceID gets db config through source id.
func (c *Coordinator) GetConfigBySourceID(source string) *config.MysqlConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if cfg, ok := c.configs[source]; ok {
		return &cfg
	}
	return nil
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
func (c *Coordinator) ObserveWorkers() {
	watcher := clientv3.NewWatcher(c.etcdCli)
	ch := watcher.Watch(c.ctx, common.WorkerKeepAliveKeyAdapter.Path(), clientv3.WithPrefix())
	t1 := time.NewTicker(time.Second * 6)
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
					kvs := common.WorkerKeepAliveKeyAdapter.Decode(string(ev.Kv.Key))
					addr, name := kvs[0], kvs[1]
					c.mu.Lock()
					if w, ok := c.workers[addr]; ok && name == w.Name() {
						log.L().Info("worker became online, state: free", zap.String("name", w.Name()), zap.String("address", w.Address()))
						if cfg, ok := c.configs[addr]; ok {
							// The worker connect to master before we transfer mysqltask into another worker,
							// try schedule mysqltask.
							c.schedule(cfg)
							w.SetStatus(WorkerBound)
						} else {
							// If this worker has not been in 'configs', it means that this worker must have lose connect from master more than 6s,
							// so the mysql task in worker had stop
							w.SetStatus(WorkerFree)
						}
					} else {
						// TODO: how to deal with unregister worker
					}
					c.mu.Unlock()
				case mvccpb.DELETE:
					log.L().Info("deletekv", zap.String("kv", string(ev.Kv.Key)))
					kvs := common.WorkerKeepAliveKeyAdapter.Decode(string(ev.Kv.Key))
					addr, name := kvs[0], kvs[1]
					c.mu.Lock()
					if w, ok := c.workers[addr]; ok && name == w.Name() {
						log.L().Info("worker became offline, state: closed", zap.String("name", w.Name()), zap.String("address", w.Address()))
						if cfg, ok := c.configs[addr]; ok {
							c.schedule(cfg)
						}
					}
					c.mu.Unlock()
				}
			}
		case <-c.ctx.Done():
			log.L().Info("coordinate exict due to context canceled")
			return
		case <-t1.C:
			c.tryRestartMysqlTask()
		}
	}
}

// TODO: bind the worker the upstreams and set the status to Bound.
func (c *Coordinator) schedule(cfg config.MysqlConfig) {
	c.waitingTask[cfg.SourceID] = cfg
	// TODO: store waitingTask in ETCD
}

func (c *Coordinator) unschedule(cli *clientv3.Client, cfg config.MysqlConfig) {
	delete(c.waitingTask, cfg.SourceID)
	// TODO: store waitingTask in ETCD
}

func (c *Coordinator) tryRestartMysqlTask() {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for source, cfg := range c.waitingTask {
		if w, ok := c.upstreams[source]; ok {
			// Try start mysql task at the same worker.
			c.mu.RUnlock()
			c.restartMysqlTask(w, &cfg)
			c.mu.RLock()
		} else {
			c.mu.RUnlock()
			w, _ := c.AcquireWorkerForSource(source)
			if w != nil {
				c.restartMysqlTask(w, &cfg)
			}
			c.mu.RLock()
		}
	}
}

func (c *Coordinator) restartMysqlTask(w *Worker, cfg *config.MysqlConfig) {
	task, err := cfg.Toml()
	req := &pb.MysqlTaskRequest{
		Op:     pb.WorkerOp_StartWorker,
		Config: task,
	}
	resp, err := w.OperateMysqlTask(context.Background(), req, time.Second*10)
	c.mu.Lock()
	if err == nil {
		if resp.Result {
			delete(c.waitingTask, cfg.SourceID)
			c.configs[w.Address()] = *cfg
			c.upstreams[cfg.SourceID] = w
			w.SetStatus(WorkerBound)
		} else {
			delete(c.upstreams, cfg.SourceID)
			delete(c.configs, w.Address())
			w.SetStatus(WorkerFree)
		}
	} else {
		// Error means there is something wrong about network, set worker to close.
		delete(c.configs, w.Address())
		delete(c.upstreams, cfg.SourceID)
		w.SetStatus(WorkerClosed)
	}
	delete(c.pendingtask, cfg.SourceID)
	c.mu.Unlock()
}
