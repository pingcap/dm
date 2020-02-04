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
	"sync"
	"time"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/master/workerrpc"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
)

var (
	etcdTimeout               = 3 * time.Second
	restartMysqlWorkerTimeout = 5 * time.Second
)

// Coordinator coordinate wrokers and upstream.
type Coordinator struct {
	mu sync.RWMutex
	// address ->  worker
	workers map[string]*Worker
	// upstream(source-id) -> worker
	upstreams map[string]*Worker

	// upstream(address) -> source-id
	workerToSource map[string]string

	// sourceConfigs (source) -> config
	sourceConfigs map[string]config.MysqlConfig

	// pending create task (sourceid) --> address
	pendingReqSources map[string]string

	waitingTask chan string
	etcdCli     *clientv3.Client
	ctx         context.Context
	cancel      context.CancelFunc
	started     bool
	wg          sync.WaitGroup
}

// NewCoordinator returns a coordinate.
func NewCoordinator() *Coordinator {
	return &Coordinator{
		workers:           make(map[string]*Worker),
		workerToSource:    make(map[string]string),
		pendingReqSources: make(map[string]string),
		upstreams:         make(map[string]*Worker),
		sourceConfigs:     make(map[string]config.MysqlConfig),
		waitingTask:       make(chan string, 100000),
	}
}

// Start starts the coordinator and would recover infomation from etcd.
func (c *Coordinator) Start(ctx context.Context, etcdClient *clientv3.Client) error {
	// TODO: recover upstreams and workerToSource and workers
	// workers
	c.mu.Lock()
	defer c.mu.Unlock()
	c.etcdCli = etcdClient

	// recovering.
	ectx, cancel := context.WithTimeout(etcdClient.Ctx(), etcdTimeout)
	defer cancel()
	resp, err := etcdClient.Get(ectx, common.WorkerRegisterKeyAdapter.Path(), clientv3.WithPrefix())
	if err != nil {
		return err
	}

	for _, kv := range resp.Kvs {
		kvs, err := common.WorkerRegisterKeyAdapter.Decode(string(kv.Key))
		if err != nil {
			return terror.Annotate(err, "decode worker register key from etcd failed")
		}
		addr := kvs[0]
		name := string(kv.Value)
		c.workers[addr] = NewWorker(name, addr, nil)
		log.L().Info("load worker successful", zap.String("addr", addr), zap.String("name", name))
	}

	resp, err = etcdClient.Get(ectx, common.UpstreamConfigKeyAdapter.Path(), clientv3.WithPrefix())
	if err != nil {
		return err
	}

	for _, kv := range resp.Kvs {
		kvs, err := common.UpstreamConfigKeyAdapter.Decode(string(kv.Key))
		if err != nil {
			return terror.Annotate(err, "decode upstream config key from etcd failed")
		}
		sourceID := kvs[0]
		cfgStr := string(kv.Value)
		cfg := config.NewMysqlConfig()
		err = cfg.Parse(cfgStr)
		if err != nil {
			log.L().Error("cannot parse config", zap.String("source", sourceID), zap.Error(err))
			continue
		}
		c.sourceConfigs[sourceID] = *cfg
		c.schedule(sourceID)
		log.L().Info("load config successful", zap.String("source", sourceID), zap.String("config", cfgStr))
	}

	resp, err = etcdClient.Get(ectx, common.UpstreamBoundWorkerKeyAdapter.Path(), clientv3.WithPrefix())
	if err != nil {
		return err
	}

	for _, kv := range resp.Kvs {
		kvs, err := common.UpstreamBoundWorkerKeyAdapter.Decode(string(kv.Key))
		if err != nil {
			return terror.Annotate(err, "decode upstream bound worker key from etcd failed")
		}
		addr := kvs[0]
		sourceID := string(kv.Value)
		w, ok := c.workers[addr]
		if !ok {
			log.L().Error("worker not exist but binding relationship exist", zap.String("addr", addr), zap.String("source", sourceID))
			continue
		}
		gresp, err := etcdClient.Get(ectx, common.UpstreamConfigKeyAdapter.Encode(sourceID))
		if err != nil || len(gresp.Kvs) == 0 {
			log.L().Error("cannot load config", zap.String("addr", addr), zap.String("source", sourceID), zap.Error(err))
			continue
		}
		cfgStr := string(gresp.Kvs[0].Value)
		c.upstreams[sourceID] = w
		c.workerToSource[addr] = sourceID
		log.L().Info("load config successful", zap.String("source", sourceID), zap.String("config", cfgStr))
	}

	c.ctx, c.cancel = context.WithCancel(ctx)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.ObserveWorkers()
	}()
	// wait for ObserveWorkers to start
	time.Sleep(50 * time.Millisecond)
	c.started = true
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

// RemoveWorker removes the dm-worker to the coordinate.
func (c *Coordinator) RemoveWorker(address string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.workers, address)
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
		c.workerToSource[w.Address()] = cfg.SourceID
		c.sourceConfigs[cfg.SourceID] = *cfg
	} else {
		w.SetStatus(WorkerFree)
	}
	delete(c.pendingReqSources, cfg.SourceID)
}

// HandleStoppedWorker change worker status when mysql task stopped
func (c *Coordinator) HandleStoppedWorker(w *Worker, cfg *config.MysqlConfig) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.sourceConfigs, cfg.SourceID)
	delete(c.upstreams, cfg.SourceID)
	delete(c.workerToSource, w.Address())
	w.SetStatus(WorkerFree)
	return true
}

// AcquireWorkerForSource get the free worker to create mysql delay task, and add it to pending task
// to avoid create a task in two worker
func (c *Coordinator) AcquireWorkerForSource(source string) (*Worker, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started == false {
		return nil, terror.ErrMasterCoordinatorNotStart
	}
	if addr, ok := c.pendingReqSources[source]; ok {
		return nil, terror.ErrMasterAcquireWorkerFailed.Generatef("the same source has been started in worker: %s", addr)
	}
	if _, ok := c.sourceConfigs[source]; ok {
		// this check is used to avoid a situation: one task is started twice by mistake but requires two workers
		// If ok is true, there are two situations:
		// 1. this task is mistakenly started twice, when coordinator tried to operate on the bound worker it will report an error
		// 2. this task is paused because the bound worker was out of service before, we can give this task this worker to start it again
		// If ok is false, that means the try on the bound worker has failed, we can arrange this task another worker
		// ATTENTION!!! This mechanism can't prevent this case, which should be discussed later:
		// the task is being operating to a worker(sourceConfigs and upstreams haven't been updated), but it is started again to acquire worker
		if w, ok := c.upstreams[source]; ok {
			return w, nil
		}
	}
	for _, w := range c.workers {
		if w.status.Load() == WorkerFree {
			// we bound worker to avoid another task trying to get it
			w.status.Store(WorkerBound)
			c.pendingReqSources[source] = w.Address()
			return w, nil
		}
	}
	return nil, terror.ErrMasterAcquireWorkerFailed.Generate("no free worker could start mysql task")
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

// GetWorkerByAddress gets the worker through addr.
func (c *Coordinator) GetWorkerByAddress(addr string) *Worker {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.workers[addr]
}

// GetConfigBySourceID gets db config through source id.
func (c *Coordinator) GetConfigBySourceID(source string) *config.MysqlConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if cfg, ok := c.sourceConfigs[source]; ok {
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
					kvs, err := common.WorkerKeepAliveKeyAdapter.Decode(string(ev.Kv.Key))
					if err != nil {
						log.L().Warn("coordinator decode worker keep alive key from etcd failed", zap.String("key", string(ev.Kv.Key)), zap.Error(err))
						continue
					}
					addr, name := kvs[0], kvs[1]
					c.mu.Lock()
					if w, ok := c.workers[addr]; ok && name == w.Name() {
						state := "Free"
						if source, ok := c.workerToSource[addr]; ok {
							// The worker connect to master before we transfer mysqltask into another worker,
							// try schedule MySQL-task.
							if nowWorker, ok := c.upstreams[source]; ok && nowWorker.Address() == addr {
								// If the MySQL-task is still running in this worker.
								c.schedule(source)
								w.SetStatus(WorkerBound)
								state = "bound"
							} else if _, ok := c.upstreams[source]; !ok {
								// If the MySQL-task has not been assigned to others, It could try to schedule on self.
								c.upstreams[source] = w
								w.SetStatus(WorkerBound)
								c.schedule(source)
								state = "bound"
							} else {
								delete(c.workerToSource, addr)
								w.SetStatus(WorkerFree)
								state = "free"
							}
						} else {
							// If this worker has not been in 'workerToSource', it means that this worker must have lose connect from master more than 6s,
							// so the mysql task in worker had stop
							w.SetStatus(WorkerFree)
						}
						log.L().Info("worker became online ", zap.String("name", w.Name()), zap.String("address", w.Address()), zap.String("state", state))
					} else {
						// TODO: how to deal with unregister worker
					}
					c.mu.Unlock()
				case mvccpb.DELETE:
					log.L().Info("deletekv", zap.String("kv", string(ev.Kv.Key)))
					kvs, err := common.WorkerKeepAliveKeyAdapter.Decode(string(ev.Kv.Key))
					if err != nil {
						log.L().Warn("coordinator decode worker keep alive key from etcd failed", zap.String("key", string(ev.Kv.Key)), zap.Error(err))
						continue
					}
					addr, name := kvs[0], kvs[1]
					c.mu.Lock()
					if w, ok := c.workers[addr]; ok && name == w.Name() {
						log.L().Info("worker became offline, state: closed", zap.String("name", w.Name()), zap.String("address", w.Address()))
						// Set client nil, and send request use new request
						w.client = nil
						if source, ok := c.workerToSource[addr]; ok {
							c.schedule(source)
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

func (c *Coordinator) schedule(source string) {
	c.waitingTask <- source
}

func (c *Coordinator) tryRestartMysqlTask() {
	c.mu.RLock()
	defer c.mu.RUnlock()
	scheduleNextLoop := make([]string, 0)
	hasTaskToSchedule := true
	for hasTaskToSchedule {
		select {
		case source := <-c.waitingTask:
			log.L().Info("will schedule source", zap.String("source", source))
			if cfg, ok := c.sourceConfigs[source]; ok {
				ret := false
				if w, ok := c.upstreams[source]; ok {
					// Try start mysql task at the same worker.
					c.mu.RUnlock()
					log.L().Info("try start mysql task at the same worker", zap.String("worker", w.Address()))
					ret = c.restartMysqlTask(w, &cfg)
					c.mu.RLock()
				} else {
					c.mu.RUnlock()
					w, err := c.AcquireWorkerForSource(source)
					if err != nil {
						log.L().Error("acquire worker for source", zap.String("source", source), zap.Error(err))
					} else {
						if w != nil {
							ret = c.restartMysqlTask(w, &cfg)
						} else {
							log.L().Info("acquire worker for source get nil worker")
						}
					}
					c.mu.RLock()
				}
				if !ret {
					scheduleNextLoop = append(scheduleNextLoop, source)
				}
			}
		default:
			hasTaskToSchedule = false
			break
		}
	}

	for _, source := range scheduleNextLoop {
		c.waitingTask <- source
	}
}

func (c *Coordinator) restartMysqlTask(w *Worker, cfg *config.MysqlConfig) bool {
	log.L().Info("try to schedule ", zap.String("source", cfg.SourceID), zap.String("address", w.Address()))
	task, err := cfg.Toml()
	req := &pb.MysqlWorkerRequest{
		Op:     pb.WorkerOp_StartWorker,
		Config: task,
	}
	resp, err := w.OperateMysqlWorker(context.Background(), req, restartMysqlWorkerTimeout)
	ret := false
	c.mu.Lock()
	if err == nil {
		ret = resp.Result
		if resp.Result {
			c.workerToSource[w.Address()] = cfg.SourceID
			c.upstreams[cfg.SourceID] = w
			w.SetStatus(WorkerBound)
		} else {
			log.L().Warn("restartMysqlTask failed", zap.String("error", resp.Msg))
			delete(c.upstreams, cfg.SourceID)
			if source, ok := c.workerToSource[w.Address()]; ok {
				if source == cfg.SourceID {
					delete(c.workerToSource, w.Address())
					w.SetStatus(WorkerFree)
				} else {
					// There may be another MySQL-task having been assigned to this worker.
					log.L().Warn("schedule start-task to a running-worker", zap.String("address", w.Address()),
						zap.String("running-source", source), zap.String("schedule-source", cfg.SourceID))
				}
			} else {
				w.SetStatus(WorkerFree)
			}
		}
	} else {
		// Error means there is something wrong about network, set worker to close.
		// remove sourceID from upstreams. So the source would be schedule in other worker.
		log.L().Warn("operate mysql worker", zap.Error(err), zap.Stringer("request", req))
		delete(c.upstreams, cfg.SourceID)
		delete(c.workerToSource, w.Address())
		w.SetStatus(WorkerClosed)
	}
	delete(c.pendingReqSources, cfg.SourceID)
	c.mu.Unlock()
	if w.State() == WorkerClosed {
		ectx, cancel := context.WithTimeout(c.etcdCli.Ctx(), etcdTimeout)
		defer cancel()
		resp, err := c.etcdCli.Get(ectx, common.WorkerKeepAliveKeyAdapter.Encode(w.Address(), w.Name()))
		if err != nil {
			if resp != nil && resp.Count > 0 {
				w.SetStatus(WorkerFree)
			}
		}
	}
	return ret
}
