// Copyright 2020 PingCAP, Inc.
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

package shardddl

import (
	"context"
	"sync"
	"time"

	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/etcdutil"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/shardddl/optimism"
)

// Optimist is used to coordinate the shard DDL migration in optimism mode.
type Optimist struct {
	mu sync.Mutex

	logger log.Logger

	closed bool
	cancel context.CancelFunc
	wg     sync.WaitGroup

	cli *clientv3.Client
	lk  *optimism.LockKeeper
	tk  *optimism.TableKeeper
}

// NewOptimist creates a new Optimist instance.
func NewOptimist(pLogger *log.Logger) *Optimist {
	return &Optimist{
		logger: pLogger.WithFields(zap.String("component", "shard DDL optimist")),
		closed: true,
		lk:     optimism.NewLockKeeper(),
		tk:     optimism.NewTableKeeper(),
	}
}

// Start starts the shard DDL coordination in optimism mode.
func (o *Optimist) Start(pCtx context.Context, etcdCli *clientv3.Client) error {
	o.logger.Info("the shard DDL optimist is starting")

	o.mu.Lock()
	defer o.mu.Unlock()

	revSource, revInfo, revOperation, err := o.rebuildLocks(etcdCli)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(pCtx)

	o.closed = false // started now, no error will interrupt the start process.
	o.cancel = cancel
	o.cli = etcdCli
	o.logger.Info("the shard DDL optimist has started")

	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		// TODO: handle fatal error from run
		o.run(ctx, etcdCli, revSource, revInfo, revOperation)
	}()

	return nil
}

// Close closes the Optimist instance.
func (o *Optimist) Close() {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return
	}

	if o.cancel != nil {
		o.cancel()
		o.cancel = nil
	}

	o.wg.Wait()
	o.closed = true // closed now.
	o.logger.Info("the shard DDL optimist has closed")
}

// Locks return all shard DDL locks current exist.
func (o *Optimist) Locks() map[string]*optimism.Lock {
	return o.lk.Locks()
}

// run runs jobs in the background.
func (o *Optimist) run(ctx context.Context, etcdCli *clientv3.Client, revSource, revInfo, revOperation int64) error {
	for {
		err := o.watchSourceInfoOperation(ctx, etcdCli, revSource, revInfo, revOperation)
		if etcdutil.IsRetryableError(err) {
			retryNum := 0
			for {
				retryNum++
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(500 * time.Millisecond):
					revSource, revInfo, revOperation, err = o.rebuildLocks(etcdCli)
					if err != nil {
						o.logger.Error("fail to rebuild shard DDL lock, will retry",
							zap.Int("retryNum", retryNum), zap.Error(err))
						continue
					}
				}
				break
			}
		} else {
			o.logger.Error("non-retryable error occurred, optimist will quite now", zap.Error(err))
			return err
		}
	}
}

// rebuildLocks rebuilds shard DDL locks from etcd persistent data.
func (o *Optimist) rebuildLocks(etcdCli *clientv3.Client) (revSource, revInfo, revOperation int64, err error) {
	o.lk.Clear() // clear all previous locks to support re-Start.

	// get the history & initial source tables.
	stm, revSource, err := optimism.GetAllSourceTables(etcdCli)
	if err != nil {
		return 0, 0, 0, err
	}
	o.logger.Info("get history initial source tables", zap.Int64("revision", revSource))
	o.tk.Init(stm) // re-initialize again with valid tables.

	// get the history shard DDL info.
	ifm, revInfo, err := optimism.GetAllInfo(etcdCli)
	if err != nil {
		return 0, 0, 0, err
	}
	o.logger.Info("get history shard DDL info", zap.Int64("revision", revInfo))

	// get the history shard DDL lock operation.
	// the newly operations after this GET will be received through the WATCH with `revOperation+1`,
	opm, revOperation, err := optimism.GetAllOperations(etcdCli)
	if err != nil {
		return 0, 0, 0, err
	}
	o.logger.Info("get history shard DDL lock operation", zap.Int64("revision", revOperation))

	// recover the shard DDL lock based on history shard DDL info & lock operation.
	err = o.recoverLocks(ifm, opm)
	if err != nil {
		return 0, 0, 0, err
	}
	return revSource, revInfo, revOperation, nil
}

// recoverLocks recovers shard DDL locks based on shard DDL info and shard DDL lock operation.
func (o *Optimist) recoverLocks(
	ifm map[string]map[string]map[string]map[string]optimism.Info,
	opm map[string]map[string]map[string]map[string]optimism.Operation) error {
	// TODO
	return nil
}

// watchSourceInfoOperation watches the etcd operation for source tables, shard DDL infos and shard DDL operations.
func (o *Optimist) watchSourceInfoOperation(
	ctx context.Context, etcdCli *clientv3.Client, revSource, revInfo, revOperation int64) error {
	// TODO:
	return nil
}
