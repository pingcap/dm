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
			if err != nil {
				o.logger.Error("non-retryable error occurred, optimist will quite now", zap.Error(err))
			}
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
	// we do not log `stm`, `ifm` and `opm` now, because they may too long in optimism mode.
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
	pCtx context.Context, etcdCli *clientv3.Client,
	revSource, revInfo, revOperation int64) error {
	ctx, cancel := context.WithCancel(pCtx)
	var wg sync.WaitGroup
	defer func() {
		cancel()
		wg.Wait()
	}()

	errCh := make(chan error, 10)

	// watch for source tables and handle them.
	sourceCh := make(chan optimism.SourceTables, 10)
	wg.Add(2)
	go func() {
		defer func() {
			wg.Done()
			close(sourceCh)
		}()
		optimism.WatchSourceTables(ctx, etcdCli, revSource+1, sourceCh, errCh)
	}()
	go func() {
		defer wg.Done()
		o.handleSourceTables(ctx, sourceCh)
	}()

	// watch for the shard DDL info and handle them.
	infoCh := make(chan optimism.Info, 10)
	wg.Add(2)
	go func() {
		defer func() {
			wg.Done()
			close(infoCh)
		}()
		optimism.WatchInfo(ctx, etcdCli, revInfo+1, infoCh, errCh)
	}()
	go func() {
		defer wg.Done()
		o.handleInfo(ctx, infoCh)
	}()

	// watch for the shard DDL lock operation and handle them.
	opCh := make(chan optimism.Operation, 10)
	wg.Add(2)
	go func() {
		defer func() {
			wg.Done()
			close(opCh)
		}()
		optimism.WatchOperationPut(ctx, etcdCli, "", "", "", "", revOperation+1, opCh, errCh)
	}()
	go func() {
		defer wg.Done()
		o.handleOperationPut(ctx, opCh)
	}()

	select {
	case err := <-errCh:
		return err
	case <-pCtx.Done():
		return nil
	}
}

// handleSourceTables handles PUT and DELETE for source tables.
func (o *Optimist) handleSourceTables(ctx context.Context, sourceCh <-chan optimism.SourceTables) {
	for {
		select {
		case <-ctx.Done():
			return
		case st, ok := <-sourceCh:
			if !ok {
				return
			}
			updated := o.tk.Update(st)
			o.logger.Info("receive source tables", zap.Stringer("source tables", st),
				zap.Bool("is deleted", st.IsDeleted), zap.Bool("updated", updated))
		}
	}
}

// handleInfo handles PUT and DELETE for the shard DDL info.
func (o *Optimist) handleInfo(ctx context.Context, infoCh <-chan optimism.Info) {
	for {
		select {
		case <-ctx.Done():
			return
		case info, ok := <-infoCh:
			if !ok {
				return
			}
			o.logger.Info("receive a shard DDL info", zap.Stringer("info", info), zap.Bool("is deleted", info.IsDeleted))

			if info.IsDeleted {
				lock := o.lk.FindLockByInfo(info)
				if lock == nil {
					// this often happen after the lock resolved.
					o.logger.Debug("lock for info not found", zap.Stringer("info", info))
					continue
				}
				// handle `DROP TABLE`, need to remove the table schema from the lock,
				// and remove the table name from table keeper.
				removed := lock.TryRemoveTable(info.Source, info.UpSchema, info.UpTable)
				o.logger.Debug("the table name remove from the table keeper", zap.Bool("removed", removed), zap.Stringer("info", info))
				removed = o.tk.RemoveTable(info.Task, info.Source, info.UpSchema, info.UpTable)
				o.logger.Debug("a table removed for info from the lock", zap.Bool("removed", removed), zap.Stringer("info", info))
				continue
			}

			added := o.tk.AddTable(info.Task, info.Source, info.UpSchema, info.UpTable)
			o.logger.Debug("a table added for info", zap.Bool("added", added), zap.Stringer("info", info))

			sts := o.tk.FindTables(info.Task) // TODO(csuzhangxc): handle sts is nil.
			lockID, newDDLs, err := o.lk.TrySync(info, sts)
			var cfStage = optimism.ConflictNone
			if err != nil {
				cfStage = optimism.ConflictDetected // we treat any errors returned from `TrySync` as conflict detected now.
				o.logger.Warn("error occur when trying to sync for shard DDL info, this often means shard DDL conflict detected",
					zap.String("lock", lockID), zap.Stringer("info", info), zap.Bool("is deleted", info.IsDeleted), log.ShortError(err))
			} else {
				o.logger.Info("the shard DDL lock returned some DDLs",
					zap.String("lock", lockID), zap.Strings("ddls", newDDLs), zap.Stringer("info", info), zap.Bool("is deleted", info.IsDeleted))
			}
			err = o.handleLock(lockID, info, newDDLs, cfStage)
			if err != nil {
				// TODO: add & update metrics.
				o.logger.Error("fail to handle the shard DDL lock", zap.String("lock", lockID), log.ShortError(err))
				continue
			}
		}
	}
}

// handleOperationPut handles PUT for the shard DDL lock operations.
func (o *Optimist) handleOperationPut(ctx context.Context, opCh <-chan optimism.Operation) {
	for {
		select {
		case <-ctx.Done():
			return
		case op, ok := <-opCh:
			if !ok {
				return
			}
			o.logger.Info("receive a shard DDL lock operation", zap.Stringer("operation", op))
			if !op.Done {
				o.logger.Info("the shard DDL lock operation has not done", zap.Stringer("operation", op))
				continue
			}

			lock := o.lk.FindLock(op.ID)
			if lock == nil {
				o.logger.Warn("no lock for the shard DDL lock operation exist", zap.Stringer("operation", op))
				continue
			} else if synced, _ := lock.IsSynced(); !synced {
				// this should not happen in normal case.
				o.logger.Warn("the lock for the shard DDL lock operation has not synced", zap.Stringer("operation", op))
				continue
			}

			done := lock.TryMarkDone(op.Source, op.UpSchema, op.UpTable)
			o.logger.Info("mark operation for a table as done", zap.Bool("done", done), zap.Stringer("operation", op))
			if !lock.IsResolved() {
				o.logger.Info("the lock is still not resolved", zap.Stringer("operation", op))
				continue
			}

			// the lock has done, remove the lock.
			o.logger.Info("the lock for the shard DDL lock operation has been resolved", zap.Stringer("operation", op))
			err := o.deleteInfosOps(lock)
			if err != nil {
				o.logger.Error("fail to delete the shard DDL infos and lock operations", zap.String("lock", lock.ID), log.ShortError(err))
			}
			o.lk.RemoveLock(lock.ID)
			o.logger.Info("the shard DDL infos and lock operations have been cleared", zap.Stringer("operation", op))
		}
	}
}

// handleLock handles a single shard DDL lock.
func (o *Optimist) handleLock(lockID string, info optimism.Info, newDDLs []string, cfStage optimism.ConflictStage) error {
	lock := o.lk.FindLock(lockID)
	if lock == nil {
		return nil
	}

	// check whether the lock has resolved.
	if lock.IsResolved() {
		// remove all operations for this shard DDL lock.
		// TODO(csuzhangxc:) remove lock in memory and in etcd.
		return nil
	}

	// put operation for the table. we don't set `skipDone=true` now,
	// because in optimism mode, one table may execute/done multiple DDLs but other tables may do nothing.
	op := optimism.NewOperation(lockID, lock.Task, info.Source, info.UpSchema, info.UpTable, newDDLs, cfStage, false)
	rev, succ, err := optimism.PutOperation(o.cli, false, op)
	if err != nil {
		return err
	}
	o.logger.Info("put shard DDL lock operation", zap.String("lock", lockID),
		zap.Stringer("operation", op), zap.Bool("already exist", !succ), zap.Int64("revision", rev))
	return nil
}

// deleteInfosOps DELETEs shard DDL lock info and operations.
func (o *Optimist) deleteInfosOps(lock *optimism.Lock) error {
	infos := make([]optimism.Info, 0)
	ops := make([]optimism.Operation, 0)
	for source, schemaTables := range lock.Ready() {
		for schema, tables := range schemaTables {
			for table := range tables {
				// NOTE: we rely on only `task`, `source`, `schema`, and `table` used for deletion.
				infos = append(infos, optimism.NewInfo(lock.Task, source, schema, table, "", "", nil, nil, nil))
				ops = append(ops, optimism.NewOperation(lock.ID, lock.Task, source, schema, table, nil, optimism.ConflictNone, false))
			}
		}
	}
	rev, err := optimism.DeleteInfosOperations(o.cli, infos, ops)
	if err != nil {
		return err
	}
	o.logger.Info("delete shard DDL infos and lock operations", zap.String("lock", lock.ID), zap.Int64("revision", rev))
	return nil
}
