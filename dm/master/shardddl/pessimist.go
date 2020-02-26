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

	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/shardddl/pessimism"
)

// Pessimist used to coordinate the shard DDL migration in pessimism mode.
type Pessimist struct {
	mu sync.Mutex

	logger log.Logger

	closed bool
	cancel context.CancelFunc
	wg     sync.WaitGroup

	cli *clientv3.Client
	lk  *pessimism.LockKeeper

	// taskSources used to get all sources relative to the given task.
	taskSources func(task string) []string
}

// NewPessimist creates a new Pessimist instance.
func NewPessimist(pLogger *log.Logger, taskSources func(task string) []string) *Pessimist {
	return &Pessimist{
		logger:      pLogger.WithFields(zap.String("component", "shard DDL pessimist")),
		closed:      true, // mark as closed before started.
		lk:          pessimism.NewLockKeeper(),
		taskSources: taskSources,
	}
}

// Start starts the shard DDL coordination in pessimism mode.
func (p *Pessimist) Start(pCtx context.Context, etcdCli *clientv3.Client) error {
	p.logger.Info("the shard DDL pessimist is starting")

	p.mu.Lock()
	defer p.mu.Unlock()

	p.lk.Clear() // clear all previous locks to support re-Start.

	// get the history shard DDL info.
	// for the sequence of coordinate a shard DDL lock, see `/pkg/shardddl/pessimism/doc.go`.
	ifm, rev1, err := pessimism.GetAllInfo(etcdCli)
	if err != nil {
		return err
	}
	p.logger.Info("get history shard DDL info", zap.Reflect("info", ifm), zap.Int64("revision", rev1))

	// get the history shard DDL lock operation.
	// the newly operations after this GET will be received through the WATCH with `rev2`,
	// and call `Lock.MarkDone` multiple times is fine.
	opm, rev2, err := pessimism.GetAllOperations(etcdCli)
	if err != nil {
		return err
	}
	p.logger.Info("get history shard DDL lock operation", zap.Reflect("operation", opm), zap.Int64("revision", rev2))

	// recover the shard DDL lock based on history shard DDL info & lock operation.
	err = p.recoverLocks(ifm, opm)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(pCtx)

	// watch for the shard DDL info and handle them.
	infoCh := make(chan pessimism.Info, 10)
	p.wg.Add(2)
	go func() {
		defer func() {
			p.wg.Done()
			close(infoCh)
		}()
		pessimism.WatchInfoPut(ctx, etcdCli, rev1+1, infoCh)
	}()
	go func() {
		defer p.wg.Done()
		p.handleInfoPut(ctx, infoCh)
	}()

	// watch for the shard DDL lock operation and handle them.
	opCh := make(chan pessimism.Operation, 10)
	p.wg.Add(2)
	go func() {
		defer func() {
			p.wg.Done()
			close(opCh)
		}()
		pessimism.WatchOperationPut(ctx, etcdCli, "", "", rev2+1, opCh)
	}()
	go func() {
		defer p.wg.Done()
		p.handleOperationPut(ctx, opCh)
	}()

	p.closed = false // started now.
	p.cancel = cancel
	p.cli = etcdCli
	p.logger.Info("the shard DDL pessimist has started")
	return nil
}

// Close closes the Pessimist instance.
func (p *Pessimist) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}

	if p.cancel != nil {
		p.cancel()
		p.cancel = nil
	}

	p.wg.Wait()
	p.closed = true // closed now.
	p.logger.Info("the shard DDL pessimist has closed")
}

// Locks return all shard DDL locks current exist.
func (p *Pessimist) Locks() map[string]*pessimism.Lock {
	return p.lk.Locks()
}

// recoverLocks recovers shard DDL locks based on shard DDL info and shard DDL lock operation.
func (p *Pessimist) recoverLocks(ifm map[string]map[string]pessimism.Info, opm map[string]map[string]pessimism.Operation) error {
	// construct locks based on the shard DDL info.
	for task, ifs := range ifm {
		sources := p.taskSources(task)
		// if no operation exists for the lock, we let the smallest (lexicographical order) source as the owner of the lock.
		// if any operation exists for the lock, we let the source with `exec=true` as the owner of the lock (the logic is below).
		for _, info := range pessimismInfoMapToSlice(ifs) {
			_, _, _, err := p.lk.TrySync(info, sources)
			if err != nil {
				return err
			}
		}
	}

	// update locks based on the lock operation.
	for _, ops := range opm {
		for source, op := range ops {
			lock := p.lk.FindLock(op.ID)
			if lock == nil {
				p.logger.Warn("no shard DDL lock exists for the operation", zap.Stringer("operation", op))
				continue
			}

			// if any operation exists, the lock must have been synced.
			lock.ForceSynced()

			if op.Done {
				lock.MarkDone(source)
			}
			if op.Exec {
				// restore the role of `owner` based on `exec` operation.
				// This is needed because `TrySync` can only set `owner` for the first call of the lock.
				p.logger.Info("restore the role of owner for the shard DDL lock", zap.String("lock", op.ID), zap.String("from", lock.Owner), zap.String("to", op.Source))
				lock.Owner = op.Source
			}
		}
	}

	// try to handle locks.
	for _, lock := range p.lk.Locks() {
		synced, remain := lock.IsSynced()
		if !synced {
			p.logger.Info("restored an un-synced shard DDL lock", zap.String("lock", lock.ID), zap.Int("remain", remain))
			continue
		}
		err := p.handleLock(lock.ID)
		if err != nil {
			return err
		}
	}

	return nil
}

// handleInfoPut handles the shard DDL lock info PUTed.
func (p *Pessimist) handleInfoPut(ctx context.Context, infoCh <-chan pessimism.Info) {
	for {
		select {
		case <-ctx.Done():
			return
		case info, ok := <-infoCh:
			if !ok {
				return
			}
			p.logger.Info("receive a shard DDL info", zap.Stringer("info", info))
			lockID, synced, remain, err := p.lk.TrySync(info, p.taskSources(info.Task))
			if err != nil {
				// TODO: add & update metrics.
				p.logger.Error("fail to try sync shard DDL lock", zap.Stringer("info", info), log.ShortError(err))
				continue
			} else if !synced {
				p.logger.Info("the shard DDL lock has not synced", zap.String("lock", lockID), zap.Int("remain", remain))
				continue
			}
			p.logger.Info("the shard DDL lock has synced", zap.String("lock", lockID))

			err = p.handleLock(lockID)
			if err != nil {
				// TODO: add & update metrics.
				p.logger.Error("fail to handle the shard DDL lock", zap.String("lock", lockID), log.ShortError(err))
				continue
			}
		}
	}
}

// handleOperationPut handles the shard DDL lock operations PUTed.
func (p *Pessimist) handleOperationPut(ctx context.Context, opCh <-chan pessimism.Operation) {
	for {
		select {
		case <-ctx.Done():
			return
		case op, ok := <-opCh:
			if !ok {
				return
			}
			p.logger.Info("receive a shard DDL lock operation", zap.Stringer("operation", op))
			if !op.Done {
				p.logger.Info("the shard DDL lock operation has not done", zap.Stringer("operation", op))
				continue
			}

			lock := p.lk.FindLock(op.ID)
			if lock == nil {
				p.logger.Warn("no lock for the shard DDL lock operation exist", zap.Stringer("operation", op))
				continue
			} else if synced, _ := lock.IsSynced(); !synced {
				// this should not happen in normal case.
				p.logger.Warn("the lock for the shard DDL lock operation has not synced", zap.Stringer("operation", op))
				continue
			}

			// update the `done` status of the lock and check whether is resolved.
			lock.MarkDone(op.Source)
			if lock.IsResolved() {
				p.logger.Info("the lock for the shard DDL lock operation has been resolved", zap.Stringer("operation", op))
				// remove all operations for this shard DDL lock.
				err := p.deleteOps(lock)
				if err != nil {
					// TODO: add & update metrics.
					p.logger.Error("fail to delete the shard DDL lock operations", zap.String("lock", lock.ID), log.ShortError(err))
				}
				p.lk.RemoveLock(lock.ID)
				p.logger.Info("the lock info for the shard DDL lock operation has been cleared", zap.Stringer("operation", op))
				continue
			}

			// one of the non-owner dm-worker instance has done the operation,
			// still need to wait for more `done` from other non-owner dm-worker instances.
			if op.Source != lock.Owner {
				p.logger.Info("the shard DDL lock operation of a non-owner has done", zap.Stringer("operation", op), zap.String("owner", lock.Owner))
				continue
			}

			// the owner has done the operation, put `skip` operation for non-owner dm-worker instances.
			// no need to `skipDone`, all of them should be not done just after the owner has done.
			err := p.putOpsForNonOwner(lock, false)
			if err != nil {
				// TODO: add & update metrics.
				p.logger.Error("fail to put skip shard DDL lock operations for non-owner", zap.String("lock", lock.ID), log.ShortError(err))
			}
		}
	}
}

// handleLock handles a single shard DDL lock.
func (p *Pessimist) handleLock(lockID string) error {
	lock := p.lk.FindLock(lockID)
	if lock == nil {
		return nil
	}
	if synced, _ := lock.IsSynced(); !synced {
		return nil // do not handle un-synced lock now.
	}

	// check whether the lock has resolved.
	if lock.IsResolved() {
		// remove all operations for this shard DDL lock.
		// this is to handle the case where dm-master exit before deleting operations for them.
		err := p.deleteOps(lock)
		if err != nil {
			return err
		}
		p.lk.RemoveLock(lock.ID)
		return nil
	}

	// check whether the owner has done.
	if lock.IsDone(lock.Owner) {
		// try to put the skip operation for non-owner dm-worker instances,
		// this is to handle the case where dm-master exit before putting operations for them.
		// use `skipDone` to avoid overwriting any existing operations.
		err := p.putOpsForNonOwner(lock, true)
		if err != nil {
			return err
		}
		return nil
	}

	// put `exec=true` for the owner and skip it if already existing.
	return p.putOpForOwner(lock, true)
}

// putOpForOwner PUTs the shard DDL lock operation for the owner into etcd.
func (p *Pessimist) putOpForOwner(lock *pessimism.Lock, skipDone bool) error {
	op := pessimism.NewOperation(lock.ID, lock.Task, lock.Owner, lock.DDLs, true, false)
	rev, succ, err := pessimism.PutOperations(p.cli, skipDone, op)
	if err != nil {
		return err
	}
	p.logger.Info("put exec shard DDL lock operation for the owner", zap.String("lock", lock.ID), zap.String("owner", lock.Owner), zap.Bool("already exist", !succ), zap.Int64("revision", rev))
	return nil
}

// putOpsForNonOwner PUTs shard DDL lock operations for non-owner dm-worker instances into etcd.
func (p *Pessimist) putOpsForNonOwner(lock *pessimism.Lock, skipDone bool) error {
	ready := lock.Ready()
	sources := make([]string, 0, len(ready)-1)
	ops := make([]pessimism.Operation, 0, len(ready)-1)
	for source := range ready {
		if source != lock.Owner {
			sources = append(sources, source)
			ops = append(ops, pessimism.NewOperation(lock.ID, lock.Task, source, lock.DDLs, false, false))
		}
	}
	rev, succ, err := pessimism.PutOperations(p.cli, skipDone, ops...)
	if err != nil {
		return err
	}
	p.logger.Info("put skip shard DDL lock operations for non-owner", zap.String("lock", lock.ID), zap.Strings("non-owner", sources), zap.Bool("already exist", !succ), zap.Int64("revision", rev))
	return nil
}

// deleteOps DELETEs shard DDL lock operations relative to the lock.
func (p *Pessimist) deleteOps(lock *pessimism.Lock) error {
	ready := lock.Ready()
	ops := make([]pessimism.Operation, 0, len(ready))
	for source := range ready {
		// When deleting operations, we do not verify the value of the operation now,
		// so simply set `exec=false` and `done=true`.
		ops = append(ops, pessimism.NewOperation(lock.ID, lock.Task, source, lock.DDLs, false, true))
	}
	rev, err := pessimism.DeleteOperations(p.cli, ops...)
	if err != nil {
		return err
	}
	p.logger.Info("delete shard DDL lock operations", zap.String("lock", lock.ID), zap.Int64("revision", rev))
	return err
}
