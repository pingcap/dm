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
	mu sync.RWMutex

	logger log.Logger

	closed bool
	cancel context.CancelFunc
	wg     sync.WaitGroup

	cli *clientv3.Client
	lk  *pessimism.LockKeeper
}

// NewPessimist creates a new Pessimist instance.
func NewPessimist(pLogger *log.Logger) *Pessimist {
	return &Pessimist{
		logger: pLogger.WithFields(zap.String("component", "shard DDL pessimist")),
		closed: true, // mark as closed before started.
		lk:     pessimism.NewLockKeeper(),
	}
}

// Start starts the shard DDL coordination in pessimism mode.
func (p *Pessimist) Start(pCtx context.Context, etcdCli *clientv3.Client) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// get the history shard DDL info.
	ifm, rev, err := pessimism.GetAllInfo(etcdCli)
	if err != nil {
		return err
	}

	// get the history shard DDL lock operation.
	// the newly operations after this GET will be received through the WATCH with `rev`,
	// and call `Lock.MarkDone` multiple times is fine.
	opm, _, err := pessimism.GetAllOperations(etcdCli)
	if err != nil {
		return err
	}

	// recover the shard DDL lock based on history shard DDL info & lock operation.
	for _, ifs := range ifm {
		for source, info := range ifs {
			lockID, synced, _, err2 := p.lk.TrySync(info, []string{source})
			if err2 != nil {
				return err2
			} else if !synced {
				continue
			}

			if ops, ok1 := opm[info.Task]; ok1 {
				if op, ok2 := ops[source]; ok2 && op.Done {
					// FindLock should always return non-nil, because we called `TrySync` above.
					p.lk.FindLock(lockID).MarkDone(source)
				}
			}

			err2 = p.handleLock(lockID)
			if err2 != nil {
				return err2
			}
		}
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
		pessimism.WatchInfoPut(ctx, etcdCli, rev, infoCh)
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
		pessimism.WatchOperationPut(ctx, etcdCli, "", "", rev, opCh)
	}()
	go func() {
		defer p.wg.Done()
		p.handleOperationPut(ctx, opCh)
	}()

	p.closed = false // started now.
	p.cancel = cancel
	p.cli = etcdCli
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
			lockID, synced, remain, err := p.lk.TrySync(info, []string{info.Source})
			if err != nil {
				// TODO: add & update metrics.
				p.logger.Error("fail to try sync shard DDL lock", zap.Stringer("info", info), log.ShortError(err))
				continue
			} else if !synced {
				p.logger.Info("the shard DDL lock has not synced", zap.String("lock", lockID), zap.Int("remain", remain))
				continue
			}
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

			// get all sources relative to this lock for convenience.
			// NOTE: we do not support to add any new sources after the shard DDL lock has synced.
			ready := lock.Ready()

			// update the `done` status of the lock and check whether is resolved.
			lock.MarkDone(op.Source)
			if lock.IsResolved() {
				// remove all operations for this shard DDL lock.
				ops := make([]pessimism.Operation, 0, len(ready))
				for source := range ready {
					// When deleting operations, we do not verify the value of the operation now,
					// so simply set `exec=false` and `done=true`.
					ops = append(ops, pessimism.NewOperation(lock.ID, lock.Task, source, lock.DDLs, false, true))
				}
				_, err := pessimism.DeleteOperations(p.cli, ops...)
				if err != nil {
					// TODO: add & update metrics.
					p.logger.Error("fail to delete the shard DDL lock operations", zap.String("lock", lock.ID), log.ShortError(err))
				}
				p.lk.RemoveLock(lock.ID)
				continue
			}

			// one of the non-owner dm-worker instance has done the operation,
			// still need to wait for more `done` from other non-owner dm-worker instances.
			if op.Source != lock.Owner {
				p.logger.Info("the shard DDL lock operation of a non-owner has done", zap.Stringer("operation", op), zap.String("owner", lock.Owner))
				continue
			}

			// the owner has done the operation, put `skip` operation for non-owner dm-worker instances.
			ops := make([]pessimism.Operation, 0, len(ready)-1)
			for source := range ready {
				if source != lock.Owner {
					// always set `exec=false` for non-owner, and `done=false` just after the owner has done.
					ops = append(ops, pessimism.NewOperation(lock.ID, lock.Task, source, lock.DDLs, false, false))
				}
			}
			// no need to `skipDone`, all of them should be not done just after the owner has done.
			_, _, err := pessimism.PutOperations(p.cli, false, ops...)
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

	// check whether the owner has done.
	if lock.IsDone(lock.Owner) {
		// try to put the skip operation for non-owner dm-worker instances,
		// this is to handle the case where dm-master exist before putting operations for them.
		ready := lock.Ready()
		ops := make([]pessimism.Operation, 0, len(ready)-1)
		for source := range ready {
			if source != lock.Owner {
				ops = append(ops, pessimism.NewOperation(lock.ID, lock.Task, source, lock.DDLs, false, false))
			}
		}
		// use `skipDone` to avoid overwriting any existing operations.
		_, succ, err := pessimism.PutOperations(p.cli, true, ops...)
		if err != nil {
			return err
		}
		p.logger.Info("put skip shard DDL lock operations for non-owner", zap.String("lock", lockID), zap.Bool("already exist", !succ))
		return nil
	}

	// put `exec=true` for the owner.
	op := pessimism.NewOperation(lockID, lock.Task, lock.Owner, lock.DDLs, true, false)
	_, succ, err := pessimism.PutOperations(p.cli, true, op)
	if err != nil {
		return err
	}

	p.logger.Info("put exec shard DDL lock operation for the owner", zap.String("lock", lockID), zap.Bool("already exist", !succ))
	return nil
}
