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

	for _, ifs := range ifm {
		for source, info := range ifs {
			lockID, synced, _, err2 := p.lk.TrySync(info, []string{source})
			if err2 != nil {
				return err2
			} else if !synced {
				continue
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
		p.handleInfo(ctx, infoCh)
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
		p.handleOperation(ctx, opCh)
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

// handleInfo handles the shard DDL lock info.
func (p *Pessimist) handleInfo(ctx context.Context, infoCh <-chan pessimism.Info) {
	for {
		select {
		case <-ctx.Done():
			return
		case info, ok := <-infoCh:
			if !ok {
				return
			}
			p.logger.Info("receive shard DDL info", zap.Stringer("info", info))
			lockID, synced, remain, err := p.lk.TrySync(info, []string{info.Source})
			if err != nil {
				p.logger.Error("fail to try sync shard DDL lock", zap.Stringer("info", info), log.ShortError(err))
				continue
			} else if !synced {
				p.logger.Info("shard DDL lock has not synced", zap.String("lock", lockID), zap.Int("remain", remain))
				continue
			}
			err = p.handleLock(lockID)
			if err != nil {
				p.logger.Error("fail to handle shard DDL lock", zap.String("lock ID", lockID), log.ShortError(err))
				continue
			}
		}
	}
}

// handleOperation handles the shard DDL lock operations.
func (p *Pessimist) handleOperation(ctx context.Context, opCh <-chan pessimism.Operation) {
	// TODO(csuzhangxc)
}

// handleLock handles a single shard DDL lock.
func (p *Pessimist) handleLock(lockID string) error {
	// handle for the synced lock
	lock := p.lk.FindLock(lockID)
	if lock == nil {
		return nil
	}
	if synced, _ := lock.IsSynced(); !synced {
		return nil // do not handle un-synced lock now.
	}

	// check whether the owner has done.

	// put `exec=true` for the owner & wait for the owner has done.

	// put `exec=false` for non-owner workers & wait for them have done.

	return nil
}
