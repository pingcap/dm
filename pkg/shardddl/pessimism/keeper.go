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

package pessimism

import (
	"fmt"
	"sync"

	"github.com/pingcap/tidb-tools/pkg/dbutil"

	"github.com/pingcap/dm/pkg/shardddl/optimism"
)

type LockMode uint32

const (
	LockModePessimistic LockMode = iota
	LockModeOptimistic
)

// LockKeeper used to keep and handle DDL lock conveniently.
// The lock information do not need to be persistent, and can be re-constructed from the shard DDL info.
type LockKeeper struct {
	mu    sync.RWMutex
	locks map[string]*Lock // lockID -> Lock
	mode  LockMode
}

// NewLockKeeper creates a new LockKeeper instance.
func NewLockKeeper() *LockKeeper {
	return &LockKeeper{
		locks: make(map[string]*Lock),
		mode:  LockModePessimistic,
	}
}

func (lk *LockKeeper) SetLockMode(mode LockMode) {
	lk.mu.Lock()
	lk.mode = mode
	lk.mu.Unlock()
}

func (lk *LockKeeper) LockMode() LockMode {
	lk.mu.RLock()
	mode := lk.mode
	lk.mu.RUnlock()
	return mode
}

// TrySync tries to sync the lock.
func (lk *LockKeeper) TrySync(info Info, sources []string) (string, bool, int, error) {
	var (
		lockID = genDDLLockID(info)
		l      *Lock
		ok     bool
	)

	lk.mu.Lock()
	defer lk.mu.Unlock()

	if l, ok = lk.locks[lockID]; !ok {
		switch lk.mode {
		case LockModePessimistic:
			l = NewLock(lockID, info.Task, info.Source, info.DDLs, sources)
		case LockModeOptimistic:
			l = optimism.NewLock(lockID, info.Task, info.Source, info.TableInfoBefore, sources)
		}

		lk.locks[lockID] = l
	}

	synced, remain, err := l.TrySync(info.Source, info.DDLs, info.TableInfoAfter, sources)
	return lockID, synced, remain, err
}

// RemoveLock removes a lock.
func (lk *LockKeeper) RemoveLock(lockID string) bool {
	lk.mu.Lock()
	defer lk.mu.Unlock()

	_, ok := lk.locks[lockID]
	delete(lk.locks, lockID)
	return ok
}

// FindLock finds a lock.
func (lk *LockKeeper) FindLock(lockID string) *Lock {
	lk.mu.RLock()
	defer lk.mu.RUnlock()

	return lk.locks[lockID]
}

// Locks return a copy of all Locks.
func (lk *LockKeeper) Locks() map[string]*Lock {
	lk.mu.RLock()
	defer lk.mu.RUnlock()

	locks := make(map[string]*Lock, len(lk.locks))
	for k, v := range lk.locks {
		locks[k] = v
	}
	return locks
}

// Clear clears all Locks.
func (lk *LockKeeper) Clear() {
	lk.mu.Lock()
	defer lk.mu.Unlock()

	lk.locks = make(map[string]*Lock)
}

// genDDLLockID generates DDL lock ID from its info.
func genDDLLockID(info Info) string {
	return fmt.Sprintf("%s-%s", info.Task, dbutil.TableName(info.Schema, info.Table))
}
