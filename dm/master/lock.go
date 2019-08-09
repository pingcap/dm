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

package master

import (
	"sync"

	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"

	"github.com/siddontang/go/sync2"
)

// Lock used for process synchronization
type Lock struct {
	sync.RWMutex
	ID        string           // lock's ID, constructed from task's name and SQL statement
	Task      string           // lock's corresponding task name
	Owner     string           // lock's Owner, a dm-worker
	Stmts     []string         // SQL statement
	remain    int              // remain count needed to sync
	ready     map[string]bool  // whether dm-worker is synced
	ddls      []string         // ddls of each dm-worker
	AutoRetry sync2.AtomicBool // whether re-try resolve at intervals
	Resolving sync2.AtomicBool // whether the lock is resolving
}

// NewLock creates a new Lock
func NewLock(id, task, owner string, stmts []string, workers []string) *Lock {
	l := &Lock{
		ID:     id,
		Task:   task,
		Owner:  owner,
		Stmts:  stmts,
		remain: len(workers),
		ready:  make(map[string]bool),
	}
	for _, w := range workers {
		l.ready[w] = false
	}
	return l
}

// TrySync tries to sync the lock, does decrease on remain, reentrant
// new workers may join after DDL lock is in syncing
// so we need to merge these new workers
func (l *Lock) TrySync(caller string, workers []string, ddls []string) (bool, int, error) {
	l.Lock()
	defer l.Unlock()
	for _, worker := range workers {
		if _, ok := l.ready[worker]; !ok {
			// new worker joined
			l.remain++
			l.ready[worker] = false
		}
	}

	if synced, ok := l.ready[caller]; ok {
		if len(l.ddls) == 0 {
			l.ddls = ddls
		} else if !utils.CompareShardingDDLs(ddls, l.ddls) {
			return l.remain <= 0, l.remain, terror.ErrMasterShardingDDLDiff.Generate(l.ddls, ddls)
		}

		if !synced {
			l.remain--
			l.ready[caller] = true
		}
	}
	return l.remain <= 0, l.remain, nil
}

// IsSync returns whether the lock has synced
func (l *Lock) IsSync() (bool, int) {
	l.RLock()
	defer l.RUnlock()
	return l.remain <= 0, l.remain
}

// Ready returns the dm-workers and whether it's ready synced
func (l *Lock) Ready() map[string]bool {
	l.RLock()
	defer l.RUnlock()
	// do a copy
	ret := make(map[string]bool)
	for k, v := range l.ready {
		ret[k] = v
	}
	return ret
}

// DDLs returns the DDLs in syncing
func (l *Lock) DDLs() []string {
	l.RLock()
	defer l.RUnlock()
	return l.ddls // never modify elem in slice, no copy
}
