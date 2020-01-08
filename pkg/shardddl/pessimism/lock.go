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
	"sync"

	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

// Lock represents the shard DDL lock in memory.
// This information does not need to be persistent, and can be re-constructed from the shard DDL info.
type Lock struct {
	mu sync.RWMutex

	ID     string          // lock's ID
	Task   string          // lock's corresponding task name
	Owner  string          // Owner's source ID (not DM-worker's name)
	DDLs   []string        // DDL statements
	remain int             // remain count of sources needed to receive DDL info
	ready  map[string]bool // whether the DDL info received from the source
}

// NewLock creates a new Lock instance.
func NewLock(ID, task, owner string, DDLs, sources []string) *Lock {
	l := &Lock{
		ID:     ID,
		Task:   task,
		Owner:  owner,
		DDLs:   DDLs,
		remain: len(sources),
		ready:  make(map[string]bool),
	}
	for _, s := range sources {
		l.ready[s] = false
	}

	return l
}

// TrySync tries to sync the lock, does decrease on remain, re-entrant.
// new upstream sources may join when the DDL lock is in syncing,
// so we need to merge these new sources.
func (l *Lock) TrySync(caller string, DDLs, sources []string) (bool, int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// check DDL statement first.
	if !utils.CompareShardingDDLs(DDLs, l.DDLs) {
		return l.remain <= 0, l.remain, terror.ErrMasterShardingDDLDiff.Generate(l.DDLs, DDLs)
	}

	// try to merge any newly jointed sources.
	for _, s := range sources {
		if _, ok := l.ready[s]; !ok {
			l.remain++
			l.ready[s] = false
		}
	}

	// only `sync` once.
	if synced, ok := l.ready[caller]; ok && !synced {
		l.remain--
		l.ready[caller] = true
	}

	return l.remain <= 0, l.remain, nil
}

// Ready returns the sources sync status or whether they are ready.
func (l *Lock) Ready() map[string]bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	ret := make(map[string]bool, len(l.ready))
	for k, v := range l.ready {
		ret[k] = v
	}
	return ret
}
