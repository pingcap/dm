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
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/pingcap/parser/model"

	"github.com/pingcap/dm/pkg/shardddl"
)

type Lock = shardddl.Lock

type pessimisticLockImpl struct {
	DDLs   []string // DDL statements
	remain int      // remain count of sources needed to receive DDL info

	// whether the DDL info received from the source.
	// if all of them have been ready, then we call the lock `synced`.
	ready map[string]bool
}

// NewLock creates a new pessimistic Lock instance.
func NewLock(ID, task, owner string, DDLs, sources []string) *shardddl.Lock {
	lock := shardddl.NewLock(ID, task, owner, sources, &pessimisticLockImpl{
		DDLs:  DDLs,
		ready: make(map[string]bool, len(sources)),
	})
	return lock
}

func (l *pessimisticLockImpl) AddSources(sources []string) {
	for _, source := range sources {
		if _, ok := l.ready[source]; !ok {
			l.remain++
			l.ready[source] = false
		}
	}
}

// TrySync tries to sync the lock, does decrease on remain, re-entrant.
// new upstream sources may join when the DDL lock is in syncing,
// so we need to merge these new sources.
func (l *pessimisticLockImpl) TrySync(caller string, DDLs []string, _ *model.TableInfo) ([]string, error) {
	// check DDL statement first.
	if !utils.CompareShardingDDLs(DDLs, l.DDLs) {
		return nil, terror.ErrMasterShardingDDLDiff.Generate(l.DDLs, DDLs)
	}

	// only `sync` once.
	if synced, ok := l.ready[caller]; ok && !synced {
		l.remain--
		l.ready[caller] = true
	}

	return l.DDLs, nil
}

func (l *pessimisticLockImpl) ForceSynced() {
	for source := range l.ready {
		l.ready[source] = true
	}
	l.remain = 0
}

func (l *pessimisticLockImpl) UnsyncCount() int {
	return l.remain
}

// Ready returns the sources sync status or whether they are ready.
func (l *pessimisticLockImpl) Ready() map[string]bool {
	ret := make(map[string]bool, len(l.ready))
	for k, v := range l.ready {
		ret[k] = v
	}
	return ret
}
