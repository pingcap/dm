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

package optimism

import (
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/schemacmp"

	"github.com/pingcap/dm/pkg/shardddl"
)

type optimisticLockImpl struct {
	tables map[string]schemacmp.Table
	joined schemacmp.Table
	synced bool // whether last call to TrySync() returns true
}

func NewLock(ID, task, owner string, table *model.TableInfo, sources []string) *shardddl.Lock {
	return shardddl.NewLock(ID, task, owner, sources, &optimisticLockImpl{
		tables: make(map[string]schemacmp.Table),
		joined: schemacmp.Encode(table),
		synced: true,
	})
}

func (l *optimisticLockImpl) AddSources(sources []string) {
	for _, source := range sources {
		if _, ok := l.tables[source]; !ok {
			l.tables[source] = l.joined
		}
	}
}

func (l *optimisticLockImpl) TrySync(caller string, ddls []string, newTableInfo *model.TableInfo) ([]string, error) {
	newTable := schemacmp.Encode(newTableInfo)
	newJoined := newTable

	oldJoined := l.joined

	// special case: if the DDL does not affect the schema at all, assume it is
	// idempotent and just execute the DDL directly.
	if cmpRes, err := newJoined.Compare(oldJoined); err == nil && cmpRes == 0 {
		l.synced = true
		return ddls, nil
	}

	l.tables[caller] = newTable
	for s, table := range l.tables {
		if s != caller {
			var err error
			newJoined, err = newJoined.Join(table)
			if err != nil {
				l.synced = false
				return nil, err
			}
		}
	}

	l.joined = newJoined

	// FIXME: Compute DDLs through schema diff instead of propagating DDLs directly.
	cmpRes, err := oldJoined.Compare(newJoined)
	l.synced = err == nil
	if err != nil || cmpRes == 0 {
		ddls = nil
	}
	return ddls, err
}

func (l *optimisticLockImpl) ForceSynced() {
	// FIXME: not sure if this is the correct implementation!
	for s := range l.tables {
		l.tables[s] = l.joined
	}
	l.synced = true
}

func (l *optimisticLockImpl) UnsyncCount() int {
	if l.synced {
		return 0
	}
	return len(l.tables)
}

func (l *optimisticLockImpl) Ready() map[string]bool {
	if l.synced {
		return make(map[string]bool)
	}
	ret := make(map[string]bool, len(l.tables))
	for s := range l.tables {
		ret[s] = false
	}
	return ret
}
