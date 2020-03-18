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
	"sync"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/schemacmp"
	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/log"
)

// Lock represents the shard DDL lock in memory.
// This information does not need to be persistent, and can be re-constructed from the shard DDL info.
type Lock struct {
	mu sync.RWMutex

	ID   string // lock's ID
	Task string // lock's corresponding task name

	// current joined info.
	joined schemacmp.Table
	// per-table's table info,
	// upstream source ID -> schema name -> table name -> table info.
	// if all of them are the same, then we call the lock `synced`.
	tables map[string]map[string]map[string]schemacmp.Table

	// whether the operations have done (execute the shard DDL).
	// if all of them have done, then we call the lock `resolved`.
	done map[string]map[string]map[string]bool
}

// NewLock creates a new Lock instance.
// NOTE: we MUST give the initial table info when creating the lock now.
func NewLock(ID, task string, ti *model.TableInfo, sts []SourceTables) *Lock {
	l := &Lock{
		ID:     ID,
		Task:   task,
		joined: schemacmp.Encode(ti),
		tables: make(map[string]map[string]map[string]schemacmp.Table),
		done:   make(map[string]map[string]map[string]bool),
	}
	l.addSources(sts...)
	return l
}

// TrySync tries to sync the lock, re-entrant.
// new upstream sources may join when the DDL lock is in syncing,
// so we need to merge these new sources.
func (l *Lock) TrySync(callerSource, callerSchema, callerTable string,
	ddls []string, newTI *model.TableInfo, sts ...SourceTables) (newDDLs []string, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// add any new source tables.
	l.addSources(sts...)
	// handle the case where <callerSource, callerSchema, callerTable>
	// is not in old source tables and current new source tables.
	if _, ok := l.tables[callerSource]; !ok {
		l.tables[callerSource] = make(map[string]map[string]schemacmp.Table)
	}
	if _, ok := l.tables[callerSource][callerSchema]; !ok {
		l.tables[callerSource][callerSchema] = make(map[string]schemacmp.Table)
	}
	if _, ok := l.tables[callerSource][callerSchema][callerTable]; !ok {
		l.tables[callerSource][callerSchema][callerTable] = l.joined
	}

	oldTable := l.tables[callerSource][callerSchema][callerTable]
	newTable := schemacmp.Encode(newTI)
	oldJoined := l.joined
	newJoined := newTable
	l.tables[callerSource][callerSchema][callerTable] = newTable
	log.L().Info("update table info", zap.String("source", callerSource), zap.String("schema", callerSchema), zap.String("table", callerTable),
		zap.Stringer("from", oldTable), zap.Stringer("to", newTable), zap.Strings("ddls", ddls))

	// special case: if the DDL does not affect the schema at all, assume it is
	// idempotent and just execute the DDL directly.
	var cmp int
	if cmp, err = newJoined.Compare(oldJoined); err == nil && cmp == 0 {
		return ddls, nil
	}

	// try to join tables.
	var emptyDDLs = []string{}
	for source, schemaTables := range l.tables {
		for schema, tables := range schemaTables {
			for table, ti := range tables {
				if source != callerSource || schema != callerSchema || table != callerTable {
					var err2 error
					newJoined, err2 = newJoined.Join(ti)
					if err2 != nil {
						// NOTE: conflict detected.
						return emptyDDLs, err2
					}
				}
			}
		}
	}
	l.joined = newJoined // update the current table info.
	log.L().Info("update joined table info", zap.Stringer("from", oldJoined), zap.Stringer("to", newJoined))

	// FIXME: Compute DDLs through schema diff instead of propagating DDLs directly.
	// and now we MUST ensure different sources execute same DDLs to the downstream multiple times is safe.
	// TODO: update lock status (`done`) according to the compared result.
	if cmp, err = oldJoined.Compare(newJoined); err != nil {
		// NOTE: conflict detected.
		return emptyDDLs, err
	}
	if cmp != 0 {
		// < 0: the joined schema become larger after applied these DDLs.
		//      this often happens when executing `ADD COLUMN` for the FIRST table.
		// > 0: the joined schema become smaller after applied these DDLs.
		//      this often happens when executing `DROP COLUMN` for the LAST table.
		// for these two cases, we should execute the DDLs to the downstream to update the schema.
		return ddls, nil
	}

	// NOTE: now, different DM-workers do not wait for each other when executing DDL/DML,
	// when coordinating the shard DDL between multiple DM-worker instances,
	// a possible sequences:
	//   1. DM-worker-A do this `trySync` and DM-master let it to `ADD COLUMN`.
	//   2. DM-worker-B do this `trySync` again.
	//   3. DM-worker-B replicate DML to the downstream.
	//   4. DM-worker-A replicate `ADD COLUMN` to the downstream.
	// in order to support DML from DM-worker-B matches the downstream schema,
	// two strategies exist:
	//   A. DM-worker-B waits for DM-worker-A to finish the replication of the DDL before replicating DML.
	//   B. DM-worker-B also replicates the DDL before replicating DML,
	//      but this MUST ensure we can tolerate replicating the DDL multiple times.
	// for `DROP COLUMN` or other DDL which makes the schema become smaller,
	// this is not a problem because all DML with larger schema should already replicated to the downstream,
	// and any DML with smaller schema can fit both the larger or smaller schema.
	// To make it easy to implement, we will temporarily choose strategy-B.

	cmp, err = oldTable.Compare(newTable)
	if err != nil {
		return emptyDDLs, err // NOTE: this should not happen.
	}
	if cmp < 0 {
		// let every table to replicate the DDL.
		return ddls, nil
	} else if cmp > 0 {
		// do nothing except the last table.
		return emptyDDLs, nil
	}
	// nothing need to do,
	// but in order to be uniform with the previous case `newJoined.Compare(oldJoined)`,
	// we still return the DDLs (or maybe we can return empty DDLs for both of them later).
	return ddls, nil
}

// IsSynced returns whether the lock has synced.
// In the optimistic mode, we call it `synced` if table info of all tables are the same,
// and we define `remain` as the table count which have different table info with the joined one,
// e.g. for `ADD COLUMN`, it's the table count which have not added the column,
// for `DROP COLUMN`, it's the table count which have dropped the column.
func (l *Lock) IsSynced() (bool, int) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	_, remain := l.syncStatus()
	return remain == 0, remain
}

// Ready returns the source tables' sync status (whether they are ready).
// we define `ready` if the table's info is the same with the joined one,
// e.g for `ADD COLUMN`, it's true if it has added the column,
// for `DROP COLUMN`, it's true if it has not dropped the column.
func (l *Lock) Ready() map[string]map[string]map[string]bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	ready, _ := l.syncStatus()
	return ready
}

// MarkDone marks the operation of the source table as done.
func (l *Lock) MarkDone(source, schema, table string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.done[source]; !ok {
		return
	}
	if _, ok := l.done[source][schema]; !ok {
		return
	}
	if _, ok := l.done[source][schema][table]; !ok {
		return
	}
	l.done[source][schema][table] = true
}

// IsDone returns whether the operation of the source table has done.
func (l *Lock) IsDone(source, schema, table string) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if _, ok := l.done[source]; !ok {
		return false
	}
	if _, ok := l.done[source][schema]; !ok {
		return false
	}
	if _, ok := l.done[source][schema][table]; !ok {
		return false
	}
	return l.done[source][schema][table]
}

// IsResolved returns whether the lock has resolved (all operations have done).
func (l *Lock) IsResolved() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	for _, schemaTables := range l.done {
		for _, tables := range schemaTables {
			for _, done := range tables {
				if !done {
					return false
				}
			}
		}
	}
	return true
}

// syncedStatus returns the current tables' sync status (<Ready, remain>).
func (l *Lock) syncStatus() (map[string]map[string]map[string]bool, int) {
	ready := make(map[string]map[string]map[string]bool)
	remain := 0
	for source, schemaTables := range l.tables {
		if _, ok := ready[source]; !ok {
			ready[source] = make(map[string]map[string]bool)
		}
		for schema, tables := range schemaTables {
			if _, ok := ready[source][schema]; !ok {
				ready[source][schema] = make(map[string]bool)
			}
			for table, ti := range tables {
				if cmp, err := l.joined.Compare(ti); err == nil && cmp == 0 {
					ready[source][schema][table] = true
				} else {
					ready[source][schema][table] = false
					remain++
				}
			}
		}
	}
	return ready, remain
}

// addSources adds any not-existing tables into the lock.
func (l *Lock) addSources(sts ...SourceTables) {
	for _, st := range sts {
		if _, ok := l.tables[st.Source]; !ok {
			l.tables[st.Source] = make(map[string]map[string]schemacmp.Table)
			l.done[st.Source] = make(map[string]map[string]bool)
		}
		for schema, tables := range st.Tables {
			if _, ok := l.tables[st.Source][schema]; !ok {
				l.tables[st.Source][schema] = make(map[string]schemacmp.Table)
				l.done[st.Source][schema] = make(map[string]bool)
			}
			for table := range tables {
				if _, ok := l.tables[st.Source][schema][table]; !ok {
					// NOTE: the newly added table uses the current table info.
					l.tables[st.Source][schema][table] = l.joined
					l.done[st.Source][schema][table] = false
				}
			}
		}
	}
}
