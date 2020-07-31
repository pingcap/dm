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
	"fmt"
	"sync"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/schemacmp"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/master/metrics"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
)

// Lock represents the shard DDL lock in memory.
// This information does not need to be persistent, and can be re-constructed from the shard DDL info.
type Lock struct {
	mu sync.RWMutex

	ID   string // lock's ID
	Task string // lock's corresponding task name

	DownSchema string // downstream schema name
	DownTable  string // downstream table name

	// current joined info.
	joined schemacmp.Table
	// per-table's table info,
	// upstream source ID -> upstream schema name -> upstream table name -> table info.
	// if all of them are the same, then we call the lock `synced`.
	tables map[string]map[string]map[string]schemacmp.Table
	synced bool

	// whether DDLs operations have done (execute the shard DDL) to the downstream.
	// if all of them have done and have the same schema, then we call the lock `resolved`.
	// in optimistic mode, one table should only send a new table info (and DDLs) after the old one has done,
	// so we can set `done` to `false` when received a table info (and need the table to done some DDLs),
	// and mark `done` to `true` after received the done status of the DDLs operation.
	done map[string]map[string]map[string]bool
}

// NewLock creates a new Lock instance.
// NOTE: we MUST give the initial table info when creating the lock now.
func NewLock(ID, task, downSchema, downTable string, ti *model.TableInfo, tts []TargetTable) *Lock {
	l := &Lock{
		ID:         ID,
		Task:       task,
		DownSchema: downSchema,
		DownTable:  downTable,
		joined:     schemacmp.Encode(ti),
		tables:     make(map[string]map[string]map[string]schemacmp.Table),
		done:       make(map[string]map[string]map[string]bool),
		synced:     true,
	}
	l.addTables(tts)
	metrics.ReportDDLPending(task, metrics.DDLPendingNone, metrics.DDLPendingSynced)

	return l
}

// TrySync tries to sync the lock, re-entrant.
// new upstream sources may join when the DDL lock is in syncing,
// so we need to merge these new sources.
// NOTE: now, any error returned, we treat it as conflict detected.
// NOTE: now, DDLs (not empty) returned when resolved the conflict, but in fact these DDLs should not be replicated to the downstream.
// NOTE: now, `TrySync` can detect and resolve conflicts in both of the following modes:
//   - non-intrusive: update the schema of non-conflict tables to match the conflict tables.
//                      data from conflict tables are non-intrusive.
//   - intrusive: revert the schema of the conflict tables to match the non-conflict tables.
//                  data from conflict tables are intrusive.
// TODO: but both of these modes are difficult to be implemented in DM-worker now, try to do that later.
// for non-intrusive, a broadcast mechanism needed to notify conflict tables after the conflict has resolved, or even a block mechanism needed.
// for intrusive, a DML prune or transform mechanism needed for two different schemas (before and after the conflict resolved).
func (l *Lock) TrySync(callerSource, callerSchema, callerTable string,
	ddls []string, newTI *model.TableInfo, tts []TargetTable) (newDDLs []string, err error) {
	l.mu.Lock()
	defer func() {
		if len(newDDLs) > 0 {
			// revert the `done` status if need to wait for the new operation to be done.
			// Now, we wait for the new operation to be done if any DDLs returned.
			l.tryRevertDone(callerSource, callerSchema, callerTable)
		}
		l.mu.Unlock()
	}()

	// handle the case where <callerSource, callerSchema, callerTable>
	// is not in old source tables and current new source tables.
	// duplicate append is not a problem.
	tts = append(tts, newTargetTable(l.Task, callerSource, l.DownSchema, l.DownTable,
		map[string]map[string]struct{}{callerSchema: {callerTable: struct{}{}}}))
	// add any new source tables.
	l.addTables(tts)

	var emptyDDLs = []string{}
	oldTable := l.tables[callerSource][callerSchema][callerTable]
	newTable := schemacmp.Encode(newTI)

	// special case: check whether DDLs making the schema become part of larger and another part of smaller.
	if _, err = oldTable.Compare(newTable); err != nil {
		return emptyDDLs, terror.ErrShardDDLOptimismTrySyncFail.Delegate(
			err, l.ID, fmt.Sprintf("there will be conflicts if DDLs %s are applied to the downstream. old table info: %s, new table info: %s", ddls, oldTable, newTable))
	}

	oldJoined := l.joined
	newJoined := newTable
	l.tables[callerSource][callerSchema][callerTable] = newTable
	log.L().Info("update table info", zap.String("lock", l.ID), zap.String("source", callerSource), zap.String("schema", callerSchema), zap.String("table", callerTable),
		zap.Stringer("from", oldTable), zap.Stringer("to", newTable), zap.Strings("ddls", ddls))

	oldSynced := l.synced
	defer func() {
		_, remain := l.syncStatus()
		l.synced = remain == 0
		if oldSynced != l.synced {
			if oldSynced {
				metrics.ReportDDLPending(l.Task, metrics.DDLPendingSynced, metrics.DDLPendingUnSynced)
			} else {
				metrics.ReportDDLPending(l.Task, metrics.DDLPendingUnSynced, metrics.DDLPendingSynced)
			}
		}
	}()

	// special case: if the DDL does not affect the schema at all, assume it is
	// idempotent and just execute the DDL directly.
	// if any real conflicts after joined exist, they will be detected by the following steps.
	var cmp int
	if cmp, err = newTable.Compare(oldJoined); err == nil && cmp == 0 {
		return ddls, nil
	}

	// try to join tables.
	for source, schemaTables := range l.tables {
		for schema, tables := range schemaTables {
			for table, ti := range tables {
				if source != callerSource || schema != callerSchema || table != callerTable {
					newJoined2, err2 := newJoined.Join(ti)
					if err2 != nil {
						// NOTE: conflict detected.
						return emptyDDLs, terror.ErrShardDDLOptimismTrySyncFail.Delegate(
							err2, l.ID, fmt.Sprintf("fail to join table info %s with %s", newJoined, ti))
					}
					newJoined = newJoined2
				}
			}
		}
	}

	// update the current joined table info, if it's actually changed it should be logged in `if cmp != 0` block.
	l.joined = newJoined

	cmp, err = oldJoined.Compare(newJoined)
	// FIXME: Compute DDLs through schema diff instead of propagating DDLs directly.
	// and now we MUST ensure different sources execute same DDLs to the downstream multiple times is safe.
	if err != nil {
		// resolving conflict in non-intrusive mode.
		log.L().Warn("resolving conflict", zap.String("lock", l.ID), zap.String("source", callerSource), zap.String("schema", callerSchema), zap.String("table", callerTable),
			zap.Stringer("joined-from", oldJoined), zap.Stringer("joined-to", newJoined), zap.Strings("ddls", ddls))
		return ddls, nil
	}
	if cmp != 0 {
		// < 0: the joined schema become larger after applied these DDLs.
		//      this often happens when executing `ADD COLUMN` for the FIRST table.
		// > 0: the joined schema become smaller after applied these DDLs.
		//      this often happens when executing `DROP COLUMN` for the LAST table.
		// for these two cases, we should execute the DDLs to the downstream to update the schema.
		log.L().Info("joined table info changed", zap.String("lock", l.ID), zap.Int("cmp", cmp), zap.Stringer("from", oldJoined), zap.Stringer("to", newJoined),
			zap.String("source", callerSource), zap.String("schema", callerSchema), zap.String("table", callerTable), zap.Strings("ddls", ddls))
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

	cmp, _ = oldTable.Compare(newTable) // we have checked `err` returned above.
	if cmp < 0 {
		// let every table to replicate the DDL.
		return ddls, nil
	} else if cmp > 0 {
		// do nothing except the last table.
		return emptyDDLs, nil
	}

	// compare the current table's info with joined info.
	cmp, err = newTable.Compare(newJoined)
	if err != nil {
		return emptyDDLs, terror.ErrShardDDLOptimismTrySyncFail.Delegate(
			err, l.ID, "can't compare table info (new table info) %s with (new joined table info) %s", newTable, newJoined) // NOTE: this should not happen.
	}
	if cmp < 0 {
		// no need to replicate DDLs, because has a larger joined schema (in the downstream).
		// FIXME: if the previous tables reached the joined schema has not replicated to the downstream,
		// now, they should re-try until replicated successfully, try to implement better strategy later.
		return emptyDDLs, nil
	}
	log.L().Warn("new table info >= new joined table info", zap.Stringer("table info", newTable), zap.Stringer("joined table info", newJoined))
	return ddls, nil // NOTE: this should not happen.
}

// TryRemoveTable tries to remove a table in the lock.
// it returns whether the table has been removed.
// TODO: it does NOT try to rebuild the joined schema after the table removed now.
// try to support this if needed later.
// NOTE: if no table exists in the lock after removed the table,
// it's the caller's responsibility to decide whether remove the lock or not.
func (l *Lock) TryRemoveTable(source, schema, table string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.tables[source]; !ok {
		return false
	}
	if _, ok := l.tables[source][schema]; !ok {
		return false
	}

	ti, ok := l.tables[source][schema][table]
	if !ok {
		return false
	}

	delete(l.tables[source][schema], table)
	_, remain := l.syncStatus()
	l.synced = remain == 0
	delete(l.done[source][schema], table)
	log.L().Info("table removed from the lock", zap.String("lock", l.ID),
		zap.String("source", source), zap.String("schema", schema), zap.String("table", table),
		zap.Stringer("table info", ti))
	return true
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

// Joined returns the joined table info.
func (l *Lock) Joined() schemacmp.Table {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.joined
}

// TryMarkDone tries to mark the operation of the source table as done.
// it returns whether marked done.
// NOTE: this method can always mark a existing table as done,
// so the caller of this method should ensure that the table has done the DDLs operation.
// NOTE: a done table may revert to not-done if new table schema received and new DDLs operation need to be done.
func (l *Lock) TryMarkDone(source, schema, table string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.done[source]; !ok {
		return false
	}
	if _, ok := l.done[source][schema]; !ok {
		return false
	}
	if _, ok := l.done[source][schema][table]; !ok {
		return false
	}

	// always mark it as `true` now.
	l.done[source][schema][table] = true
	return true
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

// IsResolved returns whether the lock has resolved.
// return true if all tables have the same schema and all DDLs operations have done.
func (l *Lock) IsResolved() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// whether all tables have the same schema.
	_, remain := l.syncStatus()
	if remain != 0 {
		return false
	}

	// whether all tables have done DDLs operations.
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

// tryRevertDone tries to revert the done status when the table's schema changed.
func (l *Lock) tryRevertDone(source, schema, table string) {
	if _, ok := l.done[source]; !ok {
		return
	}
	if _, ok := l.done[source][schema]; !ok {
		return
	}
	if _, ok := l.done[source][schema][table]; !ok {
		return
	}
	l.done[source][schema][table] = false
}

// addTables adds any not-existing tables into the lock.
func (l *Lock) addTables(tts []TargetTable) {
	for _, tt := range tts {
		if _, ok := l.tables[tt.Source]; !ok {
			l.tables[tt.Source] = make(map[string]map[string]schemacmp.Table)
			l.done[tt.Source] = make(map[string]map[string]bool)
		}
		for schema, tables := range tt.UpTables {
			if _, ok := l.tables[tt.Source][schema]; !ok {
				l.tables[tt.Source][schema] = make(map[string]schemacmp.Table)
				l.done[tt.Source][schema] = make(map[string]bool)
			}
			for table := range tables {
				if _, ok := l.tables[tt.Source][schema][table]; !ok {
					// NOTE: the newly added table uses the current table info.
					l.tables[tt.Source][schema][table] = l.joined
					l.done[tt.Source][schema][table] = false
					log.L().Info("table added to the lock", zap.String("lock", l.ID),
						zap.String("source", tt.Source), zap.String("schema", schema), zap.String("table", table),
						zap.Stringer("table info", l.joined))
				}
			}
		}
	}
}
