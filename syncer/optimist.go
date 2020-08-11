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

package syncer

import (
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/siddontang/go-mysql/replication"
	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/shardddl/optimism"
	"github.com/pingcap/dm/pkg/terror"
)

// trackedDDL keeps data needed for schema tracker.
type trackedDDL struct {
	rawSQL     string
	stmt       ast.StmtNode
	tableNames [][]*filter.Table
}

// initOptimisticShardDDL initializes the shard DDL support in the optimistic mode.
func (s *Syncer) initOptimisticShardDDL() error {
	// fetch tables from source and filter them
	sourceTables, err := s.fromDB.fetchAllDoTables(s.baList)
	if err != nil {
		return err
	}

	// convert according to router rules.
	// downstream-schema -> downstream-table -> upstream-schema -> upstream-table.
	mapper := make(map[string]map[string]map[string]map[string]struct{})
	for upSchema, UpTables := range sourceTables {
		for _, upTable := range UpTables {
			downSchema, downTable := s.renameShardingSchema(upSchema, upTable)
			if _, ok := mapper[downSchema]; !ok {
				mapper[downSchema] = make(map[string]map[string]map[string]struct{})
			}
			if _, ok := mapper[downSchema][downTable]; !ok {
				mapper[downSchema][downTable] = make(map[string]map[string]struct{})
			}
			if _, ok := mapper[downSchema][downTable][upSchema]; !ok {
				mapper[downSchema][downTable][upSchema] = make(map[string]struct{})
			}
			mapper[downSchema][downTable][upSchema][upTable] = struct{}{}
		}
	}

	return s.optimist.Init(mapper)
}

// handleQueryEventOptimistic handles QueryEvent in the optimistic shard DDL mode.
func (s *Syncer) handleQueryEventOptimistic(
	ev *replication.QueryEvent, ec eventContext,
	needHandleDDLs []string, needTrackDDLs []trackedDDL,
	onlineDDLTableNames map[string]*filter.Table) error {
	// interrupted after flush old checkpoint and before track DDL.
	failpoint.Inject("FlushCheckpointStage", func(val failpoint.Value) {
		err := handleFlushCheckpointStage(1, val.(int), "before track DDL")
		if err != nil {
			failpoint.Return(err)
		}
	})

	var (
		upSchema   string
		upTable    string
		downSchema string
		downTable  string

		isDBDDL  bool
		tiBefore *model.TableInfo
		tiAfter  *model.TableInfo
		err      error
	)

	switch needTrackDDLs[0].stmt.(type) {
	case *ast.CreateDatabaseStmt, *ast.DropDatabaseStmt, *ast.AlterDatabaseStmt:
		isDBDDL = true
	}

	for _, td := range needTrackDDLs {
		// check whether do shard DDL for multi upstream tables.
		if upSchema != "" && upSchema != td.tableNames[0][0].Schema &&
			upTable != "" && upTable != td.tableNames[0][0].Name {
			return terror.ErrSyncerUnitDDLOnMultipleTable.Generate(string(ev.Query))
		}
		upSchema = td.tableNames[0][0].Schema
		upTable = td.tableNames[0][0].Name
		downSchema = td.tableNames[1][0].Schema
		downTable = td.tableNames[1][0].Name
	}

	if !isDBDDL {
		if _, ok := needTrackDDLs[0].stmt.(*ast.CreateTableStmt); !ok {
			tiBefore, err = s.getTable(upSchema, upTable, downSchema, downTable)
			if err != nil {
				return err
			}
		}
	}

	for _, td := range needTrackDDLs {
		if err = s.trackDDL(string(ev.Schema), td.rawSQL, td.tableNames, td.stmt, &ec); err != nil {
			return err
		}
	}

	if !isDBDDL {
		tiAfter, err = s.getTable(upSchema, upTable, downSchema, downTable)
		if err != nil {
			return err
		}
	}

	// in optimistic mode, don't `saveTablePoint` before execute DDL,
	// because it has no `UnresolvedTables` to prevent the flush of this checkpoint.

	info := s.optimist.ConstructInfo(upSchema, upTable, downSchema, downTable, needHandleDDLs, tiBefore, tiAfter)

	var (
		rev    int64
		skipOp bool
		op     optimism.Operation
	)
	switch needTrackDDLs[0].stmt.(type) {
	case *ast.CreateDatabaseStmt, *ast.AlterDatabaseStmt:
		// need to execute the DDL to the downstream, but do not do the coordination with DM-master.
		op.DDLs = needHandleDDLs
		skipOp = true
	case *ast.DropDatabaseStmt:
		skipOp = true
	case *ast.CreateTableStmt:
		info.TableInfoBefore = tiAfter // for `CREATE TABLE`, we use tiAfter as tiBefore.
		rev, err = s.optimist.PutInfoAddTable(info)
		if err != nil {
			return err
		}
	case *ast.DropTableStmt:
		// no operation exist for `DROP TABLE` now.
		_, err = s.optimist.DeleteInfoRemoveTable(info)
		if err != nil {
			return err
		}
		skipOp = true
	default:
		rev, err = s.optimist.PutInfo(info)
		if err != nil {
			return err
		}
	}

	if !skipOp {
		s.tctx.L().Info("putted a shard DDL info into etcd", zap.Stringer("info", info))
		op, err = s.optimist.GetOperation(ec.tctx.Ctx, info, rev+1)
		if err != nil {
			return err
		}
		s.tctx.L().Info("got a shard DDL lock operation", zap.Stringer("operation", op))
	}

	if op.ConflictStage == optimism.ConflictDetected {
		return terror.ErrSyncerShardDDLConflict.Generate(needHandleDDLs)
	}

	// updated needHandleDDLs to DDLs received from DM-master.
	needHandleDDLs = op.DDLs

	s.tctx.L().Info("start to handle ddls in optimistic shard mode", zap.String("event", "query"),
		zap.Strings("ddls", needHandleDDLs), zap.ByteString("raw statement", ev.Query), log.WrapStringerField("location", ec.currentLocation))

	// interrupted after track DDL and before execute DDL.
	failpoint.Inject("FlushCheckpointStage", func(val failpoint.Value) {
		err = handleFlushCheckpointStage(2, val.(int), "before execute DDL")
		if err != nil {
			failpoint.Return(err)
		}
	})

	ddlInfo := &shardingDDLInfo{
		name:       needTrackDDLs[0].tableNames[0][0].String(),
		tableNames: needTrackDDLs[0].tableNames,
		stmt:       needTrackDDLs[0].stmt,
	}
	job := newDDLJob(ddlInfo, needHandleDDLs, *ec.lastLocation, *ec.startLocation, *ec.currentLocation, *ec.traceID, nil)
	err = s.addJobFunc(job)
	if err != nil {
		return err
	}

	err = s.execError.Get()
	if err != nil {
		return terror.ErrSyncerUnitHandleDDLFailed.Delegate(err, ev.Query)
	}

	for _, table := range onlineDDLTableNames {
		s.tctx.L().Info("finish online ddl and clear online ddl metadata in optimistic shard mode", zap.String("event", "query"),
			zap.Strings("ddls", needHandleDDLs), zap.ByteString("raw statement", ev.Query),
			zap.String("schema", table.Schema), zap.String("table", table.Name))
		err = s.onlineDDL.Finish(ec.tctx, table.Schema, table.Name)
		if err != nil {
			return terror.Annotatef(err, "finish online ddl on %s.%s", table.Schema, table.Name)
		}
	}

	s.tctx.L().Info("finish to handle ddls in optimistic shard mode", zap.String("event", "query"),
		zap.Strings("ddls", needHandleDDLs), zap.ByteString("raw statement", ev.Query), log.WrapStringerField("location", ec.currentLocation))
	return nil
}

// trackInitTableInfoOptimistic tries to get the initial table info (before modified by other tables) and track it in optimistic shard mode.
func (s *Syncer) trackInitTableInfoOptimistic(origSchema, origTable, renamedSchema, renamedTable string) (*model.TableInfo, error) {
	ti, err := s.optimist.GetTableInfo(renamedSchema, renamedTable)
	if err != nil {
		return nil, terror.ErrSchemaTrackerCannotGetTable.Delegate(err, origSchema, origTable)
	}
	if ti != nil {
		err = s.schemaTracker.CreateTableIfNotExists(origSchema, origTable, ti)
		if err != nil {
			return nil, terror.ErrSchemaTrackerCannotCreateTable.Delegate(err, origSchema, origTable)
		}
	}
	return ti, nil
}
