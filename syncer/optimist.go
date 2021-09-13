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
	"context"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/shardddl/optimism"
	"github.com/pingcap/dm/pkg/terror"
)

// trackedDDL keeps data needed for schema tracker.
type trackedDDL struct {
	rawSQL string
	stmt   ast.StmtNode
	tables [][]*filter.Table
}

// initOptimisticShardDDL initializes the shard DDL support in the optimistic mode.
func (s *Syncer) initOptimisticShardDDL(ctx context.Context) error {
	// fetch tables from source and filter them
	sourceTables, err := s.fromDB.FetchAllDoTables(ctx, s.baList)
	if err != nil {
		return err
	}

	// convert according to router rules.
	// downstream-schema -> downstream-table -> upstream-schema -> upstream-table.
	// TODO: refine to downstream-ID -> upstream-ID
	mapper := make(map[string]map[string]map[string]map[string]struct{})
	for upSchema, UpTables := range sourceTables {
		for _, upTable := range UpTables {
			up := &filter.Table{Schema: upSchema, Name: upTable}
			down := s.renameShardingSchema(up)
			downSchema, downTable := down.Schema, down.Name
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
func (s *Syncer) handleQueryEventOptimistic(qec *queryEventContext) error {
	// interrupted after flush old checkpoint and before track DDL.
	failpoint.Inject("FlushCheckpointStage", func(val failpoint.Value) {
		err := handleFlushCheckpointStage(1, val.(int), "before track DDL")
		if err != nil {
			failpoint.Return(err)
		}
	})

	var (
		upTable   *filter.Table
		downTable *filter.Table

		isDBDDL  bool
		tiBefore *model.TableInfo
		tiAfter  *model.TableInfo
		tisAfter []*model.TableInfo
		err      error

		needHandleDDLs = qec.needHandleDDLs
		needTrackDDLs  = qec.needTrackDDLs
	)

	err = s.execError.Load()
	if err != nil {
		qec.tctx.L().Error("error detected when executing SQL job", log.ShortError(err))
		// nolint:nilerr
		return nil
	}

	switch needTrackDDLs[0].stmt.(type) {
	case *ast.CreateDatabaseStmt, *ast.DropDatabaseStmt, *ast.AlterDatabaseStmt:
		isDBDDL = true
	}

	for _, td := range needTrackDDLs {
		// check whether do shard DDL for multi upstream tables.
		if upTable != nil && upTable.String() != "``" && upTable.String() != td.tables[0][0].String() {
			return terror.ErrSyncerUnitDDLOnMultipleTable.Generate(qec.originSQL)
		}
		upTable = td.tables[0][0]
		downTable = td.tables[1][0]
	}

	if !isDBDDL {
		if _, ok := needTrackDDLs[0].stmt.(*ast.CreateTableStmt); !ok {
			tiBefore, err = s.getTable(qec.tctx, upTable, downTable)
			if err != nil {
				return err
			}
		}
	}

	for _, td := range needTrackDDLs {
		if err = s.trackDDL(qec.ddlSchema, td.rawSQL, td.tables, td.stmt, qec.eventContext); err != nil {
			return err
		}
		if !isDBDDL {
			tiAfter, err = s.getTable(qec.tctx, upTable, downTable)
			if err != nil {
				return err
			}
			tisAfter = append(tisAfter, tiAfter)
		}
	}

	// in optimistic mode, don't `saveTablePoint` before execute DDL,
	// because it has no `UnresolvedTables` to prevent the flush of this checkpoint.

	info := s.optimist.ConstructInfo(upTable.Schema, upTable.Name, downTable.Schema, downTable.Name, needHandleDDLs, tiBefore, tisAfter)

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
		op, err = s.optimist.GetOperation(qec.tctx.Ctx, info, rev+1)
		if err != nil {
			return err
		}
		s.tctx.L().Info("got a shard DDL lock operation", zap.Stringer("operation", op))
	}

	if op.ConflictStage == optimism.ConflictDetected {
		return terror.ErrSyncerShardDDLConflict.Generate(needHandleDDLs, op.ConflictMsg)
	}

	// updated needHandleDDLs to DDLs received from DM-master.
	needHandleDDLs = op.DDLs

	s.tctx.L().Info("start to handle ddls in optimistic shard mode", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))

	// interrupted after track DDL and before execute DDL.
	failpoint.Inject("FlushCheckpointStage", func(val failpoint.Value) {
		err = handleFlushCheckpointStage(2, val.(int), "before execute DDL")
		if err != nil {
			failpoint.Return(err)
		}
	})

	qec.ddlInfo = &shardingDDLInfo{
		name:   needTrackDDLs[0].tables[0][0].String(),
		tables: needTrackDDLs[0].tables,
		stmt:   needTrackDDLs[0].stmt,
	}
	job := newDDLJob(qec)
	err = s.addJobFunc(job)
	if err != nil {
		return err
	}

	err = s.execError.Load()
	if err != nil {
		s.tctx.L().Error("error detected when executing SQL job", log.ShortError(err))
		// nolint:nilerr
		return nil
	}

	for _, table := range qec.onlineDDLTables {
		s.tctx.L().Info("finish online ddl and clear online ddl metadata in optimistic shard mode",
			zap.String("event", "query"),
			zap.Strings("ddls", needHandleDDLs),
			zap.String("raw statement", qec.originSQL),
			zap.String("schema", table.Schema),
			zap.String("table", table.Name))
		err = s.onlineDDL.Finish(qec.tctx, table.Schema, table.Name)
		if err != nil {
			return terror.Annotatef(err, "finish online ddl on %s.%s", table.Schema, table.Name)
		}
	}

	s.tctx.L().Info("finish to handle ddls in optimistic shard mode", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))
	return nil
}

// trackInitTableInfoOptimistic tries to get the initial table info (before modified by other tables) and track it in optimistic shard mode.
func (s *Syncer) trackInitTableInfoOptimistic(origTable, renamedTable *filter.Table) (*model.TableInfo, error) {
	ti, err := s.optimist.GetTableInfo(renamedTable.Schema, renamedTable.Name)
	if err != nil {
		return nil, terror.ErrSchemaTrackerCannotGetTable.Delegate(err, origTable)
	}
	if ti != nil {
		err = s.schemaTracker.CreateTableIfNotExists(origTable, ti)
		if err != nil {
			return nil, terror.ErrSchemaTrackerCannotCreateTable.Delegate(err, origTable)
		}
	}
	return ti, nil
}
