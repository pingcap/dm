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
	sourceTables, err := s.fromDB.fetchAllDoTables(s.bwList)
	if err != nil {
		return err
	}

	return s.optimist.Init(sourceTables)
}

// handleQueryEventOptimistic handles QueryEvent in the optimistic shard DDL mode.
func (s *Syncer) handleQueryEventOptimistic(
	ev *replication.QueryEvent, ec eventContext,
	needHandleDDLs []string, needTrackDDLs []trackedDDL,
	onlineDDLTableNames map[string]*filter.Table) error {
	// wait previous DMLs to be replicated
	s.jobWg.Wait()

	var (
		upSchema   string
		upTable    string
		downSchema string
		downTable  string
	)
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

	var tiBefore *model.TableInfo
	if _, ok := needTrackDDLs[0].stmt.(*ast.CreateTableStmt); !ok {
		var err error
		tiBefore, err = s.getTable(upSchema, upTable, downSchema, downTable, ec.parser2)
		if err != nil {
			return err
		}
	}

	for _, td := range needTrackDDLs {
		if err := s.trackDDL(string(ev.Schema), td.rawSQL, td.tableNames, td.stmt, &ec); err != nil {
			return err
		}
	}

	// TODO(csuzhangxc): rollback schema in the tracker if failed.
	tiAfter, err := s.getTable(upSchema, upTable, downSchema, downTable, ec.parser2)
	if err != nil {
		return err
	}

	s.tctx.L().Info("save table checkpoint", zap.String("event", "query"),
		zap.String("schema", upSchema), zap.String("table", upTable),
		zap.Strings("ddls", needHandleDDLs), log.WrapStringerField("location", ec.currentLocation))
	s.saveTablePoint(upSchema, upTable, ec.currentLocation.Clone())

	info := s.optimist.ConstructInfo(upSchema, upTable, downSchema, downTable, needHandleDDLs, tiBefore, tiAfter)

	var (
		rev    int64
		skipOp bool
	)
	switch needTrackDDLs[0].stmt.(type) {
	case *ast.CreateDatabaseStmt, *ast.DropDatabaseStmt:
	// for CREATE/DROP DATABASE, we do nothing now.
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
		needHandleDDLs = []string{} // no DDL needs to be handled for `DROP TABLE` now.
	default:
		rev, err = s.optimist.PutInfo(info)
		if err != nil {
			return err
		}
	}
	s.tctx.L().Info("putted a shard DDL info into etcd", zap.Stringer("info", info))

	var op optimism.Operation
	if !skipOp {
		op, err = s.optimist.GetOperation(ec.tctx.Ctx, info, rev+1)
		if err != nil {
			return err
		}
	}
	s.tctx.L().Info("got a shard DDL lock operation", zap.Stringer("operation", op))

	if op.ConflictStage == optimism.ConflictDetected {
		return terror.ErrSyncerShardDDLConflict.Generate(needHandleDDLs)
	}

	// updated needHandleDDLs to DDLs received from DM-master.
	needHandleDDLs = op.DDLs

	s.tctx.L().Info("start to handle ddls in optimistic shard mode", zap.String("event", "query"),
		zap.Strings("ddls", needHandleDDLs), zap.ByteString("raw statement", ev.Query), log.WrapStringerField("location", ec.currentLocation))

	// try apply SQL operator before addJob. now, one query event only has one DDL job, if updating to multi DDL jobs, refine this.
	applied, appliedSQLs, err := s.tryApplySQLOperator(ec.currentLocation.Clone(), needHandleDDLs)
	if err != nil {
		return terror.Annotatef(err, "try apply SQL operator on binlog-location %s with DDLs %v", ec.currentLocation, needHandleDDLs)
	}
	if applied {
		s.tctx.L().Info("replace ddls to preset ddls by sql operator in shard mode", zap.String("event", "query"),
			zap.Strings("preset ddls", appliedSQLs), zap.Strings("ddls", needHandleDDLs), zap.ByteString("raw statement", ev.Query),
			log.WrapStringerField("location", ec.currentLocation))
		needHandleDDLs = appliedSQLs // maybe nil
	}

	ddlInfo := &shardingDDLInfo{
		name:       needTrackDDLs[0].tableNames[0][0].String(),
		tableNames: needTrackDDLs[0].tableNames,
		stmt:       needTrackDDLs[0].stmt,
	}
	job := newDDLJob(ddlInfo, needHandleDDLs, *ec.lastLocation, *ec.currentLocation, *ec.traceID)
	err = s.addJobFunc(job)
	if err != nil {
		return err
	}

	if detected, err := s.execError.Detected(); detected {
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
