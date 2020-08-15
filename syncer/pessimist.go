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
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/siddontang/go-mysql/replication"
	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

// initShardingGroups initializes sharding groups according to source MySQL, filter rules and router rules
// NOTE: now we don't support modify router rules after task has started
func (s *Syncer) initShardingGroups() error {
	// fetch tables from source and filter them
	sourceTables, err := s.fromDB.fetchAllDoTables(s.baList)
	if err != nil {
		return err
	}

	// convert according to router rules
	// target-schema -> target-table -> source-IDs
	mapper := make(map[string]map[string][]string, len(sourceTables))
	for schema, tables := range sourceTables {
		for _, table := range tables {
			targetSchema, targetTable := s.renameShardingSchema(schema, table)
			mSchema, ok := mapper[targetSchema]
			if !ok {
				mapper[targetSchema] = make(map[string][]string, len(tables))
				mSchema = mapper[targetSchema]
			}
			_, ok = mSchema[targetTable]
			if !ok {
				mSchema[targetTable] = make([]string, 0, len(tables))
			}
			ID, _ := GenTableID(schema, table)
			mSchema[targetTable] = append(mSchema[targetTable], ID)
		}
	}

	loadMeta, err2 := s.sgk.LoadShardMeta(s.cfg.Flavor, s.cfg.EnableGTID)
	if err2 != nil {
		return err2
	}

	// add sharding group
	for targetSchema, mSchema := range mapper {
		for targetTable, sourceIDs := range mSchema {
			tableID, _ := GenTableID(targetSchema, targetTable)
			_, _, _, _, err := s.sgk.AddGroup(targetSchema, targetTable, sourceIDs, loadMeta[tableID], false)
			if err != nil {
				return err
			}
		}
	}

	shardGroup := s.sgk.Groups()
	s.tctx.L().Debug("initial sharding groups", zap.Int("shard group length", len(shardGroup)), zap.Reflect("shard group", shardGroup))

	return nil
}

// handleQueryEventPessimistic handles QueryEvent in the optimistic shard DDL mode.
func (s *Syncer) handleQueryEventPessimistic(
	ev *replication.QueryEvent, ec eventContext, needHandleDDLs []string, needTrackDDLs []trackedDDL,
	onlineDDLTableNames map[string]*filter.Table, ddlInfo *shardingDDLInfo, sqls []string) error {

	var (
		needShardingHandle bool
		group              *ShardingGroup
		synced             bool
		active             bool
		remain             int
		source             string
		err                error
	)
	usedSchema := string(ev.Schema)

	// for sharding DDL, the firstPos should be the `Pos` of the binlog, not the `End_log_pos`
	// so when restarting before sharding DDLs synced, this binlog can be re-sync again to trigger the TrySync
	startLocation := ec.startLocation.Clone()

	source, _ = GenTableID(ddlInfo.tableNames[0][0].Schema, ddlInfo.tableNames[0][0].Name)

	var annotate string
	switch ddlInfo.stmt.(type) {
	case *ast.CreateDatabaseStmt:
		// for CREATE DATABASE, we do nothing. when CREATE TABLE under this DATABASE, sharding groups will be added
	case *ast.CreateTableStmt:
		// for CREATE TABLE, we add it to group
		needShardingHandle, group, synced, remain, err = s.sgk.AddGroup(ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name, []string{source}, nil, true)
		if err != nil {
			return err
		}
		annotate = "add table to shard group"
	default:
		needShardingHandle, group, synced, active, remain, err = s.sgk.TrySync(ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name, source, startLocation.Clone(), ec.nextLocation.Clone(), needHandleDDLs)
		if err != nil {
			return err
		}
		annotate = "try to sync table in shard group"
		// meets DDL that will not be processed in sequence sharding
		if !active {
			s.tctx.L().Info("skip in-activeDDL", zap.String("event", "query"), zap.String("source", source), zap.Strings("ddls", needHandleDDLs), zap.ByteString("raw statement", ev.Query), zap.Bool("in-sharding", needShardingHandle), zap.Stringer("start location", startLocation), zap.Bool("is-synced", synced), zap.Int("unsynced", remain))
			return nil
		}
	}

	s.tctx.L().Info(annotate, zap.String("event", "query"), zap.String("source", source), zap.Strings("ddls", needHandleDDLs), zap.ByteString("raw statement", ev.Query), zap.Bool("in-sharding", needShardingHandle), zap.Stringer("start location", startLocation), zap.Bool("is-synced", synced), zap.Int("unsynced", remain))

	// interrupted after flush old checkpoint and before track DDL.
	failpoint.Inject("FlushCheckpointStage", func(val failpoint.Value) {
		err = handleFlushCheckpointStage(1, val.(int), "before track DDL")
		if err != nil {
			failpoint.Return(err)
		}
	})

	for _, td := range needTrackDDLs {
		if err = s.trackDDL(usedSchema, td.rawSQL, td.tableNames, td.stmt, &ec); err != nil {
			return err
		}
	}

	if needShardingHandle {
		target, _ := GenTableID(ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name)
		unsyncedTableGauge.WithLabelValues(s.cfg.Name, target, s.cfg.SourceID).Set(float64(remain))
		err = ec.safeMode.IncrForTable(s.tctx, ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name) // try enable safe-mode when starting syncing for sharding group
		if err != nil {
			return err
		}

		// save checkpoint in memory, don't worry, if error occurred, we can rollback it
		// for non-last sharding DDL's table, this checkpoint will be used to skip binlog event when re-syncing
		// NOTE: when last sharding DDL executed, all this checkpoints will be flushed in the same txn
		s.tctx.L().Info("save table checkpoint for source", zap.String("event", "query"), zap.String("source", source), zap.Stringer("start location", startLocation), log.WrapStringerField("end location", ec.nextLocation))
		s.saveTablePoint(ddlInfo.tableNames[0][0].Schema, ddlInfo.tableNames[0][0].Name, ec.nextLocation.Clone())
		if !synced {
			s.tctx.L().Info("source shard group is not synced", zap.String("event", "query"), zap.String("source", source), zap.Stringer("start location", startLocation), log.WrapStringerField("end location", ec.nextLocation))
			return nil
		}

		s.tctx.L().Info("source shard group is synced", zap.String("event", "query"), zap.String("source", source), zap.Stringer("start location", startLocation), log.WrapStringerField("end location", ec.nextLocation))
		err = ec.safeMode.DescForTable(s.tctx, ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name) // try disable safe-mode after sharding group synced
		if err != nil {
			return err
		}
		// maybe multi-groups' sharding DDL synced in this for-loop (one query-event, multi tables)
		if cap(*ec.shardingReSyncCh) < len(sqls) {
			*ec.shardingReSyncCh = make(chan *ShardingReSync, len(sqls))
		}
		firstEndLocation := group.FirstEndPosUnresolved()
		if firstEndLocation == nil {
			return terror.ErrSyncerUnitFirstEndPosNotFound.Generate(source)
		}

		allResolved, err2 := s.sgk.ResolveShardingDDL(ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name)
		if err2 != nil {
			return err2
		}
		*ec.shardingReSyncCh <- &ShardingReSync{
			nextLocation:   firstEndLocation.Clone(),
			latestLocation: ec.nextLocation.Clone(),
			targetSchema:   ddlInfo.tableNames[1][0].Schema,
			targetTable:    ddlInfo.tableNames[1][0].Name,
			allResolved:    allResolved,
		}

		// Don't send new DDLInfo to dm-master until all local sql jobs finished
		// since jobWg is flushed by flushJobs before, we don't wait here any more

		// NOTE: if we need singleton Syncer (without dm-master) to support sharding DDL sync
		// we should add another config item to differ, and do not save DDLInfo, and not wait for ddlExecInfo

		// construct & send shard DDL info into etcd, DM-master will handle it.
		shardInfo := s.pessimist.ConstructInfo(ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name, needHandleDDLs)
		rev, err2 := s.pessimist.PutInfo(ec.tctx.Ctx, shardInfo)
		if err2 != nil {
			return err2
		}
		shardLockResolving.WithLabelValues(s.cfg.Name, s.cfg.SourceID).Set(1) // block and wait DDL lock to be synced
		s.tctx.L().Info("putted shard DDL info", zap.Stringer("info", shardInfo), zap.Int64("revision", rev))

		shardOp, err2 := s.pessimist.GetOperation(ec.tctx.Ctx, shardInfo, rev+1)
		shardLockResolving.WithLabelValues(s.cfg.Name, s.cfg.SourceID).Set(0)
		if err2 != nil {
			return err2
		}

		if shardOp.Exec {
			failpoint.Inject("ShardSyncedExecutionExit", func() {
				s.tctx.L().Warn("exit triggered", zap.String("failpoint", "ShardSyncedExecutionExit"))
				s.flushCheckPoints()
				utils.OsExit(1)
			})
			failpoint.Inject("SequenceShardSyncedExecutionExit", func() {
				group := s.sgk.Group(ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name)
				if group != nil {
					// exit in the first round sequence sharding DDL only
					if group.meta.ActiveIdx() == 1 {
						s.tctx.L().Warn("exit triggered", zap.String("failpoint", "SequenceShardSyncedExecutionExit"))
						s.flushCheckPoints()
						utils.OsExit(1)
					}
				}
			})

			s.tctx.L().Info("execute DDL job", zap.String("event", "query"), zap.String("source", source), zap.ByteString("raw statement", ev.Query), zap.Stringer("start location", startLocation), log.WrapStringerField("end location", ec.nextLocation), zap.Stringer("operation", shardOp))
		} else {
			s.tctx.L().Info("ignore DDL job", zap.String("event", "query"), zap.String("source", source), zap.ByteString("raw statement", ev.Query), zap.Stringer("start location", startLocation), log.WrapStringerField("end location", ec.nextLocation), zap.Stringer("operation", shardOp))
		}
	}

	s.tctx.L().Info("start to handle ddls in shard mode", zap.String("event", "query"), zap.Strings("ddls", needHandleDDLs), zap.ByteString("raw statement", ev.Query), zap.Stringer("start location", startLocation), log.WrapStringerField("end location", ec.nextLocation))

	// interrupted after track DDL and before execute DDL.
	failpoint.Inject("FlushCheckpointStage", func(val failpoint.Value) {
		err = handleFlushCheckpointStage(2, val.(int), "before execute DDL")
		if err != nil {
			failpoint.Return(err)
		}
	})

	job := newDDLJob(ddlInfo, needHandleDDLs, *ec.lastLocation, *ec.startLocation, *ec.nextLocation, *ec.traceID, nil)
	err = s.addJobFunc(job)
	if err != nil {
		return err
	}

	err = s.execError.Get()
	if err != nil {
		return terror.ErrSyncerUnitHandleDDLFailed.Delegate(err, ev.Query)
	}

	if len(onlineDDLTableNames) > 0 {
		err = s.clearOnlineDDL(ec.tctx, ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name)
		if err != nil {
			return err
		}
	}

	s.tctx.L().Info("finish to handle ddls in shard mode", zap.String("event", "query"), zap.Strings("ddls", needHandleDDLs), zap.ByteString("raw statement", ev.Query), zap.Stringer("start location", startLocation), log.WrapStringerField("end location", ec.nextLocation))
	return nil
}
