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
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/pingcap/dm/syncer/metrics"
)

// handleQueryEventPessimistic handles QueryEvent in the pessimistic shard DDL mode.
func (s *Syncer) handleQueryEventPessimistic(
	usedSchema string, ev *replication.QueryEvent, ec eventContext,
	sqls []string, needHandleDDLs []string, needTrackDDLs []trackedDDL,
	onlineDDLTableNames map[string]*filter.Table, originSQL string, ddlInfo *shardingDDLInfo,
) error {
	// handle sharding ddl
	var (
		needShardingHandle bool
		group              *ShardingGroup
		synced             bool
		active             bool
		remain             int
		source             string
		err                error
	)
	// for sharding DDL, the firstPos should be the `Pos` of the binlog, not the `End_log_pos`
	// so when restarting before sharding DDLs synced, this binlog can be re-sync again to trigger the TrySync
	startLocation := ec.startLocation

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
		needShardingHandle, group, synced, active, remain, err = s.sgk.TrySync(ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name, source, *startLocation, *ec.currentLocation, needHandleDDLs)
		if err != nil {
			return err
		}
		annotate = "try to sync table in shard group"
		// meets DDL that will not be processed in sequence sharding
		if !active {
			ec.tctx.L().Info("skip in-activeDDL", zap.String("event", "query"), zap.String("source", source), zap.Strings("ddls", needHandleDDLs), zap.ByteString("raw statement", ev.Query), zap.Bool("in-sharding", needShardingHandle), zap.Stringer("start location", startLocation), zap.Bool("is-synced", synced), zap.Int("unsynced", remain))
			return nil
		}
	}

	ec.tctx.L().Info(annotate, zap.String("event", "query"), zap.String("source", source), zap.Strings("ddls", needHandleDDLs), zap.ByteString("raw statement", ev.Query), zap.Bool("in-sharding", needShardingHandle), zap.Stringer("start location", startLocation), zap.Bool("is-synced", synced), zap.Int("unsynced", remain))

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
		metrics.UnsyncedTableGauge.WithLabelValues(s.cfg.Name, target, s.cfg.SourceID).Set(float64(remain))
		err = ec.safeMode.IncrForTable(ec.tctx, ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name) // try enable safe-mode when starting syncing for sharding group
		if err != nil {
			return err
		}

		// save checkpoint in memory, don't worry, if error occurred, we can rollback it
		// for non-last sharding DDL's table, this checkpoint will be used to skip binlog event when re-syncing
		// NOTE: when last sharding DDL executed, all this checkpoints will be flushed in the same txn
		ec.tctx.L().Info("save table checkpoint for source", zap.String("event", "query"), zap.String("source", source), zap.Stringer("start location", startLocation), log.WrapStringerField("end location", ec.currentLocation))
		s.saveTablePoint(ddlInfo.tableNames[0][0].Schema, ddlInfo.tableNames[0][0].Name, *ec.currentLocation)
		if !synced {
			ec.tctx.L().Info("source shard group is not synced", zap.String("event", "query"), zap.String("source", source), zap.Stringer("start location", startLocation), log.WrapStringerField("end location", ec.currentLocation))
			return nil
		}

		ec.tctx.L().Info("source shard group is synced", zap.String("event", "query"), zap.String("source", source), zap.Stringer("start location", startLocation), log.WrapStringerField("end location", ec.currentLocation))
		err = ec.safeMode.DescForTable(ec.tctx, ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name) // try disable safe-mode after sharding group synced
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
			currLocation:   *firstEndLocation,
			latestLocation: *ec.currentLocation,
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
		metrics.ShardLockResolving.WithLabelValues(s.cfg.Name, s.cfg.SourceID).Set(1) // block and wait DDL lock to be synced
		ec.tctx.L().Info("putted shard DDL info", zap.Stringer("info", shardInfo), zap.Int64("revision", rev))

		shardOp, err2 := s.pessimist.GetOperation(ec.tctx.Ctx, shardInfo, rev+1)
		metrics.ShardLockResolving.WithLabelValues(s.cfg.Name, s.cfg.SourceID).Set(0)
		if err2 != nil {
			return err2
		}

		if shardOp.Exec {
			failpoint.Inject("ShardSyncedExecutionExit", func() {
				ec.tctx.L().Warn("exit triggered", zap.String("failpoint", "ShardSyncedExecutionExit"))
				//nolint:errcheck
				s.flushCheckPoints()
				utils.OsExit(1)
			})
			failpoint.Inject("SequenceShardSyncedExecutionExit", func() {
				group := s.sgk.Group(ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name)
				if group != nil {
					// exit in the first round sequence sharding DDL only
					if group.meta.ActiveIdx() == 1 {
						ec.tctx.L().Warn("exit triggered", zap.String("failpoint", "SequenceShardSyncedExecutionExit"))
						//nolint:errcheck
						s.flushCheckPoints()
						utils.OsExit(1)
					}
				}
			})

			ec.tctx.L().Info("execute DDL job", zap.String("event", "query"), zap.String("source", source), zap.ByteString("raw statement", ev.Query), zap.Stringer("start location", startLocation), log.WrapStringerField("end location", ec.currentLocation), zap.Stringer("operation", shardOp))
		} else {
			ec.tctx.L().Info("ignore DDL job", zap.String("event", "query"), zap.String("source", source), zap.ByteString("raw statement", ev.Query), zap.Stringer("start location", startLocation), log.WrapStringerField("end location", ec.currentLocation), zap.Stringer("operation", shardOp))
		}
	}

	ec.tctx.L().Info("start to handle ddls in shard mode", zap.String("event", "query"), zap.Strings("ddls", needHandleDDLs), zap.ByteString("raw statement", ev.Query), zap.Stringer("start location", startLocation), log.WrapStringerField("end location", ec.currentLocation))

	// interrupted after track DDL and before execute DDL.
	failpoint.Inject("FlushCheckpointStage", func(val failpoint.Value) {
		err = handleFlushCheckpointStage(2, val.(int), "before execute DDL")
		if err != nil {
			failpoint.Return(err)
		}
	})

	job := newDDLJob(ddlInfo, needHandleDDLs, *ec.lastLocation, *ec.startLocation, *ec.currentLocation, nil, originSQL, ec.header)
	err = s.addJobFunc(job)
	if err != nil {
		return err
	}

	err = s.execError.Load()
	if err != nil {
		ec.tctx.L().Error("error detected when executing SQL job", log.ShortError(err))
		// nolint:nilerr
		return nil
	}

	if len(onlineDDLTableNames) > 0 {
		err = s.clearOnlineDDL(ec.tctx, ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name)
		if err != nil {
			return err
		}
	}
	ec.tctx.L().Info("finish to handle ddls in shard mode", zap.String("event", "query"), zap.Strings("ddls", needHandleDDLs), zap.ByteString("raw statement", ev.Query), zap.Stringer("start location", startLocation), log.WrapStringerField("end location", ec.currentLocation))
	return nil
}
