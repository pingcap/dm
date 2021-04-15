// Copyright 2019 PingCAP, Inc.
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
	"time"

	"github.com/siddontang/go-mysql/mysql"
	"go.uber.org/zap"

	"github.com/pingcap/failpoint"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/binlog"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/utils"
)

// Status implements Unit.Status
// it returns status, but does not calc status.
func (s *Syncer) Status(ctx context.Context) interface{} {
	var (
		masterPos     mysql.Position
		masterGTIDSet gtid.Set
	)
	total := s.count.Get()
	totalTps := s.totalTps.Get()
	tps := s.tps.Get()
	masterPos, masterGTIDSet, err := s.getMasterStatus(ctx)
	if err != nil {
		s.tctx.L().Warn("fail to get master status", zap.Error(err))
	}

	syncerLocation := s.checkpoint.FlushedGlobalPoint()
	st := &pb.SyncStatus{
		TotalEvents:  total,
		TotalTps:     totalTps,
		RecentTps:    tps,
		MasterBinlog: masterPos.String(),
		SyncerBinlog: syncerLocation.Position.String(),
	}
	if masterGTIDSet != nil { // masterGTIDSet maybe a nil interface
		st.MasterBinlogGtid = masterGTIDSet.String()
	}

	if syncerLocation.GetGTID() != nil {
		st.SyncerBinlogGtid = syncerLocation.GetGTID().String()
	}

	st.BinlogType = "unknown"
	if s.streamerController != nil {
		st.BinlogType = binlogTypeToString(s.streamerController.GetBinlogType())
	}

	if s.cfg.EnableGTID {
		if masterGTIDSet != nil && syncerLocation.GetGTID() != nil && masterGTIDSet.Equal(syncerLocation.GetGTID()) {
			st.Synced = true
		}
	} else {
		// If a syncer unit is waiting for relay log catch up, it has not executed
		// LoadMeta and will return a parsed binlog name error. As we can find mysql
		// position in syncer status, we record this error only in debug level.
		realPos, err := binlog.RealMySQLPos(syncerLocation.Position)
		if err != nil {
			s.tctx.L().Debug("fail to parse real mysql position", zap.Stringer("position", syncerLocation.Position), log.ShortError(err))
		}
		st.Synced = utils.CompareBinlogPos(masterPos, realPos, 0) == 0
	}

	// only support to show `UnresolvedGroups` in pessimistic mode now.
	if s.cfg.ShardMode == config.ShardPessimistic {
		st.UnresolvedGroups = s.sgk.UnresolvedGroups()
	}

	pendingShardInfo := s.pessimist.PendingInfo()
	if pendingShardInfo != nil {
		st.BlockingDDLs = pendingShardInfo.DDLs
	}

	failpoint.Inject("BlockSyncStatus", func(val failpoint.Value) {
		interval, err := time.ParseDuration(val.(string))
		if err != nil {
			s.tctx.L().Warn("inject failpoint BlockSyncStatus failed", zap.Reflect("value", val), zap.Error(err))
		} else {
			s.tctx.L().Info("set BlockSyncStatus", zap.String("failpoint", "BlockSyncStatus"), zap.Duration("value", interval))
			time.Sleep(interval)
		}
	})
	return st
}
