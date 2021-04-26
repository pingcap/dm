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
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/failpoint"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
)

// Status implements Unit.Status
// it returns status, but does not calc status.
func (s *Syncer) Status() interface{} {
	total := s.count.Load()
	totalTps := s.totalTps.Load()
	tps := s.tps.Load()

	syncerLocation := s.checkpoint.FlushedGlobalPoint()
	st := &pb.SyncStatus{
		TotalEvents:  total,
		TotalTps:     totalTps,
		RecentTps:    tps,
		SyncerBinlog: syncerLocation.Position.String(),
	}

	if syncerLocation.GetGTID() != nil {
		st.SyncerBinlogGtid = syncerLocation.GetGTID().String()
	}

	st.BinlogType = "unknown"
	if s.streamerController != nil {
		st.BinlogType = binlogTypeToString(s.streamerController.GetBinlogType())
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
