// Copyright 2018 PingCAP, Inc.
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
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-enterprise-tools/dm/pb"
	"github.com/pingcap/tidb-enterprise-tools/pkg/gtid"
	"github.com/siddontang/go-mysql/mysql"
)

// Status implements SubTaskUnit.Status
// it returns status, but does not calc status
func (s *Syncer) Status() interface{} {
	var (
		masterPos     mysql.Position
		masterGTIDSet gtid.Set
	)
	total := s.count.Get()
	totalTps := s.totalTps.Get()
	tps := s.totalTps.Get()
	masterPos, masterGTIDSet, err := s.getMasterStatus()
	if err != nil {
		log.Warnf("[syncer] get master status err %v", errors.ErrorStack(err))
	}

	syncerPos := s.checkpoint.GlobalPoint()
	if err != nil {
		log.Warnf("[syncer] get gtid err %v", errors.ErrorStack(err))
	}
	st := &pb.SyncStatus{
		TotalEvents:  total,
		TotalTps:     totalTps,
		RecentTps:    tps,
		MasterBinlog: masterPos.String(),
		SyncerBinlog: syncerPos.String(),
		BlockingDDLs: s.ddlExecInfo.BlockingDDLs(),
	}
	if masterGTIDSet != nil { // masterGTIDSet maybe a nil interface
		st.MasterBinlogGtid = masterGTIDSet.String()
	}

	if s.cfg.IsSharding {
		st.UnresolvedGroups = s.sgk.UnresolvedGroups()
	}
	return st
}
