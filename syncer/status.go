package syncer

import (
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/siddontang/go-mysql/mysql"

	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/streamer"
	"github.com/pingcap/dm/pkg/utils"
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
	tps := s.tps.Get()
	masterPos, masterGTIDSet, err := s.getMasterStatus()
	if err != nil {
		log.Warnf("[syncer] get master status err %v", errors.ErrorStack(err))
	}

	syncerPos := s.checkpoint.FlushedGlobalPoint()
	if err != nil {
		log.Warnf("[syncer] get gtid err %v", errors.ErrorStack(err))
	}
	st := &pb.SyncStatus{
		TotalEvents:  total,
		TotalTps:     totalTps,
		RecentTps:    tps,
		MasterBinlog: masterPos.String(),
		SyncerBinlog: syncerPos.String(),
	}
	if masterGTIDSet != nil { // masterGTIDSet maybe a nil interface
		st.MasterBinlogGtid = masterGTIDSet.String()
	}

	// If a syncer unit is waiting for relay log catch up, it has not executed
	// LoadMeta and will return a parsed binlog name error. As we can find mysql
	// position in syncer status, we record this error only in debug level.
	realPos, err := streamer.RealMySQLPos(syncerPos)
	if err != nil {
		log.Debugf("[syncer] parse real mysql position err %v", err)
	}
	st.Synced = utils.CompareBinlogPos(masterPos, realPos, 0) == 0

	if s.cfg.IsSharding {
		st.UnresolvedGroups = s.sgk.UnresolvedGroups()
		st.BlockingDDLs = s.ddlExecInfo.BlockingDDLs()
	}
	return st
}
