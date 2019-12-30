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
	"path/filepath"
	"strings"

	"github.com/siddontang/go-mysql/mysql"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/binlog"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

func (s *Syncer) setInitActiveRelayLog() error {
	if s.binlogType != LocalBinlog {
		return nil
	}

	var (
		pos        mysql.Position
		activeUUID string
		extractPos bool
		err        error
	)

	indexPath := filepath.Join(s.cfg.RelayDir, utils.UUIDIndexFilename)
	uuids, err := utils.ParseUUIDIndex(indexPath)
	if err != nil {
		return terror.Annotatef(err, "UUID index file path %s", indexPath)
	}
	if len(uuids) == 0 {
		return terror.ErrRelayNoValidRelaySubDir.Generate()
	}

	checkPos := s.checkpoint.GlobalPoint()
	if binlog.ComparePosition(checkPos, minCheckpoint) > 0 {
		// continue from previous checkpoint
		pos = checkPos
		extractPos = true
	} else if s.cfg.Mode == config.ModeIncrement {
		// fresh start for task-mode increment
		pos = mysql.Position{
			Name: s.cfg.Meta.BinLogName,
			Pos:  s.cfg.Meta.BinLogPos,
		}
	} else {
		// start from dumper or loader, get current pos from master
		pos, _, err = s.fromDB.getMasterStatus(s.cfg.Flavor)
		if err != nil {
			return terror.Annotatef(err, "get master status")
		}
	}

	if extractPos {
		activeUUID, _, pos, err = binlog.ExtractPos(pos, uuids)
		if err != nil {
			return err
		}
	} else {
		var uuid string
		latestUUID := uuids[len(uuids)-1]
		uuid, err = s.fromDB.getServerUUID(s.cfg.Flavor)
		if err != nil {
			return terror.WithScope(terror.Annotatef(err, "get server UUID"), terror.ScopeUpstream)
		}
		// latest should be the current
		if !strings.HasPrefix(latestUUID, uuid) {
			return terror.ErrSyncerUnitUUIDNotLatest.Generate(uuid, uuids)
		}
		activeUUID = latestUUID
	}

	err = s.readerHub.UpdateActiveRelayLog(s.cfg.Name, activeUUID, pos.Name)
	s.tctx.L().Info("current earliest active relay log", log.WrapStringerField("active relay log", s.readerHub.EarliestActiveRelayLog()))
	return err
}

func (s *Syncer) updateActiveRelayLog(pos mysql.Position) error {
	if s.binlogType != LocalBinlog {
		return nil
	}

	indexPath := filepath.Join(s.cfg.RelayDir, utils.UUIDIndexFilename)
	uuids, err := utils.ParseUUIDIndex(indexPath)
	if err != nil {
		return terror.Annotatef(err, "UUID index file path %s", indexPath)
	}
	if len(uuids) == 0 {
		return terror.ErrRelayNoValidRelaySubDir.Generate()
	}

	activeUUID, _, pos, err := binlog.ExtractPos(pos, uuids)
	if err != nil {
		return err
	}

	err = s.readerHub.UpdateActiveRelayLog(s.cfg.Name, activeUUID, pos.Name)
	s.tctx.L().Info("current earliest active relay log", log.WrapStringerField("active relay log", s.readerHub.EarliestActiveRelayLog()))
	return err
}

func (s *Syncer) removeActiveRelayLog() {
	if s.binlogType != LocalBinlog {
		return
	}

	s.readerHub.RemoveActiveRelayLog(s.cfg.Name)
	s.tctx.L().Info("current earliest active relay log", log.WrapStringerField("active relay log", s.readerHub.EarliestActiveRelayLog()))
}
