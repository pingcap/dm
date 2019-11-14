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
	"github.com/pingcap/parser/ast"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/siddontang/go-mysql/replication"

	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

func (s *Syncer) skipQuery(tables []*filter.Table, stmt ast.StmtNode, sql string) (bool, error) {
	if utils.IsBuildInSkipDDL(sql) {
		return true, nil
	}

	for _, table := range tables {
		if filter.IsSystemSchema(table.Schema) {
			return true, nil
		}
	}

	if len(tables) > 0 {
		tbs := s.bwList.ApplyOn(tables)
		if len(tbs) != len(tables) {
			return true, nil
		}
	}

	if s.binlogFilter == nil {
		return false, nil
	}

	et := bf.NullEvent
	if stmt != nil {
		et = bf.AstToDDLEvent(stmt)
	}
	if len(tables) == 0 {
		action, err := s.binlogFilter.Filter("", "", et, sql)
		if err != nil {
			return false, terror.Annotatef(terror.ErrSyncerUnitBinlogEventFilter.New(err.Error()), "skip query %s", sql)
		}

		if action == bf.Ignore {
			return true, nil
		}
	}

	for _, table := range tables {
		action, err := s.binlogFilter.Filter(table.Schema, table.Name, et, sql)
		if err != nil {
			return false, terror.Annotatef(terror.ErrSyncerUnitBinlogEventFilter.New(err.Error()), "skip query %s on `%s`.`%s`", sql, table.Schema, table.Name)
		}

		if action == bf.Ignore {
			return true, nil
		}
	}

	return false, nil
}

func (s *Syncer) skipDMLEvent(schema string, table string, eventType replication.EventType) (bool, error) {
	if filter.IsSystemSchema(schema) {
		return true, nil
	}

	tbs := []*filter.Table{{Schema: schema, Name: table}}
	tbs = s.bwList.ApplyOn(tbs)
	if len(tbs) == 0 {
		return true, nil
	}
	// filter ghost table
	if s.onlineDDL != nil {
		tp := s.onlineDDL.TableType(table)
		if tp != realTable {
			return true, nil
		}
	}

	if s.binlogFilter == nil {
		return false, nil
	}

	var et bf.EventType
	switch eventType {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		et = bf.InsertEvent
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		et = bf.UpdateEvent
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		et = bf.DeleteEvent
	default:
		return false, terror.ErrSyncerUnitInvalidReplicaEvent.Generate(eventType)
	}

	action, err := s.binlogFilter.Filter(schema, table, et, "")
	if err != nil {
		return false, terror.Annotatef(terror.ErrSyncerUnitBinlogEventFilter.New(err.Error()), "skip row event %s on `%s`.`%s`", eventType, schema, table)
	}

	return action == bf.Ignore, nil
}
