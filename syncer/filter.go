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
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/parser/ast"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tidb-tools/pkg/filter"

	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
	onlineddl "github.com/pingcap/dm/syncer/online-ddl-tools"
)

func (s *Syncer) filterQueryEvent(tables []*filter.Table, stmt ast.StmtNode, sql string) (bool, error) {
	et := bf.AstToDDLEvent(stmt)
	for _, table := range tables {
		needFilter, err := s.filterOneEvent(table, et, sql)
		if err != nil || needFilter {
			return needFilter, err
		}
	}
	return false, nil
}

func (s *Syncer) filterRowsEvent(table *filter.Table, eventType replication.EventType) (bool, error) {
	// filter ghost table
	if s.onlineDDL != nil {
		tp := s.onlineDDL.TableType(table.Name)
		if tp != onlineddl.RealTable {
			return true, nil
		}
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
	return s.filterOneEvent(table, et, "")
}

// filterSQL filter unsupported sql in tidb and global sql-patterns in binlog-filter config file.
func (s *Syncer) filterSQL(sql string) (bool, error) {
	if utils.IsBuildInSkipDDL(sql) {
		return true, nil
	}
	action, err := s.binlogFilter.Filter("", "", bf.NullEvent, sql)
	if err != nil {
		return false, terror.Annotatef(terror.ErrSyncerUnitBinlogEventFilter.New(err.Error()), "skip query %s", sql)
	}
	return action == bf.Ignore, nil
}

// filterOneEvent will return true when
// - given `sql` matches builtin pattern.
// - any schema of table names is system schema.
// - any table name doesn't pass block-allow list.
// - type of SQL doesn't pass binlog filter.
// - pattern of SQL doesn't pass binlog filter.
func (s *Syncer) filterOneEvent(table *filter.Table, et bf.EventType, sql string) (bool, error) {
	if utils.IsBuildInSkipDDL(sql) {
		return true, nil
	}
	if filter.IsSystemSchema(table.Schema) {
		return true, nil
	}
	tables := s.baList.Apply([]*filter.Table{table})
	if len(tables) == 0 {
		return true, nil
	}
	if s.binlogFilter == nil {
		return false, nil
	}
	action, err := s.binlogFilter.Filter(table.Schema, table.Name, et, sql)
	if err != nil {
		return false, terror.Annotatef(terror.ErrSyncerUnitBinlogEventFilter.New(err.Error()), "skip event %s on `%s`.`%s`", et, table.Schema, table.Name)
	}
	return action == bf.Ignore, nil
}
