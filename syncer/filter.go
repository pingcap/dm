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
	"regexp"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/siddontang/go-mysql/replication"
)

/*
CREATE [TEMPORARY] TABLE [IF NOT EXISTS] tbl_name
    { LIKE old_tbl_name | (LIKE old_tbl_name) }
*/
var (
	builtInSkipDDLs = []string{
		// transaction
		"^SAVEPOINT",

		// skip all flush sqls
		"^FLUSH",

		// table maintenance
		"^OPTIMIZE\\s+TABLE",
		"^ANALYZE\\s+TABLE",
		"^REPAIR\\s+TABLE",

		// temporary table
		"^DROP\\s+(\\/\\*\\!40005\\s+)?TEMPORARY\\s+(\\*\\/\\s+)?TABLE",

		// trigger
		"^CREATE\\s+(DEFINER\\s?=.+?)?TRIGGER",
		"^DROP\\s+TRIGGER",

		// procedure
		"^DROP\\s+PROCEDURE",
		"^CREATE\\s+(DEFINER\\s?=.+?)?PROCEDURE",
		"^ALTER\\s+PROCEDURE",

		// view
		"^CREATE\\s*(OR REPLACE)?\\s+(ALGORITHM\\s?=.+?)?(DEFINER\\s?=.+?)?\\s+(SQL SECURITY DEFINER)?VIEW",
		"^DROP\\s+VIEW",
		"^ALTER\\s+(ALGORITHM\\s?=.+?)?(DEFINER\\s?=.+?)?(SQL SECURITY DEFINER)?VIEW",

		// function
		// user-defined function
		"^CREATE\\s+(AGGREGATE)?\\s*?FUNCTION",
		// stored function
		"^CREATE\\s+(DEFINER\\s?=.+?)?FUNCTION",
		"^ALTER\\s+FUNCTION",
		"^DROP\\s+FUNCTION",

		// tableSpace
		"^CREATE\\s+TABLESPACE",
		"^ALTER\\s+TABLESPACE",
		"^DROP\\s+TABLESPACE",

		// account management
		"^GRANT",
		"^REVOKE",
		"^CREATE\\s+USER",
		"^ALTER\\s+USER",
		"^RENAME\\s+USER",
		"^DROP\\s+USER",
		"^SET\\s+PASSWORD",

		// alter database
		"^ALTER DATABASE",
	}
)

var (
	builtInSkipDDLPatterns *regexp.Regexp
)

func init() {
	builtInSkipDDLPatterns = regexp.MustCompile("(?i)" + strings.Join(builtInSkipDDLs, "|"))
}

func (s *Syncer) skipQuery(tables []*filter.Table, stmt ast.StmtNode, sql string) (bool, error) {
	if builtInSkipDDLPatterns.FindStringIndex(sql) != nil {
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
			return false, errors.Annotatef(err, "skip query %s", sql)
		}

		if action == bf.Ignore {
			return true, nil
		}
	}

	for _, table := range tables {
		action, err := s.binlogFilter.Filter(table.Schema, table.Name, et, sql)
		if err != nil {
			return false, errors.Annotatef(err, "skip query %s on `%s`.`%s`", sql, table.Schema, table.Name)
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
		return false, errors.Errorf("[syncer] invalid replication event type %v", eventType)
	}

	action, err := s.binlogFilter.Filter(schema, table, et, "")
	if err != nil {
		return false, errors.Annotatef(err, "skip row event %s on `%s`.`%s`", eventType, schema, table)
	}

	return action == bf.Ignore, nil
}
