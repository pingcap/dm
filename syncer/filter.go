// Copyright 2017 PingCAP, Inc.
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

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-enterprise-tools/pkg/filter"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tidb/ast"
	"github.com/siddontang/go-mysql/replication"
)

/*
CREATE [TEMPORARY] TABLE [IF NOT EXISTS] tbl_name
    { LIKE old_tbl_name | (LIKE old_tbl_name) }
*/
var (
	// https://dev.mysql.com/doc/refman/5.7/en/create-database.html
	createDatabaseRegex = regexp.MustCompile("(?i)CREATE\\s+(DATABASE|SCHEMA)\\s+(IF NOT EXISTS\\s+)?\\S+")
	// https://dev.mysql.com/doc/refman/5.7/en/drop-database.html
	dropDatabaseRegex = regexp.MustCompile("(?i)DROP\\s+(DATABASE|SCHEMA)\\s+(IF EXISTS\\s+)?\\S+")
	// https://dev.mysql.com/doc/refman/5.7/en/create-index.html
	// https://dev.mysql.com/doc/refman/5.7/en/drop-index.html
	createIndexDDLRegex = regexp.MustCompile("(?i)ON\\s+\\S+\\s*\\(")
	dropIndexDDLRegex   = regexp.MustCompile("(?i)ON\\s+\\S+")
	// https://dev.mysql.com/doc/refman/5.7/en/create-table.html
	createTableRegex     = regexp.MustCompile("(?i)^CREATE\\s+(TEMPORARY\\s+)?TABLE\\s+(IF NOT EXISTS\\s+)?\\S+")
	createTableLikeRegex = regexp.MustCompile("(?i)^CREATE\\s+(TEMPORARY\\s+)?TABLE\\s+(IF NOT EXISTS\\s+)?\\S+\\s*\\(?\\s*LIKE\\s+\\S+")
	// https://dev.mysql.com/doc/refman/5.7/en/drop-table.html
	dropTableRegex = regexp.MustCompile("^(?i)DROP\\s+(TEMPORARY\\s+)?TABLE\\s+(IF EXISTS\\s+)?\\S+")
	// https://dev.mysql.com/doc/refman/5.7/en/alter-table.html
	alterTableRegex = regexp.MustCompile("^(?i)ALTER\\s+TABLE\\s+\\S+")
	// https://dev.mysql.com/doc/refman/5.7/en/create-trigger.html
	builtInSkipDDLs = []string{
		// For mariadb, for query event, like `# Dumm`
		// But i don't know what is the meaning of this event.
		"^#",

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

func (s *Syncer) skipQuery(tables []*filter.Table, sql string) (bool, error) {
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
		if len(tbs) == 0 {
			return true, nil
		}
	}

	if s.binlogFilter == nil {
		return false, nil
	}

	if len(tables) == 0 {
		action, err := s.binlogFilter.Filter("", "", bf.NullEvent, bf.NullEvent, sql)
		if err != nil {
			return false, errors.Annotatef(err, "skip query %s", sql)
		}

		if action == bf.Ignore {
			return true, nil
		}
	}

	for _, table := range tables {
		action, err := s.binlogFilter.Filter(table.Schema, table.Name, bf.NullEvent, bf.NullEvent, sql)
		if err != nil {
			return false, errors.Annotatef(err, "skip query %s on `%s`.`%s`", sql, table.Schema, table.Name)
		}

		if action == bf.Ignore {
			return true, nil
		}
	}

	return false, nil
}

func (s *Syncer) skipDDLEvent(tables []*filter.Table, stmt ast.StmtNode) (bool, error) {
	for _, table := range tables {
		if filter.IsSystemSchema(table.Schema) {
			return true, nil
		}
	}

	if len(tables) > 0 {
		tbs := s.bwList.ApplyOn(tables)
		if len(tbs) == 0 {
			return true, nil
		}
	}
	if s.binlogFilter == nil {
		return false, nil
	}

	et := bf.AstToDDLEvent(stmt)
	if len(tables) == 0 {
		action, err := s.binlogFilter.Filter("", "", bf.NullEvent, et, "")
		if err != nil {
			return false, errors.Annotatef(err, "skip query event %s", et)
		}

		if action == bf.Ignore {
			return true, nil
		}
	}

	for _, table := range tables {
		action, err := s.binlogFilter.Filter(table.Schema, table.Name, bf.NullEvent, et, "")
		if err != nil {
			return false, errors.Annotatef(err, "skip query event %s on `%s`.`%s`", et, table.Schema, table.Name)
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

	schema = strings.ToLower(schema)
	table = strings.ToLower(table)
	tbs := []*filter.Table{{schema, table}}
	tbs = s.bwList.ApplyOn(tbs)
	if len(tbs) == 0 {
		return true, nil
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

	action, err := s.binlogFilter.Filter(schema, table, et, bf.NullEvent, "")
	if err != nil {
		return false, errors.Annotatef(err, "skip row event %s on `%s`.`%s`", eventType, schema, table)
	}

	return action == bf.Ignore, nil
}

// fetchAllDoTables returns all need to do tables after filtered (fetches from upstream MySQL)
func (s *Syncer) fetchAllDoTables() (map[string][]string, error) {
	schemas, err := getSchemas(s.fromDB, maxRetryCount)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// TODO zxc: replace when new filter is merged
	ftSchemas := make([]*filter.Table, 0, len(schemas))
	for _, schema := range schemas {
		if filter.IsSystemSchema(schema) {
			continue
		}
		ftSchemas = append(ftSchemas, &filter.Table{
			Schema: strings.ToLower(schema),
			Name:   "", // schema level
		})
	}
	ftSchemas = s.bwList.ApplyOn(ftSchemas)
	if len(ftSchemas) == 0 {
		log.Warn("[syncer] no schema need to sync")
		return nil, nil
	}

	schemaToTables := make(map[string][]string)
	for _, ftSchema := range ftSchemas {
		schema := ftSchema.Schema
		tables, err := getTables(s.fromDB, schema, maxRetryCount)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ftTables := make([]*filter.Table, 0, len(tables))
		for _, table := range tables {
			ftTables = append(ftTables, &filter.Table{
				Schema: schema,
				Name:   strings.ToLower(table),
			})
		}
		ftTables = s.bwList.ApplyOn(ftTables)
		if len(ftTables) == 0 {
			log.Infof("[syncer] schema %s no tables need to sync", schema)
			continue // NOTE: should we still keep it as an empty elem?
		}
		tables = tables[:0]
		for _, ftTable := range ftTables {
			tables = append(tables, ftTable.Name)
		}
		schemaToTables[schema] = tables
	}

	return schemaToTables, nil
}
