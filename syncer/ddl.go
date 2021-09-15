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

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"go.uber.org/zap"

	tcontext "github.com/pingcap/dm/pkg/context"
	parserpkg "github.com/pingcap/dm/pkg/parser"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/pingcap/dm/syncer/metrics"
)

// parseDDLResult represents the result of parseDDLSQL.
type parseDDLResult struct {
	stmt   ast.StmtNode
	ignore bool
	isDDL  bool
}

func (s *Syncer) parseDDLSQL(sql string, p *parser.Parser, schema string) (result parseDDLResult, err error) {
	// check skip before parse (used to skip some un-supported DDLs)
	ignore, err := s.skipQuery(nil, nil, sql)
	if err != nil {
		return parseDDLResult{
			stmt:   nil,
			ignore: false,
			isDDL:  false,
		}, err
	} else if ignore {
		return parseDDLResult{
			stmt:   nil,
			ignore: true,
			isDDL:  false,
		}, nil
	}

	// We use Parse not ParseOneStmt here, because sometimes we got a commented out ddl which can't be parsed
	// by ParseOneStmt(it's a limitation of tidb parser.)
	stmts, err := parserpkg.Parse(p, sql, "", "")
	if err != nil {
		// log error rather than fatal, so other defer can be executed
		s.tctx.L().Error("parse ddl", zap.String("sql", sql))
		return parseDDLResult{
			stmt:   nil,
			ignore: false,
			isDDL:  false,
		}, terror.ErrSyncerParseDDL.Delegate(err, sql)
	}

	if len(stmts) == 0 {
		return parseDDLResult{
			stmt:   nil,
			ignore: false,
			isDDL:  false,
		}, nil
	}

	stmt := stmts[0]
	switch node := stmt.(type) {
	case ast.DDLNode:
		return parseDDLResult{
			stmt:   stmt,
			ignore: false,
			isDDL:  true,
		}, nil
	case ast.DMLNode:
		// if DML can be ignored, we do not report an error
		table, err2 := getTableByDML(node)
		if err2 == nil {
			if len(table.Schema) == 0 {
				table.Schema = schema
			}
			ignore, err2 := s.skipDMLEvent(table, replication.QUERY_EVENT)
			if err2 == nil && ignore {
				return parseDDLResult{
					stmt:   nil,
					ignore: true,
					isDDL:  false,
				}, nil
			}
		}
		return parseDDLResult{
			stmt:   nil,
			ignore: false,
			isDDL:  false,
		}, terror.Annotatef(terror.ErrSyncUnitDMLStatementFound.Generate(), "query %s", sql)
	default:
		// BEGIN statement is included here.
		// let sqls be empty
		return parseDDLResult{
			stmt:   nil,
			ignore: false,
			isDDL:  false,
		}, nil
	}
}

// splitAndFilterDDL will split multi-schema change DDL into multiple one schema change DDL due to TiDB's limitation.
// the returned DDLs is split from `stmt`, or DDLs that online DDL component has saved before (which means the input
// `stmt` is a RENAME statement which often generated by online DDL tools).
// return @spilt sqls, @online ddl table names, @error.
func (s *Syncer) splitAndFilterDDL(
	ec eventContext,
	p *parser.Parser,
	stmt ast.StmtNode,
	schema string,
) (sqls []string, tableMap map[string]*filter.Table, err error) {
	sqls, err = parserpkg.SplitDDL(stmt, schema)
	if err != nil {
		return nil, nil, err
	}

	statements := make([]string, 0, len(sqls))
	tableMap = make(map[string]*filter.Table)
	for _, sql := range sqls {
		stmt2, err2 := p.ParseOneStmt(sql, "", "")
		if err2 != nil {
			return nil, nil, terror.Annotatef(terror.ErrSyncerUnitParseStmt.New(err2.Error()), "ddl %s", sql)
		}

		tables, err2 := parserpkg.FetchDDLTables(schema, stmt2, s.SourceTableNamesFlavor)
		if err2 != nil {
			return nil, nil, err2
		}

		// get real tableNames before apply block-allow list
		if s.onlineDDL != nil {
			for _, table := range tables {
				table.Name = s.onlineDDL.RealName(table.Name)
			}
		}

		shouldSkip, err2 := s.skipQuery(tables, stmt2, sql)
		if err2 != nil {
			return nil, nil, err2
		}
		if shouldSkip {
			metrics.SkipBinlogDurationHistogram.WithLabelValues("query", s.cfg.Name, s.cfg.SourceID).Observe(time.Since(ec.startTime).Seconds())
			ec.tctx.L().Warn("skip event", zap.String("event", "query"), zap.String("statement", sql), zap.String("schema", schema))
			continue
		}

		// filter and store ghost table ddl, transform online ddl
		ss, tableName, err2 := s.handleOnlineDDL(ec.tctx, p, schema, sql, stmt2)
		if err2 != nil {
			return nil, nil, err2
		}

		if tableName != nil {
			tableMap[tableName.String()] = tableName
		}

		statements = append(statements, ss...)
	}
	return statements, tableMap, nil
}

// routeDDL will rename tables in DDL.
func (s *Syncer) routeDDL(p *parser.Parser, schema, sql string) (string, [][]*filter.Table, ast.StmtNode, error) {
	stmt, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		return "", nil, nil, terror.Annotatef(terror.ErrSyncerUnitParseStmt.New(err.Error()), "ddl %s", sql)
	}

	sourceTables, err := parserpkg.FetchDDLTables(schema, stmt, s.SourceTableNamesFlavor)
	if err != nil {
		return "", nil, nil, err
	}

	targetTables := make([]*filter.Table, 0, len(sourceTables))
	for i := range sourceTables {
		renamedTable := s.renameShardingSchema(sourceTables[i])
		targetTables = append(targetTables, renamedTable)
	}

	ddl, err := parserpkg.RenameDDLTable(stmt, targetTables)
	return ddl, [][]*filter.Table{sourceTables, targetTables}, stmt, err
}

// handleOnlineDDL checks if the input `sql` is came from online DDL tools.
// If so, it will save actual DDL or return the actual DDL depending on online DDL types of `sql`.
// If not, it returns original SQL and no table names.
func (s *Syncer) handleOnlineDDL(tctx *tcontext.Context, p *parser.Parser, schema, sql string, stmt ast.StmtNode) ([]string, *filter.Table, error) {
	if s.onlineDDL == nil {
		return []string{sql}, nil, nil
	}

	tables, err := parserpkg.FetchDDLTables(schema, stmt, s.SourceTableNamesFlavor)
	if err != nil {
		return nil, nil, err
	}

	sqls, err := s.onlineDDL.Apply(tctx, tables, sql, stmt)
	if err != nil {
		return nil, nil, err
	}

	// skip or origin sqls
	if len(sqls) == 0 || (len(sqls) == 1 && sqls[0] == sql) {
		return sqls, nil, nil
	}

	// remove empty sqls which inserted because online DDL is filtered
	end := 0
	for _, sql2 := range sqls {
		if sql2 != "" {
			sqls[end] = sql2
			end++
		}
	}
	sqls = sqls[:end]

	// tableNames[1:] is the real table name
	targetTables := tables[1:2]
	for i := range sqls {
		stmt, err := p.ParseOneStmt(sqls[i], "", "")
		if err != nil {
			return nil, nil, terror.ErrSyncerUnitParseStmt.New(err.Error())
		}

		sqls[i], err = parserpkg.RenameDDLTable(stmt, targetTables)
		if err != nil {
			return nil, nil, err
		}
	}
	return sqls, tables[0], nil
}

func (s *Syncer) dropSchemaInSharding(tctx *tcontext.Context, sourceSchema string) error {
	sources := make(map[string][]*filter.Table)
	sgs := s.sgk.Groups()
	for name, sg := range sgs {
		if sg.IsSchemaOnly {
			// in sharding group leave handling, we always process schema group,
			// we can ignore schema only group here
			continue
		}
		tables := sg.Tables()
		for _, table := range tables {
			if table.Schema != sourceSchema {
				continue
			}
			sources[name] = append(sources[name], table)
		}
	}
	// delete from sharding group firstly
	for name, tables := range sources {
		targetTable := utils.UnpackTableID(name)
		sourceTableIDs := make([]string, 0, len(tables))
		for _, table := range tables {
			sourceTableIDs = append(sourceTableIDs, utils.GenTableID(table))
		}
		err := s.sgk.LeaveGroup(targetTable, sourceTableIDs)
		if err != nil {
			return err
		}
	}
	// delete from checkpoint
	for _, tables := range sources {
		for _, table := range tables {
			// refine clear them later if failed
			// now it doesn't have problems
			if err1 := s.checkpoint.DeleteTablePoint(tctx, table); err1 != nil {
				s.tctx.L().Error("fail to delete checkpoint", zap.Stringer("table", table))
			}
		}
	}
	return nil
}

func (s *Syncer) clearOnlineDDL(tctx *tcontext.Context, targetTable *filter.Table) error {
	group := s.sgk.Group(targetTable)
	if group == nil {
		return nil
	}

	// return [[schema, table]...]
	tables := group.Tables()

	for _, table := range tables {
		s.tctx.L().Info("finish online ddl", zap.Stringer("table", table))
		err := s.onlineDDL.Finish(tctx, table)
		if err != nil {
			return terror.Annotatef(err, "finish online ddl on %v", table)
		}
	}

	return nil
}

type shardingDDLInfo struct {
	name   string
	tables [][]*filter.Table
	// sourceTables []*filter.Table
	// targetTable  *filter.Table
	stmt ast.StmtNode
}
