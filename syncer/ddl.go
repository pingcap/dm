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
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/siddontang/go-mysql/replication"
	"go.uber.org/zap"

	tcontext "github.com/pingcap/dm/pkg/context"
	parserpkg "github.com/pingcap/dm/pkg/parser"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

var (
	// IncompatibleDDLFormat is for incompatible ddl
	IncompatibleDDLFormat = `encountered incompatible DDL in TiDB:
	please confirm your DDL statement is correct and needed.
	for TiDB compatible DDL, please see the docs:
	  English version: https://pingcap.com/docs/dev/reference/mysql-compatibility/#ddl
	  Chinese version: https://pingcap.com/docs-cn/dev/reference/mysql-compatibility/#ddl
	if the DDL is not needed, you can use a filter rule with "*" schema-pattern to ignore it.
	 `
)

// parseDDLResult represents the result of parseDDLSQL
type parseDDLResult struct {
	stmt   ast.StmtNode
	ignore bool
	isDDL  bool
}

func (s *Syncer) parseDDLSQL(sql string, p *parser.Parser, schema string) (result parseDDLResult, err error) {
	sql = utils.TrimCtrlChars(sql)

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
		s.tctx.L().Error(IncompatibleDDLFormat, zap.String("sql", sql))
		return parseDDLResult{
			stmt:   nil,
			ignore: false,
			isDDL:  false,
		}, terror.Annotatef(err, IncompatibleDDLFormat, sql)
	}

	if len(stmts) == 0 {
		return parseDDLResult{
			stmt:   nil,
			ignore: false,
			isDDL:  false,
		}, nil
	}

	stmt := stmts[0]
	switch stmt.(type) {
	case ast.DDLNode:
		return parseDDLResult{
			stmt:   stmt,
			ignore: false,
			isDDL:  true,
		}, nil
	case ast.DMLNode:
		// if DML can be ignored, we do not report an error
		dml := stmt.(ast.DMLNode)
		schema2, table, err2 := tableNameForDML(dml)
		if err2 == nil {
			if len(schema2) > 0 {
				schema = schema2
			}
			ignore, err2 := s.skipDMLEvent(schema, table, replication.QUERY_EVENT)
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

/// resolveDDLSQL do two things
// * it splits multiple operations in one DDL statement into multiple DDL statements
// * try to apply online ddl by given online
// return @spilted sqls, @online ddl table names, @error
func (s *Syncer) resolveDDLSQL(tctx *tcontext.Context, p *parser.Parser, stmt ast.StmtNode, schema string) (sqls []string, tables map[string]*filter.Table, err error) {
	sqls, err = parserpkg.SplitDDL(stmt, schema)
	if err != nil {
		return nil, nil, err
	}
	if s.onlineDDL == nil {
		return sqls, nil, nil
	}

	statements := make([]string, 0, len(sqls))
	tables = make(map[string]*filter.Table)
	for _, sql := range sqls {
		// filter and store ghost table ddl, transform online ddl
		ss, tableName, err := s.handleOnlineDDL(tctx, p, schema, sql)
		if err != nil {
			return statements, tables, err
		}

		if tableName != nil {
			tables[tableName.String()] = tableName
		}

		statements = append(statements, ss...)
	}
	return statements, tables, nil
}

func (s *Syncer) handleDDL(p *parser.Parser, schema, sql string) (string, [][]*filter.Table, ast.StmtNode, error) {
	stmt, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		return "", nil, nil, terror.Annotatef(terror.ErrSyncerUnitParseStmt.New(err.Error()), "ddl %s", sql)
	}

	tableNames, err := parserpkg.FetchDDLTableNames(schema, stmt)
	if err != nil {
		return "", nil, nil, err
	}

	ignore, err := s.skipQuery(tableNames, stmt, sql)
	if err != nil {
		return "", nil, nil, err
	}
	if ignore {
		return "", nil, stmt, nil
	}

	targetTableNames := make([]*filter.Table, 0, len(tableNames))
	for i := range tableNames {
		schema, table := s.renameShardingSchema(tableNames[i].Schema, tableNames[i].Name)
		tableName := &filter.Table{
			Schema: schema,
			Name:   table,
		}
		targetTableNames = append(targetTableNames, tableName)
	}

	ddl, err := parserpkg.RenameDDLTable(stmt, targetTableNames)
	return ddl, [][]*filter.Table{tableNames, targetTableNames}, stmt, err
}

// handle online ddls
// if sql is online ddls, we would find it's ghost table, and ghost ddls, then replay its table name by real table name
func (s *Syncer) handleOnlineDDL(tctx *tcontext.Context, p *parser.Parser, schema, sql string) ([]string, *filter.Table, error) {
	if s.onlineDDL == nil {
		return []string{sql}, nil, nil
	}

	stmt, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		return nil, nil, terror.Annotatef(terror.ErrSyncerUnitParseStmt.New(err.Error()), "ddl %s", sql)
	}

	tableNames, err := parserpkg.FetchDDLTableNames(schema, stmt)
	if err != nil {
		return nil, nil, err
	}

	sqls, realSchema, realTable, err := s.onlineDDL.Apply(tctx, tableNames, sql, stmt)
	if err != nil {
		return nil, nil, err
	}

	// skip or origin sqls
	if len(sqls) == 0 || (len(sqls) == 1 && sqls[0] == sql) {
		return sqls, nil, nil
	}

	// replace ghost table name by real table name
	targetTables := []*filter.Table{
		{Schema: realSchema, Name: realTable},
	}
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
	return sqls, tableNames[0], nil
}

func (s *Syncer) dropSchemaInSharding(tctx *tcontext.Context, sourceSchema string) error {
	sources := make(map[string][][]string)
	sgs := s.sgk.Groups()
	for name, sg := range sgs {
		if sg.IsSchemaOnly {
			// in sharding group leave handling, we always process schema group,
			// we can ignore schema only group here
			continue
		}
		tables := sg.Tables()
		for _, table := range tables {
			if table[0] != sourceSchema {
				continue
			}
			sources[name] = append(sources[name], table)
		}
	}
	// delete from sharding group firstly
	for name, tables := range sources {
		targetSchema, targetTable := UnpackTableID(name)
		sourceIDs := make([]string, 0, len(tables))
		for _, table := range tables {
			sourceID, _ := GenTableID(table[0], table[1])
			sourceIDs = append(sourceIDs, sourceID)
		}
		err := s.sgk.LeaveGroup(targetSchema, targetTable, sourceIDs)
		if err != nil {
			return err
		}
	}
	// delete from checkpoint
	for _, tables := range sources {
		for _, table := range tables {
			// refine clear them later if failed
			// now it doesn't have problems
			if err1 := s.checkpoint.DeleteTablePoint(tctx, table[0], table[1]); err1 != nil {
				s.tctx.L().Error("fail to delete checkpoint", zap.String("schema", table[0]), zap.String("table", table[1]))
			}
		}
	}
	return nil
}

func (s *Syncer) clearOnlineDDL(tctx *tcontext.Context, targetSchema, targetTable string) error {
	group := s.sgk.Group(targetSchema, targetTable)
	if group == nil {
		return nil
	}

	// return [[schema, table]...]
	tables := group.Tables()

	for _, table := range tables {
		s.tctx.L().Info("finish online ddl", zap.String("schema", table[0]), zap.String("table", table[1]))
		err := s.onlineDDL.Finish(tctx, table[0], table[1])
		if err != nil {
			return terror.Annotatef(err, "finish online ddl on %s.%s", table[0], table[1])
		}
	}

	return nil
}

type shardingDDLInfo struct {
	name       string
	tableNames [][]*filter.Table
	stmt       ast.StmtNode
}
