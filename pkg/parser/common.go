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

package parser

import (
	"bytes"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/filter"
	_ "github.com/pingcap/tidb/types/parser_driver" // for import parser driver
	"go.uber.org/zap"
)

// Parse wraps parser.Parse(), makes `parser` suitable for dm
func Parse(p *parser.Parser, sql, charset, collation string) (stmt []ast.StmtNode, err error) {
	stmts, warnings, err := p.Parse(sql, charset, collation)
	if err != nil {
		log.L().Error("parse statement", zap.String("sql", sql), log.ShortError(err))
	}

	if len(warnings) > 0 {
		log.L().Warn("parse statement", zap.String("sql", sql), zap.Errors("warning messages", warnings))
	}

	return stmts, terror.ErrParseSQL.Delegate(err)
}

// ref: https://github.com/pingcap/tidb/blob/09feccb529be2830944e11f5fed474020f50370f/server/sql_info_fetcher.go#L46
type tableNameExtractor struct {
	curDB string
	names []*filter.Table
}

func (tne *tableNameExtractor) Enter(in ast.Node) (ast.Node, bool) {
	if t, ok := in.(*ast.TableName); ok {
		tb := &filter.Table{Schema: t.Schema.L, Name: t.Name.L}
		if tb.Schema == "" {
			tb.Schema = tne.curDB
		}
		tne.names = append(tne.names, tb)
		return in, true
	}
	return in, false
}

func (tne *tableNameExtractor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

// FetchDDLTableNames returns non-repeat tableNames in ddl, duplicate tableName is left at its first appearance
// the result contains many tableName. Because we use visitor pattern, first tableName is always upper-most table in ast
// specifically, for `create table like` DDL, result contains [sourceTableName, sourceRefTableName]
// for rename table ddl, result contains [oldTableName, newTableName]
// for other DDL, order of tableName is the node visit order
func FetchDDLTableNames(schema string, stmt ast.StmtNode) ([]*filter.Table, error) {
	var res []*filter.Table

	switch stmt.(type) {
	case ast.DDLNode:
	default:
		return res, terror.ErrUnknownTypeDDL.Generate(stmt)
	}

	// special cases: schema related SQLs doesn't have tableName, and keep old behavior with drop multiple tables
	shouldReturn := false
	switch v := stmt.(type) {
	case *ast.AlterDatabaseStmt:
		res = append(res, genTableName(v.Name, ""))
		shouldReturn = true
	case *ast.CreateDatabaseStmt:
		res = append(res, genTableName(v.Name, ""))
		shouldReturn = true
	case *ast.DropDatabaseStmt:
		res = append(res, genTableName(v.Name, ""))
		shouldReturn = true
	case *ast.DropTableStmt:
		if len(v.Tables) != 1 {
			return res, terror.ErrDropMultipleTables.Generate()
		}
	}
	if shouldReturn {
		return res, nil
	}

	e := &tableNameExtractor{
		curDB: schema,
		names: make([]*filter.Table, 0),
	}
	stmt.Accept(e)

	seen := map[filter.Table]struct{}{}
	for _, table := range e.names {
		if _, ok := seen[*table]; !ok {
			seen[*table] = struct{}{}
			res = append(res, table)
		}
	}

	return res, nil
}

// RenameDDLTable renames table names in ddl by given `targetTableNames`
// argument `targetTableNames` is same with return value of FetchDDLTableNames
// returned DDL is formatted like StringSingleQuotes, KeyWordUppercase and NameBackQuotes
func RenameDDLTable(stmt ast.StmtNode, targetTableNames []*filter.Table) (string, error) {
	switch v := stmt.(type) {
	case *ast.AlterDatabaseStmt:
		v.Name = targetTableNames[0].Schema

	case *ast.CreateDatabaseStmt:
		v.Name = targetTableNames[0].Schema

	case *ast.DropDatabaseStmt:
		v.Name = targetTableNames[0].Schema

	case *ast.CreateTableStmt:
		v.Table.Schema = model.NewCIStr(targetTableNames[0].Schema)
		v.Table.Name = model.NewCIStr(targetTableNames[0].Name)

		if v.ReferTable != nil {
			v.ReferTable.Schema = model.NewCIStr(targetTableNames[1].Schema)
			v.ReferTable.Name = model.NewCIStr(targetTableNames[1].Name)
		}

	case *ast.DropTableStmt:
		if len(v.Tables) > 1 {
			return "", terror.ErrDropMultipleTables.Generate()
		}

		v.Tables[0].Schema = model.NewCIStr(targetTableNames[0].Schema)
		v.Tables[0].Name = model.NewCIStr(targetTableNames[0].Name)

	case *ast.TruncateTableStmt:
		v.Table.Schema = model.NewCIStr(targetTableNames[0].Schema)
		v.Table.Name = model.NewCIStr(targetTableNames[0].Name)

	case *ast.DropIndexStmt:
		v.Table.Schema = model.NewCIStr(targetTableNames[0].Schema)
		v.Table.Name = model.NewCIStr(targetTableNames[0].Name)
	case *ast.CreateIndexStmt:
		v.Table.Schema = model.NewCIStr(targetTableNames[0].Schema)
		v.Table.Name = model.NewCIStr(targetTableNames[0].Name)
	case *ast.RenameTableStmt:
		if len(v.TableToTables) > 1 {
			return "", terror.ErrRenameMultipleTables.Generate()
		}

		v.TableToTables[0].OldTable.Schema = model.NewCIStr(targetTableNames[0].Schema)
		v.TableToTables[0].OldTable.Name = model.NewCIStr(targetTableNames[0].Name)
		v.TableToTables[0].NewTable.Schema = model.NewCIStr(targetTableNames[1].Schema)
		v.TableToTables[0].NewTable.Name = model.NewCIStr(targetTableNames[1].Name)

	case *ast.AlterTableStmt:
		if len(v.Specs) > 1 {
			return "", terror.ErrAlterMultipleTables.Generate()
		}

		v.Table.Schema = model.NewCIStr(targetTableNames[0].Schema)
		v.Table.Name = model.NewCIStr(targetTableNames[0].Name)

		if v.Specs[0].Tp == ast.AlterTableRenameTable {
			v.Specs[0].NewTable.Schema = model.NewCIStr(targetTableNames[1].Schema)
			v.Specs[0].NewTable.Name = model.NewCIStr(targetTableNames[1].Name)
		}

	default:
		return "", terror.ErrUnknownTypeDDL.Generate(stmt)
	}

	var b []byte
	bf := bytes.NewBuffer(b)
	err := stmt.Restore(&format.RestoreCtx{
		Flags: format.DefaultRestoreFlags,
		In:    bf,
	})
	if err != nil {
		return "", terror.ErrRestoreASTNode.Delegate(err)
	}

	return bf.String(), nil
}

// SplitDDL splits multiple operations in one DDL statement into multiple DDL statements
// returned DDL is formatted like StringSingleQuotes, KeyWordUppercase and NameBackQuotes
// if fail to restore, it would not restore the value of `stmt` (it changes it's values if `stmt` is one of  DropTableStmt, RenameTableStmt, AlterTableStmt)
func SplitDDL(stmt ast.StmtNode, schema string) (sqls []string, err error) {
	var (
		schemaName = model.NewCIStr(schema) // fill schema name
		bf         = new(bytes.Buffer)
		ctx        = &format.RestoreCtx{
			Flags: format.DefaultRestoreFlags,
			In:    bf,
		}
	)

	switch v := stmt.(type) {
	case *ast.AlterDatabaseStmt:
	case *ast.CreateDatabaseStmt:
		v.IfNotExists = true
	case *ast.DropDatabaseStmt:
		v.IfExists = true
	case *ast.DropTableStmt:
		v.IfExists = true

		tables := v.Tables
		for _, t := range tables {
			if t.Schema.O == "" {
				t.Schema = schemaName
			}

			v.Tables = []*ast.TableName{t}
			bf.Reset()
			err = stmt.Restore(ctx)
			if err != nil {
				v.Tables = tables
				return nil, terror.ErrRestoreASTNode.Delegate(err)
			}

			sqls = append(sqls, bf.String())
		}
		v.Tables = tables

		return sqls, nil
	case *ast.CreateTableStmt:
		v.IfNotExists = true
		if v.Table.Schema.O == "" {
			v.Table.Schema = schemaName
		}

		if v.ReferTable != nil && v.ReferTable.Schema.O == "" {
			v.ReferTable.Schema = schemaName
		}
	case *ast.TruncateTableStmt:
		if v.Table.Schema.O == "" {
			v.Table.Schema = schemaName
		}
	case *ast.DropIndexStmt:
		v.IfExists = true
		if v.Table.Schema.O == "" {
			v.Table.Schema = schemaName
		}
	case *ast.CreateIndexStmt:
		if v.Table.Schema.O == "" {
			v.Table.Schema = schemaName
		}
	case *ast.RenameTableStmt:
		t2ts := v.TableToTables
		for _, t2t := range t2ts {
			if t2t.OldTable.Schema.O == "" {
				t2t.OldTable.Schema = schemaName
			}
			if t2t.NewTable.Schema.O == "" {
				t2t.NewTable.Schema = schemaName
			}

			v.TableToTables = []*ast.TableToTable{t2t}

			bf.Reset()
			err = stmt.Restore(ctx)
			if err != nil {
				v.TableToTables = t2ts
				return nil, terror.ErrRestoreASTNode.Delegate(err)
			}

			sqls = append(sqls, bf.String())
		}
		v.TableToTables = t2ts

		return sqls, nil
	case *ast.AlterTableStmt:
		specs := v.Specs
		table := v.Table

		if v.Table.Schema.O == "" {
			v.Table.Schema = schemaName
		}

		for _, spec := range specs {
			if spec.Tp == ast.AlterTableRenameTable {
				if spec.NewTable.Schema.O == "" {
					spec.NewTable.Schema = schemaName
				}
			}

			v.Specs = []*ast.AlterTableSpec{spec}

			bf.Reset()
			err = stmt.Restore(ctx)
			if err != nil {
				v.Specs = specs
				v.Table = table
				return nil, terror.ErrRestoreASTNode.Delegate(err)
			}
			sqls = append(sqls, bf.String())

			if spec.Tp == ast.AlterTableRenameTable {
				v.Table = spec.NewTable
			}
		}
		v.Specs = specs
		v.Table = table

		return sqls, nil
	default:
		return nil, terror.ErrUnknownTypeDDL.Generate(stmt)
	}

	bf.Reset()
	err = stmt.Restore(ctx)
	if err != nil {
		return nil, terror.ErrRestoreASTNode.Delegate(err)
	}
	sqls = append(sqls, bf.String())

	return sqls, nil
}

func genTableName(schema string, table string) *filter.Table {
	return &filter.Table{Schema: schema, Name: table}
}
