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

const (
	// SingleRenameTableNameNum stands for number of TableNames in a single table renaming. it's 2 after
	// https://github.com/pingcap/parser/pull/1021
	SingleRenameTableNameNum = 2
)

// Parse wraps parser.Parse(), makes `parser` suitable for dm
func Parse(p *parser.Parser, sql, charset, collation string) (stmt []ast.StmtNode, err error) {
	stmts, warnings, err := p.Parse(sql, charset, collation)
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
	switch n := in.(type) {
	case *ast.TableName:
		tb := &filter.Table{Schema: n.Schema.L, Name: n.Name.L}
		if tb.Schema == "" {
			tb.Schema = tne.curDB
		}
		tne.names = append(tne.names, tb)
		return in, true
	case *ast.ColumnName:
		tb := &filter.Table{Schema: n.Schema.L, Name: n.Table.L}
		// this column has specified a table, such as
		// CREATE VIEW `v1` AS SELECT `t1`.`c1` AS `c1` FROM `t1`
		if tb.Name != "" {
			if tb.Schema == "" {
				tb.Schema = tne.curDB
			}
			tne.names = append(tne.names, tb)
		}
		return in, true
	}

	return in, false
}

func (tne *tableNameExtractor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

// FetchDDLTableNames returns tableNames in ddl the result contains many tableName.
// Because we use visitor pattern, first tableName is always upper-most table in ast
// specifically, for `create table like` DDL, result contains [sourceTableName, sourceRefTableName]
// for rename table ddl, result contains [old1, new1, old1, new1, old2, new2, old3, new3, ...] because of TiDB parser
// for other DDL, order of tableName is the node visit order
func FetchDDLTableNames(schema string, stmt ast.StmtNode) ([]*filter.Table, error) {
	switch stmt.(type) {
	case ast.DDLNode:
	default:
		return nil, terror.ErrUnknownTypeDDL.Generate(stmt)
	}

	// special cases: schema related SQLs doesn't have tableName
	switch v := stmt.(type) {
	case *ast.AlterDatabaseStmt:
		return []*filter.Table{genTableName(v.Name, "")}, nil
	case *ast.CreateDatabaseStmt:
		return []*filter.Table{genTableName(v.Name, "")}, nil
	case *ast.DropDatabaseStmt:
		return []*filter.Table{genTableName(v.Name, "")}, nil
	}

	e := &tableNameExtractor{
		curDB: schema,
		names: make([]*filter.Table, 0),
	}
	stmt.Accept(e)

	return e.names, nil
}

type tableRenameVisitor struct {
	targetNames []*filter.Table
	i           int
	hasErr      bool
}

func (v *tableRenameVisitor) Enter(in ast.Node) (ast.Node, bool) {
	if v.hasErr {
		return in, true
	}
	switch n := in.(type) {
	case *ast.TableName:
		if v.i >= len(v.targetNames) {
			v.hasErr = true
			return in, true
		}
		n.Schema = model.NewCIStr(v.targetNames[v.i].Schema)
		n.Name = model.NewCIStr(v.targetNames[v.i].Name)
		v.i++
		return in, true
	case *ast.ColumnName:
		if n.Table.L != "" {
			if v.i >= len(v.targetNames) {
				v.hasErr = true
				return in, true
			}
			n.Schema = model.NewCIStr(v.targetNames[v.i].Schema)
			n.Table = model.NewCIStr(v.targetNames[v.i].Name)
			v.i++
		}
		return in, true
	}

	return in, false
}

func (v *tableRenameVisitor) Leave(in ast.Node) (ast.Node, bool) {
	if v.hasErr {
		return in, false
	}
	return in, true
}

// RenameDDLTable renames table names in ddl by given `targetTableNames`
// argument `targetTableNames` is same with return value of FetchDDLTableNames
// returned DDL is formatted like StringSingleQuotes, KeyWordUppercase and NameBackQuotes
func RenameDDLTable(stmt ast.StmtNode, targetTableNames []*filter.Table) (string, error) {
	switch stmt.(type) {
	case ast.DDLNode:
	default:
		return "", terror.ErrUnknownTypeDDL.Generate(stmt)
	}

	switch v := stmt.(type) {
	case *ast.AlterDatabaseStmt:
		v.Name = targetTableNames[0].Schema
	case *ast.CreateDatabaseStmt:
		v.Name = targetTableNames[0].Schema
	case *ast.DropDatabaseStmt:
		v.Name = targetTableNames[0].Schema
	default:
		visitor := &tableRenameVisitor{
			targetNames: targetTableNames,
		}
		stmt.Accept(visitor)
		if visitor.hasErr {
			return "", terror.ErrRewriteSQL.Generate(stmt, targetTableNames)
		}
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

type dbNameAppender struct {
	curDB model.CIStr
}

func (v *dbNameAppender) Enter(in ast.Node) (ast.Node, bool) {
	switch n := in.(type) {
	case *ast.TableName:
		if n.Schema.O == "" {
			n.Schema = v.curDB
		}
		return in, true
	case *ast.ColumnName:
		if n.Table.O != "" && n.Schema.O == "" {
			n.Schema = v.curDB
		}
		return in, true
	}

	return in, false
}

func (v *dbNameAppender) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
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
	case *ast.CreateViewStmt:
		visitor := &dbNameAppender{curDB: schemaName}
		v.Accept(visitor)
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
