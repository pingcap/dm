package main

import (
	"bytes"
	"fmt"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/model"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

func main() {
	sqls := []string{
		"ALTER TABLE `test`.`test` ADD COLUMN d int(11) GENERATED ALWAYS AS (c + 1) VIRTUAL",
		"ALTER TABLE `test`.`test` ADD COLUMN d int(11) AS (1 + 1) STORED",
	}

	var b []byte
	bf := bytes.NewBuffer(b)
	schemaName := model.NewCIStr("test")
	ctx := &format.RestoreCtx{
		Flags: format.DefaultRestoreFlags,
		In:    bf,
	}

	for _, sql := range sqls {
		fmt.Println("\n###################################################")
		fmt.Println("handle", sql)
		stmts, warnings, err := parser.New().Parse(sql, "", "")
		fmt.Println("result:", warnings, err)

		for _, stmt := range stmts {
			switch v := stmt.(type) {
			case *ast.CreateDatabaseStmt:
				v.IfNotExists = true
			case *ast.DropDatabaseStmt:
				v.IfExists = true
			case *ast.DropTableStmt:
				v.IfExists = true
				for _, t := range v.Tables {
					if t.Schema.O == "" {
						t.Schema = schemaName
					}
				}
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
					stmt.Restore(ctx)
					fmt.Println(bf.String())
				}
				continue
			case *ast.AlterTableStmt:
				specs := v.Specs

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
					stmt.Restore(ctx)
					fmt.Println(bf.String())

					if spec.Tp == ast.AlterTableRenameTable {
						v.Table = spec.NewTable
					}
				}

				continue
			}

			bf.Reset()
			stmt.Restore(ctx)
			fmt.Println(bf.String())
		}
	}
}
