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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb-tools/pkg/filter"

	"github.com/pingcap/dm/dm/config"
)

// PT handles pt online schema changes
// (_*).*_new ghost table
// (_*).*_old ghost transh table
// we don't support `--new-table-name` flag
type PT struct {
	storge *OnlineDDLStorage
}

// NewPT returns pt online schema changes plugin
func NewPT(cfg *config.SubTaskConfig) (OnlinePlugin, error) {
	g := &PT{
		storge: NewOnlineDDLStorage(cfg),
	}

	return g, errors.Trace(g.storge.Init())
}

// Apply implements interface.
// returns ddls, real schema, real table, error
func (p *PT) Apply(tables []*filter.Table, statement string, stmt ast.StmtNode) ([]string, string, string, error) {
	if len(tables) < 1 {
		return nil, "", "", errors.NotValidf("tables should not be empty!")
	}

	schema, table := tables[0].Schema, tables[0].Name
	targetSchema, targetTable := p.RealName(schema, table)
	tp := p.TableType(table)

	switch tp {
	case realTable:
		switch stmt.(type) {
		case *ast.RenameTableStmt:
			if len(tables) != 2 {
				return nil, "", "", errors.NotValidf("tables should contain old and new table name")
			}

			tp1 := p.TableType(tables[1].Name)
			if tp1 == trashTable {
				return nil, "", "", nil
			} else if tp1 == ghostTable {
				return nil, "", "", errors.NotSupportedf("rename table to ghost table %s", statement)
			}
		}
		return []string{statement}, schema, table, nil
	case trashTable:
		// ignore trashTable
		switch stmt.(type) {
		case *ast.RenameTableStmt:
			if len(tables) != 2 {
				return nil, "", "", errors.NotValidf("tables should contain old and new table name")
			}

			tp1 := p.TableType(tables[1].Name)
			if tp1 == ghostTable {
				return nil, "", "", errors.NotSupportedf("rename ghost table to other ghost table %s", statement)
			}
		}
	case ghostTable:
		// record ghost table ddl changes
		switch stmt.(type) {
		case *ast.CreateTableStmt:
			err := p.storge.Delete(schema, table)
			if err != nil {
				return nil, "", "", errors.Trace(err)
			}
		case *ast.DropTableStmt:
			err := p.storge.Delete(schema, table)
			if err != nil {
				return nil, "", "", errors.Trace(err)
			}
		case *ast.RenameTableStmt:
			if len(tables) != 2 {
				return nil, "", "", errors.NotValidf("tables should contain old and new table name")
			}

			tp1 := p.TableType(tables[1].Name)
			if tp1 == realTable {
				ghostInfo := p.storge.Get(schema, table)
				if ghostInfo != nil {
					return ghostInfo.DDLs, tables[1].Schema, tables[1].Name, nil
				}
				return nil, "", "", errors.NotFoundf("online ddls on ghost table `%s`.`%s`", schema, table)
			} else if tp1 == ghostTable {
				return nil, "", "", errors.NotSupportedf("rename ghost table to other ghost table %s", statement)
			}

			// rename ghost table to trash table
			err := p.storge.Delete(schema, table)
			if err != nil {
				return nil, "", "", errors.Trace(err)
			}

		default:
			err := p.storge.Save(schema, table, targetSchema, targetTable, statement)
			if err != nil {
				return nil, "", "", errors.Trace(err)
			}
		}
	}

	return nil, schema, table, nil
}

// Finish implements interface
func (p *PT) Finish(schema, table string) error {
	if p == nil {
		return nil
	}

	return errors.Trace(p.storge.Delete(schema, table))
}

// TableType implements interface
func (p *PT) TableType(table string) TableType {
	// 5 is _ _gho/ghc/del
	if len(table) > 5 {
		if strings.HasPrefix(table, "_") && strings.HasSuffix(table, "_new") {
			return ghostTable
		}

		if strings.HasPrefix(table, "_") && strings.HasSuffix(table, "_old") {
			return trashTable
		}
	}

	return realTable
}

// RealName implements interface
func (p *PT) RealName(schema, table string) (string, string) {
	tp := p.TableType(table)
	if tp == trashTable || tp == ghostTable {
		table = strings.TrimLeft(table, "_")
		table = table[:len(table)-4]
	}

	return schema, table
}

// Clear clears online ddl information
func (p *PT) Clear() error {
	return errors.Trace(p.storge.Clear())
}

// Close implements interface
func (p *PT) Close() {
	p.storge.Close()
}
