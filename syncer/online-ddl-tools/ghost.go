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

package onlineddl

import (
	"strings"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/config"
	tcontext "github.com/pingcap/dm/pkg/context"
	parserpkg "github.com/pingcap/dm/pkg/parser"
	"github.com/pingcap/dm/pkg/terror"
)

// Ghost handles gh-ost online ddls (not complete, don't need to review it)
// _*_gho ghost table
// _*_ghc ghost changelog table
// _*_del ghost transh table.
type Ghost struct {
	storge *Storage
}

// NewGhost returns gh-oat online plugin.
func NewGhost(tctx *tcontext.Context, cfg *config.SubTaskConfig) (OnlinePlugin, error) {
	g := &Ghost{
		storge: NewOnlineDDLStorage(tcontext.Background().WithLogger(tctx.L().WithFields(zap.String("online ddl", "gh-ost"))), cfg), // create a context for logger
	}

	return g, g.storge.Init(tctx)
}

// Apply implements interface.
// returns ddls, real schema, real table, error.
// nolint:dupl
func (g *Ghost) Apply(tctx *tcontext.Context, tables []*filter.Table, statement string, stmt ast.StmtNode) ([]string, string, string, error) {
	if len(tables) < 1 {
		return nil, "", "", terror.ErrSyncerUnitGhostApplyEmptyTable.Generate()
	}

	schema, table := tables[0].Schema, tables[0].Name
	targetTable := g.RealName(table)
	tp := g.TableType(table)

	switch tp {
	case RealTable:
		if _, ok := stmt.(*ast.RenameTableStmt); ok {
			if len(tables) != parserpkg.SingleRenameTableNameNum {
				return nil, "", "", terror.ErrSyncerUnitGhostRenameTableNotValid.Generate()
			}

			tp1 := g.TableType(tables[1].Name)
			if tp1 == TrashTable {
				return nil, "", "", nil
			} else if tp1 == GhostTable {
				return nil, "", "", terror.ErrSyncerUnitGhostRenameToGhostTable.Generate(statement)
			}
		}
		return []string{statement}, schema, table, nil
	case TrashTable:
		// ignore TrashTable
		if _, ok := stmt.(*ast.RenameTableStmt); ok {
			if len(tables) != parserpkg.SingleRenameTableNameNum {
				return nil, "", "", terror.ErrSyncerUnitGhostRenameTableNotValid.Generate()
			}

			tp1 := g.TableType(tables[1].Name)
			if tp1 == GhostTable {
				return nil, "", "", terror.ErrSyncerUnitGhostRenameGhostTblToOther.Generate(statement)
			}
		}
	case GhostTable:
		// record ghost table ddl changes
		switch stmt.(type) {
		case *ast.CreateTableStmt:
			err := g.storge.Delete(tctx, schema, table)
			if err != nil {
				return nil, "", "", err
			}
		case *ast.DropTableStmt:
			err := g.storge.Delete(tctx, schema, table)
			if err != nil {
				return nil, "", "", err
			}
		case *ast.RenameTableStmt:
			if len(tables) != parserpkg.SingleRenameTableNameNum {
				return nil, "", "", terror.ErrSyncerUnitGhostRenameTableNotValid.Generate()
			}

			tp1 := g.TableType(tables[1].Name)
			if tp1 == RealTable {
				ghostInfo := g.storge.Get(schema, table)
				if ghostInfo != nil {
					return ghostInfo.DDLs, tables[1].Schema, tables[1].Name, nil
				}
				return nil, "", "", terror.ErrSyncerUnitGhostOnlineDDLOnGhostTbl.Generate(schema, table)
			} else if tp1 == GhostTable {
				return nil, "", "", terror.ErrSyncerUnitGhostRenameGhostTblToOther.Generate(statement)
			}

			// rename ghost table to trash table
			err := g.storge.Delete(tctx, schema, table)
			if err != nil {
				return nil, "", "", err
			}

		default:
			err := g.storge.Save(tctx, schema, table, schema, targetTable, statement)
			if err != nil {
				return nil, "", "", err
			}
		}
	}

	return nil, schema, table, nil
}

// Finish implements interface.
func (g *Ghost) Finish(tctx *tcontext.Context, schema, table string) error {
	if g == nil {
		return nil
	}

	return g.storge.Delete(tctx, schema, table)
}

// TableType implements interface.
func (g *Ghost) TableType(table string) TableType {
	// 5 is _ _gho/ghc/del
	if len(table) > 5 && table[0] == '_' {
		if strings.HasSuffix(table, "_gho") {
			return GhostTable
		}

		if strings.HasSuffix(table, "_ghc") || strings.HasSuffix(table, "_del") {
			return TrashTable
		}
	}

	return RealTable
}

// RealName implements interface.
func (g *Ghost) RealName(table string) string {
	tp := g.TableType(table)
	if tp == TrashTable || tp == GhostTable {
		table = table[1 : len(table)-4]
	}

	return table
}

// Clear clears online ddl information.
func (g *Ghost) Clear(tctx *tcontext.Context) error {
	return g.storge.Clear(tctx)
}

// Close implements interface.
func (g *Ghost) Close() {
	g.storge.Close()
}

// ResetConn implements interface.
func (g *Ghost) ResetConn(tctx *tcontext.Context) error {
	return g.storge.ResetConn(tctx)
}
