package syncer

import (
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-enterprise-tools/dm/config"
	"github.com/pingcap/tidb/ast"
)

// Ghost handles gh-ost online ddls (not complete, don't need to review it)
// _*_gho ghost table
// _*_ghc ghost changelog table
// _*_del ghost transh table
type Ghost struct {
	storge *OnlineDDLStorage
}

// NewGhost returns gh-oat online plugin
func NewGhost(cfg *config.SubTaskConfig) (OnlinePlugin, error) {
	g := &Ghost{
		storge: NewOnlineDDLStorage(cfg),
	}

	return g, errors.Trace(g.storge.Init())
}

// Apply implements interface.
// returns ignored(bool), replaced(self) schema, repliaced(slef) table, error
func (g *Ghost) Apply(schema, table, statement string, stmt ast.StmtNode) ([]string, string, string, error) {
	targetSchema, targetTable := g.RealName(schema, table)
	tp := g.TableType(table)

	switch tp {
	case realTable:
		// if rename ddl, we should find whether there are related ghost table
		_, ok := stmt.(*ast.RenameTableStmt)
		if ok {
			ghostInfo := g.storge.Get(targetSchema, targetTable)
			if ghostInfo != nil {
				return ghostInfo.DDLs, ghostInfo.Schema, ghostInfo.Table, nil
			}
		}
		return []string{statement}, schema, table, nil
	case trashTable:
		// ignore trashTable
	case ghostTable:
		// record ghost table ddl changes
		switch stmt.(type) {
		case *ast.CreateTableStmt:
			err := g.storge.Delete(targetSchema, targetTable)
			if err != nil {
				return nil, "", "", errors.Trace(err)
			}
		case *ast.RenameTableStmt:
			// not delete ddl changes
		default:
			err := g.storge.Save(targetSchema, targetTable, schema, table, statement)
			if err != nil {
				return nil, "", "", errors.Trace(err)
			}
		}
	}

	return nil, schema, table, nil
}

// InOnlineDDL implements interface
func (g *Ghost) InOnlineDDL(schema, table string) bool {
	if g == nil {
		return false
	}

	targetSchema, targetTable := g.RealName(schema, table)
	ghostInfo := g.storge.Get(targetSchema, targetTable)
	return ghostInfo != nil
}

// Finish implements interface
func (g *Ghost) Finish(schema, table string) error {
	if g == nil {
		return nil
	}

	targetSchema, targetTable := g.RealName(schema, table)
	return errors.Trace(g.storge.Delete(targetSchema, targetTable))
}

// TableType implements interface
func (g *Ghost) TableType(table string) TableType {
	// 5 is _ _gho/ghc/del
	if len(table) > 5 && table[0] == '_' {
		if strings.HasSuffix(table, "_gho") {
			return ghostTable
		}

		if strings.HasSuffix(table, "_ghc") || strings.HasSuffix(table, "_del") {
			return trashTable
		}
	}

	return realTable
}

// RealName implements interface
func (g *Ghost) RealName(schema, table string) (string, string) {
	tp := g.TableType(table)
	if tp == trashTable || tp == ghostTable {
		table = table[1 : len(table)-4]
	}

	return schema, table
}

// GhostName implements interface
func (g *Ghost) GhostName(schema, table string) (string, string) {
	tp := g.TableType(table)
	if tp == ghostTable {
		return schema, table
	}

	if tp == trashTable {
		table = table[1 : len(table)-4]
	}

	return schema, fmt.Sprintf("_%s_gho", table)
}

// SchemeName implements interface
func (g *Ghost) SchemeName() string {
	return config.GHOST
}

// Clear clears online ddl information
func (g *Ghost) Clear() error {
	return errors.Trace(g.storge.Clear())
}

// Close implements interface
func (g *Ghost) Close() {
	g.storge.Close()
}
