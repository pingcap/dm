package syncer

import (
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb-enterprise-tools/dm/config"
	"github.com/pingcap/tidb-enterprise-tools/pkg/filter"
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
// returns ddls, real schema, real table, error
func (g *Ghost) Apply(tables []*filter.Table, statement string, stmt ast.StmtNode) ([]string, string, string, error) {
	if len(tables) < 1 {
		return nil, "", "", errors.NotValidf("tables should not be empty!")
	}

	schema, table := tables[0].Schema, tables[0].Name
	targetSchema, targetTable := g.RealName(schema, table)
	tp := g.TableType(table)

	switch tp {
	case realTable:
		switch stmt.(type) {
		case *ast.RenameTableStmt:
			if len(tables) != 2 {
				return nil, "", "", errors.NotValidf("tables should contain old and new table name")
			}

			tp1 := g.TableType(tables[1].Name)
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

			tp1 := g.TableType(tables[1].Name)
			if tp1 == ghostTable {
				return nil, "", "", errors.NotSupportedf("rename ghost table to other ghost table %s", statement)
			}
		}
	case ghostTable:
		// record ghost table ddl changes
		switch stmt.(type) {
		case *ast.CreateTableStmt:
			err := g.storge.Delete(schema, table)
			if err != nil {
				return nil, "", "", errors.Trace(err)
			}
		case *ast.DropTableStmt:
			err := g.storge.Delete(schema, table)
			if err != nil {
				return nil, "", "", errors.Trace(err)
			}
		case *ast.RenameTableStmt:
			if len(tables) != 2 {
				return nil, "", "", errors.NotValidf("tables should contain old and new table name")
			}

			tp1 := g.TableType(tables[1].Name)
			if tp1 == realTable {
				ghostInfo := g.storge.Get(schema, table)
				if ghostInfo != nil {
					return ghostInfo.DDLs, tables[1].Schema, tables[1].Name, nil
				}
				return nil, "", "", errors.NotFoundf("online ddls on ghost table `%s`.`%s`", schema, table)
			} else if tp1 == ghostTable {
				return nil, "", "", errors.NotSupportedf("rename ghost table to other ghost table %s", statement)
			}

			// rename ghost table to trash table
			err := g.storge.Delete(schema, table)
			if err != nil {
				return nil, "", "", errors.Trace(err)
			}

		default:
			err := g.storge.Save(schema, table, targetSchema, targetTable, statement)
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

	ghostInfo := g.storge.Get(schema, table)
	return ghostInfo != nil
}

// Finish implements interface
func (g *Ghost) Finish(schema, table string) error {
	if g == nil {
		return nil
	}

	return errors.Trace(g.storge.Delete(schema, table))
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
