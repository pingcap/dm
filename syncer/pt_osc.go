package syncer

import (
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-enterprise-tools/dm/config"
	"github.com/pingcap/tidb/ast"
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
// returns ignored(bool), replaced(self) schema, repliaced(slef) table, error
func (p *PT) Apply(schema, table, statement string, stmt ast.StmtNode) ([]string, string, string, error) {
	targetSchema, targetTable := p.RealName(schema, table)
	tp := p.TableType(table)

	switch tp {
	case realTable:
		// if rename ddl, we should find whether there are related ghost table
		_, ok := stmt.(*ast.RenameTableStmt)
		if ok {
			ghostInfo := p.storge.Get(targetSchema, targetTable)
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
			err := p.storge.Delete(targetSchema, targetTable)
			if err != nil {
				return nil, "", "", errors.Trace(err)
			}
		case *ast.RenameTableStmt:
			// not delete ddl changes
		default:
			err := p.storge.Save(targetSchema, targetTable, schema, table, statement)
			if err != nil {
				return nil, "", "", errors.Trace(err)
			}
		}
	}

	return nil, schema, table, nil
}

// InOnlineDDL implements interface
func (p *PT) InOnlineDDL(schema, table string) bool {
	if p == nil {
		return false
	}

	targetSchema, targetTable := p.RealName(schema, table)
	ghostInfo := p.storge.Get(targetSchema, targetTable)
	return ghostInfo != nil
}

// Finish implements interface
func (p *PT) Finish(schema, table string) error {
	if p == nil {
		return nil
	}

	targetSchema, targetTable := p.RealName(schema, table)
	return errors.Trace(p.storge.Delete(targetSchema, targetTable))
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

// GhostName implements interface
func (p *PT) GhostName(schema, table string) (string, string) {
	tp := p.TableType(table)
	if tp == ghostTable {
		return schema, table
	}

	if tp == trashTable {
		table = strings.TrimLeft(table, "_")
		table = table[:len(table)-4]
	}

	return schema, fmt.Sprintf("_%s_new", table)
}

// SchemeName implements interface
func (p *PT) SchemeName() string {
	return config.PT
}

// Clear clears online ddl information
func (p *PT) Clear() error {
	return errors.Trace(p.storge.Clear())
}

// Close implements interface
func (p *PT) Close() {
	p.storge.Close()
}
