package check

import (
	"context"
	"database/sql"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/util/charset"
)

// TablesChecker checks compatibility of table structures, there are differents between MySQL and TiDB.
// In generally we need to check definitions of columns, constraints and table options.
// Because of the early TiDB engineering design, we did not have a complete list of check items, which are all based on experience now.
type TablesChecker struct {
	db     *sql.DB
	tables map[string][]string // schema => []table; if []table is empty, query tables from db

	errors, warnings int
}

// NewTablesChecker returns a Checker
func NewTablesChecker(db *sql.DB, tables map[string][]string) Checker {
	return &TablesChecker{
		db:     db,
		tables: tables,
	}
}

// Check implements Checker interface
func (c *TablesChecker) Check(ctx context.Context) *Result {
	r := &Result{
		Name:  c.Name(),
		Desc:  "table structure compatibility check",
		State: StateSuccess,
	}

	var err error

	for schema, tables := range c.tables {
		if len(tables) == 0 {
			tables, err = dbutil.GetTables(ctx, c.db, schema)
			if err != nil {
				markCheckError(r, err)
				return r
			}
		}

		for _, table := range tables {
			statement, err := dbutil.GetCreateTableSQL(ctx, c.db, schema, table)
			if err != nil {
				markCheckError(r, err)
				return r
			}

			err = c.checkCreateSQL(statement)
			if err != nil {
				markCheckError(r, errors.Errorf("\n[schema %s, table %s]\n%v", schema, table, err))
			}
		}
	}

	if c.errors > 0 {
		r.State = StateFailure
	} else if c.warnings > 0 {
		r.State = StateWarning
	}

	return r
}

// Name implements Checker interface
func (c *TablesChecker) Name() string {
	return "table structure compatibility check"
}

func (c *TablesChecker) checkCreateSQL(statement string) error {
	stmt, err := parser.New().ParseOneStmt(statement, "", "")
	if err != nil {
		return errors.Annotatef(err, " parse %s error", statement)
	}
	// Analyze ast
	err = c.checkAST(stmt)
	if err != nil {
		return errors.Annotatef(err, "create table statement %s", statement)
	}
	return nil
}

func (c *TablesChecker) checkAST(stmt ast.StmtNode) error {
	st, ok := stmt.(*ast.CreateTableStmt)
	if !ok {
		return errors.Errorf("Expect CreateTableStmt but got %T", stmt)
	}
	var err error

	// check columns
	for _, def := range st.Cols {
		err = c.checkColumnDef(def)
		if err != nil {
			return errors.Trace(err)
		}
	}
	// check constrains
	for _, cst := range st.Constraints {
		err = c.checkConstraint(cst)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// check options
	for _, opt := range st.Options {
		err = c.checkTableOption(opt)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (c *TablesChecker) checkColumnDef(def *ast.ColumnDef) error {
	return nil
}

func (c *TablesChecker) checkConstraint(cst *ast.Constraint) error {
	switch cst.Tp {
	case ast.ConstraintForeignKey:
		c.warnings++
		return errors.Errorf("Foreign Key is parsed but ignored by TiDB.")
	}
	return nil
}

func (c *TablesChecker) checkTableOption(opt *ast.TableOption) error {
	switch opt.Tp {
	case ast.TableOptionCharset:
		// Check charset
		cs := strings.ToLower(opt.StrValue)
		if cs != "binary" && !charset.ValidCharsetAndCollation(cs, "") {
			c.errors++
			return errors.Errorf("Unsupported charset %s", opt.StrValue)
		}
	}
	return nil
}
