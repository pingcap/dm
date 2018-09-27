// Copyright 2018 PingCAP, Inc.
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

package check

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pingcap/tidb-tools/pkg/column-mapping"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/util/charset"
)

// AutoIncrementKeyChecking is an identification for auto increment key checking
const AutoIncrementKeyChecking = "auto-increment key checking"

// hold information of incompatibility option
type incompatibilityOption struct {
	state       State
	instruction string
	errMessage  string
}

// String returns raw text of this incompatibility option
func (o *incompatibilityOption) String() string {
	var text bytes.Buffer

	if len(o.errMessage) > 0 {
		fmt.Fprintf(&text, "information: %s\n", o.errMessage)
	}

	if len(o.instruction) > 0 {
		fmt.Fprintf(&text, "instruction: %s\n", o.instruction)
	}

	return text.String()
}

// TablesChecker checks compatibility of table structures, there are differents between MySQL and TiDB.
// In generally we need to check definitions of columns, constraints and table options.
// Because of the early TiDB engineering design, we did not have a complete list of check items, which are all based on experience now.
type TablesChecker struct {
	db     *sql.DB
	dbinfo *dbutil.DBConfig
	tables map[string][]string // schema => []table; if []table is empty, query tables from db
}

// NewTablesChecker returns a Checker
func NewTablesChecker(db *sql.DB, dbinfo *dbutil.DBConfig, tables map[string][]string) Checker {
	return &TablesChecker{
		db:     db,
		dbinfo: dbinfo,
		tables: tables,
	}
}

// Check implements Checker interface
func (c *TablesChecker) Check(ctx context.Context) *Result {
	r := &Result{
		Name:  c.Name(),
		Desc:  "check compatibility of table structure",
		State: StateSuccess,
		Extra: fmt.Sprintf("address of db instance - %s:%d", c.dbinfo.Host, c.dbinfo.Port),
	}

	var (
		err        error
		options    = make(map[string][]*incompatibilityOption)
		statements = make(map[string]string)
	)
	for schema, tables := range c.tables {
		if len(tables) == 0 {
			tables, err = dbutil.GetTables(ctx, c.db, schema)
			if err != nil {
				markCheckError(r, err)
				return r
			}
		}

		for _, table := range tables {
			tableName := dbutil.TableName(schema, table)
			statement, err := dbutil.GetCreateTableSQL(ctx, c.db, schema, table)
			if err != nil {
				markCheckError(r, err)
				return r
			}

			opts := c.checkCreateSQL(statement)
			if len(opts) > 0 {
				options[tableName] = opts
				statements[tableName] = statement
			}
		}
	}

	var information bytes.Buffer
	for name, opts := range options {
		if len(opts) == 0 {
			continue
		}

		var (
			errMessages     = make([]string, 0, len(opts))
			warningMessages = make([]string, 0, len(opts))
		)
		fmt.Fprintf(&information, "********** table %s **********\n", name)
		fmt.Fprintf(&information, "statement: %s\n", statements[name])

		for _, option := range opts {
			switch option.state {
			case StateWarning:
				if len(r.State) == 0 {
					r.State = StateWarning
				}
				warningMessages = append(warningMessages, option.String())
			case StateFailure:
				r.State = StateFailure
				errMessages = append(errMessages, option.String())
			}
		}

		if len(errMessages) > 0 {
			fmt.Fprint(&information, "---------- error options ----------\n")
			fmt.Fprintf(&information, "%s\n", strings.Join(errMessages, "\n"))
		}

		if len(warningMessages) > 0 {
			fmt.Fprint(&information, "---------- warning options ----------\n")
			fmt.Fprintf(&information, "%s\n", strings.Join(warningMessages, "\n"))
		}
	}

	r.ErrorMsg = information.String()

	return r
}

// Name implements Checker interface
func (c *TablesChecker) Name() string {
	return "table structure compatibility check"
}

func (c *TablesChecker) checkCreateSQL(statement string) []*incompatibilityOption {
	stmt, err := parser.New().ParseOneStmt(statement, "", "")
	if err != nil {
		return []*incompatibilityOption{
			{
				state:      StateFailure,
				errMessage: err.Error(),
			},
		}
	}
	// Analyze ast
	return c.checkAST(stmt)
}

func (c *TablesChecker) checkAST(stmt ast.StmtNode) []*incompatibilityOption {
	st, ok := stmt.(*ast.CreateTableStmt)
	if !ok {
		return []*incompatibilityOption{
			{
				state:      StateFailure,
				errMessage: fmt.Sprintf("Expect CreateTableStmt but got %T", stmt),
			},
		}
	}

	var options []*incompatibilityOption
	// check columns
	for _, def := range st.Cols {
		option := c.checkColumnDef(def)
		if option != nil {
			options = append(options, option)
		}
	}
	// check constrains
	for _, cst := range st.Constraints {
		option := c.checkConstraint(cst)
		if option != nil {
			options = append(options, option)
		}
	}

	// check options
	for _, opt := range st.Options {
		option := c.checkTableOption(opt)
		if option != nil {
			options = append(options, option)
		}
	}
	return options
}

func (c *TablesChecker) checkColumnDef(def *ast.ColumnDef) *incompatibilityOption {
	return nil
}

func (c *TablesChecker) checkConstraint(cst *ast.Constraint) *incompatibilityOption {
	switch cst.Tp {
	case ast.ConstraintForeignKey:
		return &incompatibilityOption{
			state:       StateWarning,
			instruction: fmt.Sprintf("please ref document: https://github.com/pingcap/docs-cn/blob/master/sql/ddl.md"),
			errMessage:  fmt.Sprintf("Foreign Key %s is parsed but ignored by TiDB.", cst.Name),
		}
	}

	return nil
}

func (c *TablesChecker) checkTableOption(opt *ast.TableOption) *incompatibilityOption {
	switch opt.Tp {
	case ast.TableOptionCharset:
		// Check charset
		cs := strings.ToLower(opt.StrValue)
		if cs != "binary" && !charset.ValidCharsetAndCollation(cs, "") {
			return &incompatibilityOption{
				state:       StateFailure,
				instruction: fmt.Sprintf("please ref document: https://github.com/pingcap/docs-cn/blob/master/sql/character-set-support.md"),
				errMessage:  fmt.Sprintf("unsupport charset %s", opt.StrValue),
			}
		}
	}
	return nil
}

// ShardingTablesCheck checks consistency of table structures of one sharding group
// * check whether they have same column list
// * check whether they have auto_increment key
type ShardingTablesCheck struct {
	name string

	dbs     map[string]*sql.DB
	tables  map[string]map[string][]string // instance => {schema: [table1, table2, ...]}
	mapping map[string]*column.Mapping
}

// NewShardingTablesCheck returns a Checker
func NewShardingTablesCheck(name string, dbs map[string]*sql.DB, tables map[string]map[string][]string, mapping map[string]*column.Mapping) Checker {
	return &ShardingTablesCheck{
		name:    name,
		dbs:     dbs,
		tables:  tables,
		mapping: mapping,
	}
}

// Check implements Checker interface
func (c *ShardingTablesCheck) Check(ctx context.Context) *Result {
	r := &Result{
		Name:  c.Name(),
		Desc:  "check consistency of sharding table structures",
		State: StateSuccess,
		Extra: fmt.Sprintf("sharding %s", c.name),
	}

	var (
		stmtNode  *ast.CreateTableStmt
		tableName string
	)
	for instance, schemas := range c.tables {
		db, ok := c.dbs[instance]
		if !ok {
			markCheckError(r, errors.NotFoundf("client for instance %s", instance))
			return r
		}

		for schema, tables := range schemas {
			for _, table := range tables {
				statement, err := dbutil.GetCreateTableSQL(ctx, db, schema, table)
				if err != nil {
					markCheckError(r, err)
					r.Extra = fmt.Sprintf("instance %s on sharding %s", instance, c.name)
					return r
				}

				info, err := dbutil.GetTableInfoBySQL(statement)
				if err != nil {
					markCheckError(r, err)
					r.Extra = fmt.Sprintf("instance %s on sharding %s", instance, c.name)
					return r
				}

				stmt, err := parser.New().ParseOneStmt(statement, "", "")
				if err != nil {
					markCheckError(r, errors.Annotatef(err, "statement %s", statement))
					r.Extra = fmt.Sprintf("instance %s on sharding %s", instance, c.name)
					return r
				}

				ctStmt, ok := stmt.(*ast.CreateTableStmt)
				if !ok {
					markCheckError(r, errors.Errorf("Expect CreateTableStmt but got %T", stmt))
					r.Extra = fmt.Sprintf("instance %s on sharding %s", instance, c.name)
					return r
				}

				passed := c.checkAutoIncrementKey(instance, schema, table, ctStmt, info, r)
				if !passed {
					return r
				}

				if stmtNode == nil {
					stmtNode = ctStmt
					tableName = dbutil.TableName(schema, table)
					continue
				}

				err = c.checkConsistency(stmtNode, ctStmt, tableName, dbutil.TableName(schema, table))
				if err != nil {
					markCheckError(r, err)
					r.Extra = fmt.Sprintf("instance %s on sharding %s", instance, c.name)
					r.Instruction = "please set same table structure for sharding tables"
					return r
				}
			}
		}
	}

	return r
}

func (c *ShardingTablesCheck) checkAutoIncrementKey(instance, schema, table string, ctStmt *ast.CreateTableStmt, info *model.TableInfo, r *Result) bool {
	autoIncrementKeys := c.findAutoIncrementKey(ctStmt, info)
	for columnName, isBigInt := range autoIncrementKeys {
		hasMatchedRule := false
		if cm, ok1 := c.mapping[instance]; ok1 {
			ruleSet := cm.Selector.Match(schema, table)
			for _, rule := range ruleSet {
				r, ok2 := rule.(*column.Rule)
				if !ok2 {
					continue
				}

				if r.Expression == column.PartitionID && r.TargetColumn == columnName {
					hasMatchedRule = true
					break
				}
			}

			if hasMatchedRule && !isBigInt {
				r.State = StateFailure
				r.ErrorMsg = fmt.Sprintf("instance %s table `%s`.`%s` of sharding %s have auto-increment key %s and column mapping, but type of %s should be bigint", instance, schema, table, c.name, columnName, columnName)
				r.Instruction = "please set auto-increment key type to bigint"
				r.Extra = AutoIncrementKeyChecking
				return false
			}
		}

		if !hasMatchedRule {
			r.State = StateFailure
			r.ErrorMsg = fmt.Sprintf("instance %s table `%s`.`%s` of sharding %s have auto-increment key, would conflict with each other to cause data corruption", instance, schema, table, c.name)
			r.Instruction = "please set column mapping rules for them, or handle it by yourself"
			r.Extra = AutoIncrementKeyChecking
			return false
		}
	}

	return true
}

func (c *ShardingTablesCheck) findAutoIncrementKey(stmt *ast.CreateTableStmt, info *model.TableInfo) map[string]bool {
	autoIncrementKeys := make(map[string]bool)
	autoIncrementCols := make(map[string]bool)

	for _, col := range stmt.Cols {
		var (
			hasAutoIncrementOpt bool
			isUnique            bool
		)
		for _, opt := range col.Options {
			switch opt.Tp {
			case ast.ColumnOptionAutoIncrement:
				hasAutoIncrementOpt = true
			case ast.ColumnOptionPrimaryKey, ast.ColumnOptionUniqKey:
				isUnique = true
			}
		}

		if hasAutoIncrementOpt {
			if isUnique {
				autoIncrementKeys[col.Name.Name.O] = col.Tp.Tp == mysql.TypeLonglong
			} else {
				autoIncrementCols[col.Name.Name.O] = col.Tp.Tp == mysql.TypeLonglong
			}
		}
	}

	for _, index := range info.Indices {
		if index.Unique || index.Primary {
			if len(index.Columns) == 1 {
				if isBigInt, ok := autoIncrementCols[index.Columns[0].Name.O]; ok {
					autoIncrementKeys[index.Columns[0].Name.O] = isBigInt
				}
			}
		}
	}

	return autoIncrementKeys
}

type briefColumnInfo struct {
	name         string
	tp           string
	isUniqueKey  bool
	isPrimaryKey bool
}

func (c *briefColumnInfo) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s %s", c.name, c.tp)
	if c.isPrimaryKey {
		fmt.Fprintln(&buf, " primary key")
	} else if c.isUniqueKey {
		fmt.Fprintln(&buf, " unique key")
	}

	return buf.String()
}

type briefColumnInfos []*briefColumnInfo

func (cs briefColumnInfos) String() string {
	var colStrs = make([]string, 0, len(cs))
	for _, col := range cs {
		colStrs = append(colStrs, col.String())
	}

	return strings.Join(colStrs, "\n")
}

func (c *ShardingTablesCheck) checkConsistency(self, other *ast.CreateTableStmt, selfName, otherName string) error {
	selfColumnList := getBriefColumnList(self)
	otherColumnList := getBriefColumnList(other)

	if len(selfColumnList) != len(otherColumnList) {
		return errors.Errorf("column length mismatch (%d vs %d)\n table %s\ncolumns %s\n\ntable%s\ncolumns %s", len(selfColumnList), len(otherColumnList), selfName, selfColumnList, otherName, otherColumnList)
	}

	for i := range selfColumnList {
		if *selfColumnList[i] != *otherColumnList[i] {
			return errors.Errorf("different column definition\ncolumn %s on table %s\ncolumn %s on table %s", selfColumnList[i], selfName, otherColumnList[i], otherName)
		}
	}

	return nil
}

func getBriefColumnList(stmt *ast.CreateTableStmt) briefColumnInfos {
	columnList := make(briefColumnInfos, 0, len(stmt.Cols))

	for _, col := range stmt.Cols {
		bc := &briefColumnInfo{
			name: col.Name.Name.L,
			tp:   col.Tp.String(),
		}

		for _, opt := range col.Options {
			switch opt.Tp {
			case ast.ColumnOptionPrimaryKey:
				bc.isPrimaryKey = true
			case ast.ColumnOptionUniqKey:
				bc.isUniqueKey = true
			}
		}

		columnList = append(columnList, bc)
	}

	return columnList
}

// Name implements Checker interface
func (c *ShardingTablesCheck) Name() string {
	return "sharding table consistency check"
}
