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
	"bytes"
	"fmt"
	"strings"

	"github.com/ngaut/log"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	_ "github.com/pingcap/tidb/types/parser_driver" // use value expression impl in TiDB
)

func defaultValueToSQL(opt *ast.ColumnOption) string {
	var value string

	switch expr := opt.Expr.(type) {
	case *ast.UnaryOperationExpr:
		var str bytes.Buffer
		expr.Format(&str)
		value = str.String()

	case *ast.FuncCallExpr:
		value = expr.FnName.O

	case ast.ValueExpr:
		datum := opt.Expr.(ast.ValueExpr)
		buf := new(bytes.Buffer)
		datum.Format(buf)
		value = buf.String()
		if len(value) > 1 && value[0] == '"' && value[len(value)-1] == '"' {
			value = formatStringValue(value[1 : len(value)-1])
		}

	default:
		log.Errorf("unhandle expression type: %v", opt.Expr)
		value = "NULL"
	}

	return fmt.Sprintf(" DEFAULT %s", value)
}

func formatStringValue(s string) string {
	if s == "" {
		return "''"
	}
	return fmt.Sprintf("'%s'", escapeSingleQuote(s))
}

// for change/modify column use only.
func fieldTypeToSQL(ft *types.FieldType) string {
	strs := []string{ft.CompactStr()}
	log.Debugf("tp %v, flag %v, flen %v, decimal %v, charset %v, collate %v, strs %v",
		ft.Tp, ft.Flag, ft.Flen, ft.Decimal, ft.Charset, ft.Collate, strs)
	if mysql.HasUnsignedFlag(ft.Flag) {
		strs = append(strs, "UNSIGNED")
	}
	if mysql.HasZerofillFlag(ft.Flag) {
		strs = append(strs, "ZEROFILL")
	}

	if mysql.HasBinaryFlag(ft.Flag) && (ft.Charset != charset.CharsetBin || (!types.IsTypeChar(ft.Tp) && !types.IsTypeBlob(ft.Tp))) {
		strs = append(strs, "BINARY")
	}

	return strings.Join(strs, " ")
}

// for add column use only. Work likes FieldType.String(), but bug free(?)
func fullFieldTypeToSQL(ft *types.FieldType) string {
	sql := fieldTypeToSQL(ft)
	strs := strings.Split(sql, " ")

	if types.IsTypeChar(ft.Tp) || types.IsTypeBlob(ft.Tp) {
		if ft.Charset != "" && ft.Charset != charset.CharsetBin {
			strs = append(strs, fmt.Sprintf("CHARACTER SET %s", ft.Charset))
		}
		if ft.Collate != "" && ft.Collate != charset.CharsetBin {
			strs = append(strs, fmt.Sprintf("COLLATE %s", ft.Collate))
		}
	}

	return strings.Join(strs, " ")
}

// FIXME: tidb's AST is error-some to handle more condition
func columnOptionsToSQL(options []*ast.ColumnOption) string {
	sql := ""
	for _, opt := range options {
		switch opt.Tp {
		case ast.ColumnOptionNotNull:
			sql += " NOT NULL"
		case ast.ColumnOptionNull:
			sql += " NULL"
		case ast.ColumnOptionDefaultValue:
			sql += defaultValueToSQL(opt)
		case ast.ColumnOptionAutoIncrement:
			sql += " AUTO_INCREMENT"
		case ast.ColumnOptionUniqKey:
			sql += " UNIQUE KEY"
		case ast.ColumnOptionPrimaryKey:
			sql += " PRIMARY KEY"
		case ast.ColumnOptionComment:
			comment := opt.Expr.(ast.ValueExpr).GetDatumString()
			comment = escapeSingleQuote(comment)
			sql += fmt.Sprintf(" COMMENT '%s'", comment)
		case ast.ColumnOptionOnUpdate: // For Timestamp and Datetime only.
			sql += " ON UPDATE CURRENT_TIMESTAMP"
		case ast.ColumnOptionFulltext:
			panic("not implemented yet")
		default:
			panic("not implemented yet")
		}
	}

	return sql
}

func escapeName(name string) string {
	return strings.Replace(name, "`", "``", -1)
}

func tableNameToSQL(tbl *ast.TableName) string {
	sql := ""
	if tbl.Schema.O != "" {
		sql += fmt.Sprintf("`%s`.", tbl.Schema.O)
	}
	sql += fmt.Sprintf("`%s`", tbl.Name.O)
	return sql
}

func columnNameToSQL(name *ast.ColumnName) string {
	sql := ""
	if name.Schema.O != "" {
		sql += fmt.Sprintf("`%s`.", escapeName(name.Schema.O))
	}
	if name.Table.O != "" {
		sql += fmt.Sprintf("`%s`.", escapeName(name.Table.O))
	}
	sql += fmt.Sprintf("`%s`", escapeName(name.Name.O))
	return sql
}

func indexColNameToSQL(name *ast.IndexColName) string {
	sql := columnNameToSQL(name.Column)
	if name.Length != types.UnspecifiedLength {
		sql += fmt.Sprintf(" (%d)", name.Length)
	}
	return sql
}

func constraintKeysToSQL(keys []*ast.IndexColName) string {
	if len(keys) == 0 {
		panic("unreachable")
	}
	sql := ""
	for i, indexColName := range keys {
		if i == 0 {
			sql += "("
		}
		sql += indexColNameToSQL(indexColName)
		if i != len(keys)-1 {
			sql += ", "
		}
	}
	sql += ")"
	return sql
}

func referenceDefToSQL(refer *ast.ReferenceDef) string {
	sql := fmt.Sprintf("%s ", tableNameToSQL(refer.Table))
	sql += constraintKeysToSQL(refer.IndexColNames)
	if refer.OnDelete != nil && refer.OnDelete.ReferOpt != ast.ReferOptionNoOption {
		sql += fmt.Sprintf(" ON DELETE %s", refer.OnDelete.ReferOpt)
	}
	if refer.OnUpdate != nil && refer.OnUpdate.ReferOpt != ast.ReferOptionNoOption {
		sql += fmt.Sprintf(" ON UPDATE %s", refer.OnUpdate.ReferOpt)
	}
	return sql
}

func indexTypeToSQL(opt *ast.IndexOption) string {
	// opt can be nil.
	if opt == nil {
		return ""
	}
	switch opt.Tp {
	case model.IndexTypeBtree:
		return "USING BTREE "
	case model.IndexTypeHash:
		return "USING HASH "
	default:
		// nothing to do
		return ""
	}
}

func constraintToSQL(constraint *ast.Constraint) string {
	sql := ""
	switch constraint.Tp {
	case ast.ConstraintKey, ast.ConstraintIndex:
		sql += "ADD INDEX "
		if constraint.Name != "" {
			sql += fmt.Sprintf("`%s` ", escapeName(constraint.Name))
		}
		sql += indexTypeToSQL(constraint.Option)
		sql += constraintKeysToSQL(constraint.Keys)
		sql += indexOptionToSQL(constraint.Option)

	case ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
		sql += "ADD CONSTRAINT "
		if constraint.Name != "" {
			sql += fmt.Sprintf("`%s` ", escapeName(constraint.Name))
		}
		sql += "UNIQUE INDEX "
		sql += indexTypeToSQL(constraint.Option)
		sql += constraintKeysToSQL(constraint.Keys)
		sql += indexOptionToSQL(constraint.Option)

	case ast.ConstraintForeignKey:
		sql += "ADD CONSTRAINT "
		if constraint.Name != "" {
			sql += fmt.Sprintf("`%s` ", escapeName(constraint.Name))
		}
		sql += "FOREIGN KEY "
		sql += constraintKeysToSQL(constraint.Keys)
		sql += " REFERENCES "
		sql += referenceDefToSQL(constraint.Refer)

	case ast.ConstraintPrimaryKey:
		sql += "ADD CONSTRAINT "
		if constraint.Name != "" {
			sql += fmt.Sprintf("`%s` ", escapeName(constraint.Name))
		}
		sql += "PRIMARY KEY "
		sql += indexTypeToSQL(constraint.Option)
		sql += constraintKeysToSQL(constraint.Keys)
		sql += indexOptionToSQL(constraint.Option)

	case ast.ConstraintFulltext:
		sql += "ADD FULLTEXT INDEX "
		if constraint.Name != "" {
			sql += fmt.Sprintf("`%s` ", escapeName(constraint.Name))
		}
		sql += constraintKeysToSQL(constraint.Keys)
		sql += indexOptionToSQL(constraint.Option)

	default:
		panic("not implemented yet")
	}
	return sql
}

func positionToSQL(pos *ast.ColumnPosition) string {
	var sql string
	switch pos.Tp {
	case ast.ColumnPositionNone:
	case ast.ColumnPositionFirst:
		sql = " FIRST"
	case ast.ColumnPositionAfter:
		colName := pos.RelativeColumn.Name.O
		sql = fmt.Sprintf(" AFTER `%s`", escapeName(colName))
	default:
		panic("unreachable")
	}
	return sql
}

// Convert constraint indexoption to sql. Currently only support comment.
func indexOptionToSQL(option *ast.IndexOption) string {
	if option == nil {
		return ""
	}

	if option.Comment != "" {
		return fmt.Sprintf(" COMMENT '%s'", escapeSingleQuote(option.Comment))
	}

	return ""
}

func tableOptionsToSQL(options []*ast.TableOption) string {
	sql := ""
	if len(options) == 0 {
		return sql
	}

	for _, opt := range options {
		switch opt.Tp {
		case ast.TableOptionEngine:
			if opt.StrValue == "" {
				sql += fmt.Sprintf(" ENGINE = ''")
			} else {
				sql += fmt.Sprintf(" ENGINE = %s", opt.StrValue)
			}

		case ast.TableOptionCharset:
			sql += fmt.Sprintf(" CHARACTER SET = %s", opt.StrValue)

		case ast.TableOptionCollate:
			sql += fmt.Sprintf(" COLLATE = %s", opt.StrValue)

		case ast.TableOptionAutoIncrement:
			sql += fmt.Sprintf(" AUTO_INCREMENT = %d", opt.UintValue)

		case ast.TableOptionComment:
			sql += fmt.Sprintf(" COMMENT '%s'", escapeSingleQuote(opt.StrValue))

		case ast.TableOptionAvgRowLength:
			sql += fmt.Sprintf(" AVG_ROW_LENGTH = %d", opt.UintValue)

		case ast.TableOptionCheckSum:
			sql += fmt.Sprintf(" CHECKSUM = %d", opt.UintValue)

		case ast.TableOptionCompression:
			// In TiDB parser.y, the rule is "COMPRESSION" EqOpt Identifier. No single quote here.
			sql += fmt.Sprintf(" COMPRESSION = '%s'", escapeSingleQuote(opt.StrValue))

		case ast.TableOptionConnection:
			sql += fmt.Sprintf(" CONNECTION = '%s'", escapeSingleQuote(opt.StrValue))

		case ast.TableOptionPassword:
			sql += fmt.Sprintf(" PASSWORD = '%s'", escapeSingleQuote(opt.StrValue))

		case ast.TableOptionKeyBlockSize:
			sql += fmt.Sprintf(" KEY_BLOCK_SIZE = %d", opt.UintValue)

		case ast.TableOptionMaxRows:
			sql += fmt.Sprintf(" MAX_ROWS = %d", opt.UintValue)

		case ast.TableOptionMinRows:
			sql += fmt.Sprintf(" MIN_ROWS = %d", opt.UintValue)

		case ast.TableOptionDelayKeyWrite:
			sql += fmt.Sprintf(" DELAY_KEY_WRITE = %d", opt.UintValue)

		case ast.TableOptionRowFormat:
			sql += fmt.Sprintf(" ROW_FORMAT = %s", formatRowFormat(opt.UintValue))

		case ast.TableOptionStatsPersistent:
			// Since TiDB doesn't support this feature, we just give a default value.
			sql += " STATS_PERSISTENT = DEFAULT"
		default:
			panic("unreachable")
		}

	}

	// trim prefix space
	sql = strings.TrimPrefix(sql, " ")
	return sql
}

func escapeSingleQuote(str string) string {
	return strings.Replace(str, "'", "''", -1)
}

func formatRowFormat(rf uint64) string {
	var s string
	switch rf {
	case ast.RowFormatDefault:
		s = "DEFAULT"
	case ast.RowFormatDynamic:
		s = "DYNAMIC"
	case ast.RowFormatFixed:
		s = "FIXED"
	case ast.RowFormatCompressed:
		s = "COMPRESSED"
	case ast.RowFormatRedundant:
		s = "REDUNDANT"
	case ast.RowFormatCompact:
		s = "COMPACT"
	default:
		panic("unreachable")
	}
	return s
}

func columnToSQL(typeDef string, newColumn *ast.ColumnDef) string {
	return fmt.Sprintf("%s %s%s", columnNameToSQL(newColumn.Name), typeDef, columnOptionsToSQL(newColumn.Options))
}

func alterTableSpecToSQL(spec *ast.AlterTableSpec, ntable *ast.TableName) []string {
	var (
		suffixes []string
		suffix   string
	)

	switch spec.Tp {
	case ast.AlterTableOption:
		suffix += tableOptionsToSQL(spec.Options)
		suffixes = append(suffixes, suffix)

	case ast.AlterTableAddColumns:
		for _, newColumn := range spec.NewColumns {
			suffix = ""
			typeDef := fullFieldTypeToSQL(newColumn.Tp)
			suffix += fmt.Sprintf("ADD COLUMN %s", columnToSQL(typeDef, newColumn))
			if spec.Position != nil {
				suffix += positionToSQL(spec.Position)
			}
			suffixes = append(suffixes, suffix)
		}

	case ast.AlterTableDropColumn:
		suffix += fmt.Sprintf("DROP COLUMN %s", columnNameToSQL(spec.OldColumnName))
		suffixes = append(suffixes, suffix)

	case ast.AlterTableDropIndex:
		suffix += fmt.Sprintf("DROP INDEX `%s`", escapeName(spec.Name))
		suffixes = append(suffixes, suffix)

	case ast.AlterTableAddConstraint:
		suffix += constraintToSQL(spec.Constraint)
		suffixes = append(suffixes, suffix)

	case ast.AlterTableDropForeignKey:
		suffix += fmt.Sprintf("DROP FOREIGN KEY `%s`", escapeName(spec.Name))
		suffixes = append(suffixes, suffix)

	case ast.AlterTableModifyColumn:
		// TiDB doesn't support alter table modify column charset and collation.
		typeDef := fieldTypeToSQL(spec.NewColumns[0].Tp)
		suffix += fmt.Sprintf("MODIFY COLUMN %s", columnToSQL(typeDef, spec.NewColumns[0]))
		if spec.Position != nil {
			suffix += positionToSQL(spec.Position)
		}
		suffixes = append(suffixes, suffix)

	// FIXME: should support [FIRST|AFTER col_name], but tidb parser not support this currently.
	case ast.AlterTableChangeColumn:
		// TiDB doesn't support alter table change column charset and collation.
		typeDef := fieldTypeToSQL(spec.NewColumns[0].Tp)
		suffix += "CHANGE COLUMN "
		suffix += fmt.Sprintf("%s %s",
			columnNameToSQL(spec.OldColumnName),
			columnToSQL(typeDef, spec.NewColumns[0]))
		if spec.Position != nil {
			suffix += positionToSQL(spec.Position)
		}
		suffixes = append(suffixes, suffix)

	case ast.AlterTableRenameTable:
		*ntable = *spec.NewTable
		suffix += fmt.Sprintf("RENAME TO %s", tableNameToSQL(spec.NewTable))
		suffixes = append(suffixes, suffix)

	case ast.AlterTableAlterColumn:
		suffix += fmt.Sprintf("ALTER COLUMN %s ", columnNameToSQL(spec.NewColumns[0].Name))
		if options := spec.NewColumns[0].Options; options != nil {
			suffix += fmt.Sprintf("SET%s", defaultValueToSQL(options[0]))
		} else {
			suffix += "DROP DEFAULT"
		}
		suffixes = append(suffixes, suffix)

	case ast.AlterTableDropPrimaryKey:
		suffix += "DROP PRIMARY KEY"
		suffixes = append(suffixes, suffix)

	case ast.AlterTableLock:
		// just ignore it
	case ast.AlterTableRenameIndex:
		suffix := fmt.Sprintf("RENAME INDEX %s TO %s", indexNameToSQL(spec.FromKey), indexNameToSQL(spec.ToKey))
		suffixes = append(suffixes, suffix)

	default:
	}
	return suffixes
}

func indexNameToSQL(name model.CIStr) string {
	return fmt.Sprintf("`%s`", escapeName(name.String()))
}

func alterTableStmtToSQL(stmt *ast.AlterTableStmt, ntable *ast.TableName) []string {
	var (
		sqls   []string
		prefix string
	)
	if ntable.Name.O != "" {
		prefix = fmt.Sprintf("ALTER TABLE %s ", tableNameToSQL(ntable))
	} else {
		prefix = fmt.Sprintf("ALTER TABLE %s ", tableNameToSQL(stmt.Table))
	}

	if len(stmt.Specs) != 1 {
		log.Warnf("[syncer] statement specs length != 1, %+v", stmt.Specs)
	}

	for _, spec := range stmt.Specs {
		log.Infof("spec %+v", spec)
		for _, suffix := range alterTableSpecToSQL(spec, ntable) {
			sqls = append(sqls, prefix+suffix)
		}
	}

	log.Debugf("alter table stmt to sql:%v", sqls)
	return sqls
}
