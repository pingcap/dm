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
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/types"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/pingcap/tidb/expression"
	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
)

// genDMLParam stores pruned columns, data as well as the original columns, data, index.
type genDMLParam struct {
	tableID           string              // as a key in map like `schema`.`table`
	originTable       *filter.Table       // origin table
	safeMode          bool                // only used in update
	data              [][]interface{}     // pruned data
	originalData      [][]interface{}     // all data
	columns           []*model.ColumnInfo // pruned columns
	originalTableInfo *model.TableInfo    // all table info
}

func extractValueFromData(data []interface{}, columns []*model.ColumnInfo) []interface{} {
	value := make([]interface{}, 0, len(data))
	for i := range data {
		value = append(value, castUnsigned(data[i], &columns[i].FieldType))
	}
	return value
}

func (s *Syncer) genAndFilterInsertdmls(param *genDMLParam, filterExprs []expression.Expression) ([]*DML, error) {
	var (
		dataSeq         = param.data
		originalDataSeq = param.originalData
		columns         = param.columns
		ti              = param.originalTableInfo
		dmls            = make([]*DML, 0, len(dataSeq))
	)

RowLoop:
	for dataIdx, data := range dataSeq {
		if len(data) != len(columns) {
			return nil, terror.ErrSyncerUnitDMLColumnNotMatch.Generate(len(columns), len(data))
		}

		value := extractValueFromData(data, columns)
		originalValue := value
		if len(columns) != len(ti.Columns) {
			originalValue = extractValueFromData(originalDataSeq[dataIdx], ti.Columns)
		}

		for _, expr := range filterExprs {
			skip, err := SkipDMLByExpression(originalValue, expr, ti.Columns)
			if err != nil {
				return nil, err
			}
			if skip {
				s.filteredInsert.Add(1)
				continue RowLoop
			}
		}

		dmls = append(dmls, newDML(insert, param.safeMode, param.tableID, param.originTable, nil, value, nil, originalValue, columns, ti))
	}

	return dmls, nil
}

func (s *Syncer) genAndFilterUpdatedmls(
	param *genDMLParam,
	oldValueFilters []expression.Expression,
	newValueFilters []expression.Expression,
) ([]*DML, error) {
	var (
		data         = param.data
		originalData = param.originalData
		columns      = param.columns
		ti           = param.originalTableInfo
		dmls         = make([]*DML, 0, len(data)/2)
	)

RowLoop:
	for i := 0; i < len(data); i += 2 {
		oldData := data[i]
		changedData := data[i+1]
		oriOldData := originalData[i]
		oriChangedData := originalData[i+1]

		if len(oldData) != len(changedData) {
			return nil, terror.ErrSyncerUnitDMLOldNewValueMismatch.Generate(len(oldData), len(changedData))
		}

		if len(oldData) != len(columns) {
			return nil, terror.ErrSyncerUnitDMLColumnNotMatch.Generate(len(columns), len(oldData))
		}

		oldValues := extractValueFromData(oldData, columns)
		changedValues := extractValueFromData(changedData, columns)

		var oriOldValues, oriChangedValues []interface{}
		if len(columns) == len(ti.Columns) {
			oriOldValues = oldValues
			oriChangedValues = changedValues
		} else {
			oriOldValues = extractValueFromData(oriOldData, ti.Columns)
			oriChangedValues = extractValueFromData(oriChangedData, ti.Columns)
		}

		for j := range oldValueFilters {
			// AND logic
			oldExpr, newExpr := oldValueFilters[j], newValueFilters[j]
			skip1, err := SkipDMLByExpression(oriOldValues, oldExpr, ti.Columns)
			if err != nil {
				return nil, err
			}
			skip2, err := SkipDMLByExpression(oriChangedValues, newExpr, ti.Columns)
			if err != nil {
				return nil, err
			}
			if skip1 && skip2 {
				s.filteredUpdate.Add(1)
				// TODO: we skip generating the UPDATE SQL, so we left the old value here. Is this expected?
				continue RowLoop
			}
		}

		dmls = append(dmls, newDML(update, param.safeMode, param.tableID, param.originTable, oldValues, changedValues, oriOldValues, oriChangedValues, columns, ti))
	}

	return dmls, nil
}

func (s *Syncer) genAndFilterDeletedmls(param *genDMLParam, filterExprs []expression.Expression) ([]*DML, error) {
	var (
		dataSeq = param.originalData
		ti      = param.originalTableInfo
		dmls    = make([]*DML, 0, len(dataSeq))
	)

RowLoop:
	for _, data := range dataSeq {
		if len(data) != len(ti.Columns) {
			return nil, terror.ErrSyncerUnitDMLColumnNotMatch.Generate(len(ti.Columns), len(data))
		}

		value := extractValueFromData(data, ti.Columns)

		for _, expr := range filterExprs {
			skip, err := SkipDMLByExpression(value, expr, ti.Columns)
			if err != nil {
				return nil, err
			}
			if skip {
				s.filteredDelete.Add(1)
				continue RowLoop
			}
		}
		dmls = append(dmls, newDML(del, false, param.tableID, param.originTable, nil, value, nil, value, ti.Columns, ti))
	}

	return dmls, nil
}

func castUnsigned(data interface{}, ft *types.FieldType) interface{} {
	if !mysql.HasUnsignedFlag(ft.Flag) {
		return data
	}

	switch v := data.(type) {
	case int:
		return uint(v)
	case int8:
		return uint8(v)
	case int16:
		return uint16(v)
	case int32:
		if ft.Tp == mysql.TypeInt24 {
			// we use int32 to store MEDIUMINT, if the value is signed, it's fine
			// but if the value is un-signed, simply convert it use `uint32` may out of the range
			// like -4692783 converted to 4290274513 (2^32 - 4692783), but we expect 12084433 (2^24 - 4692783)
			data := make([]byte, 4)
			binary.LittleEndian.PutUint32(data, uint32(v))
			return uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16
		}
		return uint32(v)
	case int64:
		return uint64(v)
	}

	return data
}

func columnValue(value interface{}, ft *types.FieldType) string {
	castValue := castUnsigned(value, ft)

	var data string
	switch v := castValue.(type) {
	case nil:
		data = "null"
	case bool:
		if v {
			data = "1"
		} else {
			data = "0"
		}
	case int:
		data = strconv.FormatInt(int64(v), 10)
	case int8:
		data = strconv.FormatInt(int64(v), 10)
	case int16:
		data = strconv.FormatInt(int64(v), 10)
	case int32:
		data = strconv.FormatInt(int64(v), 10)
	case int64:
		data = strconv.FormatInt(v, 10)
	case uint8:
		data = strconv.FormatUint(uint64(v), 10)
	case uint16:
		data = strconv.FormatUint(uint64(v), 10)
	case uint32:
		data = strconv.FormatUint(uint64(v), 10)
	case uint64:
		data = strconv.FormatUint(v, 10)
	case float32:
		data = strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		data = strconv.FormatFloat(v, 'f', -1, 64)
	case string:
		data = v
	case []byte:
		data = string(v)
	default:
		data = fmt.Sprintf("%v", v)
	}

	return data
}

func findFitIndex(ti *model.TableInfo) *model.IndexInfo {
	for _, idx := range ti.Indices {
		if idx.Primary {
			return idx
		}
	}

	if pk := ti.GetPkColInfo(); pk != nil {
		return &model.IndexInfo{
			Table:   ti.Name,
			Unique:  true,
			Primary: true,
			State:   model.StatePublic,
			Tp:      model.IndexTypeBtree,
			Columns: []*model.IndexColumn{{
				Name:   pk.Name,
				Offset: pk.Offset,
				Length: types.UnspecifiedLength,
			}},
		}
	}

	// second find not null unique key
	fn := func(i int) bool {
		return !mysql.HasNotNullFlag(ti.Columns[i].Flag)
	}

	return getSpecifiedIndexColumn(ti, fn)
}

func getAvailableIndexColumn(ti *model.TableInfo, data []interface{}) *model.IndexInfo {
	fn := func(i int) bool {
		return data[i] == nil
	}

	return getSpecifiedIndexColumn(ti, fn)
}

func getSpecifiedIndexColumn(ti *model.TableInfo, fn func(i int) bool) *model.IndexInfo {
	for _, indexCols := range ti.Indices {
		if !indexCols.Unique {
			continue
		}

		findFitIndex := true
		for _, col := range indexCols.Columns {
			if fn(col.Offset) {
				findFitIndex = false
				break
			}
		}

		if findFitIndex {
			return indexCols
		}
	}

	return nil
}

func getColumnData(columns []*model.ColumnInfo, indexColumns *model.IndexInfo, data []interface{}) ([]*model.ColumnInfo, []interface{}) {
	cols := make([]*model.ColumnInfo, 0, len(indexColumns.Columns))
	values := make([]interface{}, 0, len(indexColumns.Columns))
	for _, column := range indexColumns.Columns {
		cols = append(cols, columns[column.Offset])
		values = append(values, data[column.Offset])
	}

	return cols, values
}

func (s *Syncer) mappingDML(table *filter.Table, ti *model.TableInfo, data [][]interface{}) ([][]interface{}, error) {
	if s.columnMapping == nil {
		return data, nil
	}

	columns := make([]string, 0, len(ti.Columns))
	for _, col := range ti.Columns {
		columns = append(columns, col.Name.O)
	}

	var (
		err  error
		rows = make([][]interface{}, len(data))
	)
	for i := range data {
		rows[i], _, err = s.columnMapping.HandleRowValue(table.Schema, table.Name, columns, data[i])
		if err != nil {
			return nil, terror.ErrSyncerUnitDoColumnMapping.Delegate(err, data[i], table)
		}
	}
	return rows, nil
}

// pruneGeneratedColumnDML filters columns list, data and index removing all
// generated column. because generated column is not support setting value
// directly in DML, we must remove generated column from DML, including column
// list and data list including generated columns.
func pruneGeneratedColumnDML(ti *model.TableInfo, data [][]interface{}) ([]*model.ColumnInfo, [][]interface{}, error) {
	// search for generated columns. if none found, return everything as-is.
	firstGeneratedColumnIndex := -1
	for i, c := range ti.Columns {
		if c.IsGenerated() {
			firstGeneratedColumnIndex = i
			break
		}
	}
	if firstGeneratedColumnIndex < 0 {
		return ti.Columns, data, nil
	}

	// remove generated columns from the list of columns
	cols := make([]*model.ColumnInfo, 0, len(ti.Columns))
	cols = append(cols, ti.Columns[:firstGeneratedColumnIndex]...)
	for _, c := range ti.Columns[(firstGeneratedColumnIndex + 1):] {
		if !c.IsGenerated() {
			cols = append(cols, c)
		}
	}

	// remove generated columns from the list of data.
	rows := make([][]interface{}, 0, len(data))
	for _, row := range data {
		if len(row) != len(ti.Columns) {
			return nil, nil, terror.ErrSyncerUnitDMLPruneColumnMismatch.Generate(len(ti.Columns), len(data))
		}
		value := make([]interface{}, 0, len(cols))
		for i := range row {
			if !ti.Columns[i].IsGenerated() {
				value = append(value, row[i])
			}
		}
		rows = append(rows, value)
	}
	return cols, rows, nil
}

// checkLogColumns returns error when not all rows in skipped is empty, which means the binlog doesn't contain all
// columns.
// TODO: don't return error when all skipped columns is non-PK.
func checkLogColumns(skipped [][]int) error {
	for _, row := range skipped {
		if len(row) > 0 {
			return terror.ErrBinlogNotLogColumn
		}
	}
	return nil
}

// DML stores param for DML.
type DML struct {
	tableID         string
	originTable     *filter.Table
	op              opType
	oldValues       []interface{} // only for update SQL
	values          []interface{}
	columns         []*model.ColumnInfo
	ti              *model.TableInfo
	originOldValues []interface{} // use to gen `WHERE` for update SQL
	originValues    []interface{} // use to gen `WHERE` for delete SQL
	safeMode        bool
	key             string // use to detect causality
}

// newDML creates DML.
func newDML(op opType, safeMode bool, tableID string, originTable *filter.Table, oldValues, values, originOldValues, originValues []interface{}, columns []*model.ColumnInfo, ti *model.TableInfo) *DML {
	return &DML{
		op:              op,
		safeMode:        safeMode,
		tableID:         tableID,
		originTable:     originTable,
		oldValues:       oldValues,
		values:          values,
		columns:         columns,
		ti:              ti,
		originOldValues: originOldValues,
		originValues:    originValues,
	}
}

// newDelDML generate delete DML from origin update dml.
func (dml *DML) newDelDML() *DML {
	clone := &DML{}
	*clone = *dml
	clone.values = dml.values
	clone.originValues = dml.originOldValues
	clone.oldValues = nil
	clone.originOldValues = nil
	clone.op = del
	return clone
}

// newInsertDML generate insert DML from origin update dml.
func (dml *DML) newInsertDML() *DML {
	clone := &DML{}
	*clone = *dml
	clone.oldValues = nil
	clone.originOldValues = nil
	clone.op = insert
	return clone
}

// columnNames return column names of DML.
func (dml *DML) columnNames() []string {
	columnNames := make([]string, 0, len(dml.columns))
	for _, column := range dml.columns {
		columnNames = append(columnNames, column.Name.O)
	}
	return columnNames
}

// identifyColumns gets columns of unique not null index.
func (dml *DML) identifyColumns() []string {
	if defaultIndexColumns := findFitIndex(dml.ti); defaultIndexColumns != nil {
		columns := make([]string, 0, len(defaultIndexColumns.Columns))
		for _, column := range defaultIndexColumns.Columns {
			columns = append(columns, column.Name.O)
		}
		return columns
	}
	return nil
}

// identifyValues gets values of unique not null index.
func (dml *DML) identifyValues() []interface{} {
	if defaultIndexColumns := findFitIndex(dml.ti); defaultIndexColumns != nil {
		values := make([]interface{}, 0, len(defaultIndexColumns.Columns))
		for _, column := range defaultIndexColumns.Columns {
			values = append(values, dml.values[column.Offset])
		}
		return values
	}
	return nil
}

// oldIdentifyValues gets old values of unique not null index.
// only for update SQL.
func (dml *DML) oldIdentifyValues() []interface{} {
	if defaultIndexColumns := findFitIndex(dml.ti); defaultIndexColumns != nil {
		values := make([]interface{}, 0, len(defaultIndexColumns.Columns))
		for _, column := range defaultIndexColumns.Columns {
			values = append(values, dml.oldValues[column.Offset])
		}
		return values
	}
	return nil
}

// identifyKey use identifyValues to gen key.
// This is used for compacted.
// PK or (UK + NOT NULL).
func (dml *DML) identifyKey() string {
	return genKey(dml.identifyValues())
}

// identifyKeys gens keys by unique not null value.
// This is used for causality.
// PK or (UK + NOT NULL) or (UK + NULL + NOT NULL VALUE).
func (dml *DML) identifyKeys() []string {
	var keys []string
	if dml.originOldValues != nil {
		keys = append(keys, genMultipleKeys(dml.ti, dml.originOldValues, dml.tableID)...)
	}
	if dml.originValues != nil {
		keys = append(keys, genMultipleKeys(dml.ti, dml.originValues, dml.tableID)...)
	}
	return keys
}

// whereColumnsAndValues gets columns and values of unique column with not null value.
func (dml *DML) whereColumnsAndValues() ([]string, []interface{}) {
	columns, values := dml.ti.Columns, dml.originValues

	if dml.op == update {
		values = dml.originOldValues
	}

	defaultIndexColumns := findFitIndex(dml.ti)

	if defaultIndexColumns == nil {
		defaultIndexColumns = getAvailableIndexColumn(dml.ti, values)
	}
	if defaultIndexColumns != nil {
		columns, values = getColumnData(dml.ti.Columns, defaultIndexColumns, values)
	}

	columnNames := make([]string, 0, len(columns))
	for _, column := range columns {
		columnNames = append(columnNames, column.Name.O)
	}
	return columnNames, values
}

// genKey gens key by values e.g. "a.1.b".
func genKey(values []interface{}) string {
	builder := new(strings.Builder)
	for i, v := range values {
		if i != 0 {
			builder.WriteString(".")
		}
		fmt.Fprintf(builder, "%v", v)
	}

	return builder.String()
}

func genKeyList(table string, columns []*model.ColumnInfo, dataSeq []interface{}) string {
	var buf strings.Builder
	for i, data := range dataSeq {
		if data == nil {
			log.L().Debug("ignore null value", zap.String("column", columns[i].Name.O), zap.String("table", table))
			continue // ignore `null` value.
		}
		// one column key looks like:`column_val.column_name.`
		buf.WriteString(columnValue(data, &columns[i].FieldType))
		buf.WriteString(".")
		buf.WriteString(columns[i].Name.O)
		buf.WriteString(".")
	}
	if buf.Len() == 0 {
		log.L().Debug("all value are nil, no key generated", zap.String("table", table))
		return "" // all values are `null`.
	}
	buf.WriteString(table)
	return buf.String()
}

func genMultipleKeys(ti *model.TableInfo, value []interface{}, table string) []string {
	multipleKeys := make([]string, 0, len(ti.Indices)+1)
	if ti.PKIsHandle {
		if pk := ti.GetPkColInfo(); pk != nil {
			cols := []*model.ColumnInfo{pk}
			vals := []interface{}{value[pk.Offset]}
			multipleKeys = append(multipleKeys, genKeyList(table, cols, vals))
		}
	}

	for _, indexCols := range ti.Indices {
		// PK also has a true Unique
		if !indexCols.Unique {
			continue
		}
		cols, vals := getColumnData(ti.Columns, indexCols, value)
		key := genKeyList(table, cols, vals)
		if len(key) > 0 { // ignore `null` value.
			multipleKeys = append(multipleKeys, key)
			// TODO: break here? one unique index is enough?
		} else {
			log.L().Debug("ignore empty key", zap.String("table", table))
		}
	}

	if len(multipleKeys) == 0 {
		// use table name as key if no key generated (no PK/UK),
		// no concurrence for rows in the same table.
		log.L().Debug("use table name as the key", zap.String("table", table))
		multipleKeys = append(multipleKeys, table)
	}

	return multipleKeys
}

// updateIdentify check whether a update sql update its identify values.
func (dml *DML) updateIdentify() bool {
	if len(dml.oldValues) == 0 {
		return false
	}

	values := dml.identifyValues()
	oldValues := dml.oldIdentifyValues()

	for i := 0; i < len(values); i++ {
		if values[i] != oldValues[i] {
			return true
		}
	}

	return false
}

func (dml *DML) genWhere(buf *strings.Builder) []interface{} {
	whereColumns, whereValues := dml.whereColumnsAndValues()

	for i, col := range whereColumns {
		if i != 0 {
			buf.WriteString(" AND ")
		}
		buf.WriteByte('`')
		buf.WriteString(strings.ReplaceAll(col, "`", "``"))
		if whereValues[i] == nil {
			buf.WriteString("` IS ?")
		} else {
			buf.WriteString("` = ?")
		}
	}
	return whereValues
}

// genDeleteSQL generates a `UPDATE` SQL with `WHERE`.
func (dml *DML) genUpdateSQL() ([]string, [][]interface{}) {
	if dml.safeMode {
		sqls, args := dml.genDeleteSQL()
		replaceSQLs, replaceArgs := dml.genInsertReplaceSQL()
		sqls = append(sqls, replaceSQLs...)
		args = append(args, replaceArgs...)
		return sqls, args
	}
	var buf strings.Builder
	buf.Grow(2048)
	buf.WriteString("UPDATE ")
	buf.WriteString(dml.tableID)
	buf.WriteString(" SET ")

	for i, column := range dml.columns {
		if i == len(dml.columns)-1 {
			fmt.Fprintf(&buf, "`%s` = ?", strings.ReplaceAll(column.Name.O, "`", "``"))
		} else {
			fmt.Fprintf(&buf, "`%s` = ?, ", strings.ReplaceAll(column.Name.O, "`", "``"))
		}
	}

	buf.WriteString(" WHERE ")
	whereArgs := dml.genWhere(&buf)
	buf.WriteString(" LIMIT 1")

	args := dml.values
	args = append(args, whereArgs...)
	return []string{buf.String()}, [][]interface{}{args}
}

func (dml *DML) genSQL() (sql []string, arg [][]interface{}) {
	switch dml.op {
	case insert:
		return dml.genInsertReplaceSQL()
	case del:
		return dml.genDeleteSQL()
	case update:
		return dml.genUpdateSQL()
	}
	return
}

// genDeleteSQL generates a `DELETE FROM` SQL with `WHERE`.
func (dml *DML) genDeleteSQL() ([]string, [][]interface{}) {
	var buf strings.Builder
	buf.Grow(1024)
	buf.WriteString("DELETE FROM ")
	buf.WriteString(dml.tableID)
	buf.WriteString(" WHERE ")
	whereArgs := dml.genWhere(&buf)
	buf.WriteString(" LIMIT 1")

	return []string{buf.String()}, [][]interface{}{whereArgs}
}

func (dml *DML) genInsertReplaceSQL() ([]string, [][]interface{}) {
	var buf strings.Builder
	buf.Grow(256)
	if dml.safeMode {
		buf.WriteString("REPLACE INTO")
	} else {
		buf.WriteString("INSERT INTO")
	}

	buf.WriteString(" " + dml.tableID + " (")
	for i, column := range dml.columns {
		if i != len(dml.columns)-1 {
			buf.WriteString("`" + strings.ReplaceAll(column.Name.O, "`", "``") + "`,")
		} else {
			buf.WriteString("`" + strings.ReplaceAll(column.Name.O, "`", "``") + "`)")
		}
	}
	buf.WriteString(" VALUES (")

	// placeholders
	for i := range dml.columns {
		if i != len(dml.columns)-1 {
			buf.WriteString("?,")
		} else {
			buf.WriteString("?)")
		}
	}
	return []string{buf.String()}, [][]interface{}{dml.values}
}

// valuesHolder gens values holder like (?,?,?).
func valuesHolder(n int) string {
	builder := new(strings.Builder)
	builder.WriteByte('(')
	for i := 0; i < n; i++ {
		if i > 0 {
			builder.WriteString(",")
		}
		builder.WriteString("?")
	}
	builder.WriteByte(')')
	return builder.String()
}

// genMultipleRowsInsertRelace gens insert/replace multiple rows statement.
func genMultipleRowsInsertReplace(replace bool, dmls []*DML) ([]string, [][]interface{}) {
	if len(dmls) == 0 {
		return nil, nil
	}

	var buf strings.Builder
	buf.Grow(256)
	if replace {
		buf.WriteString("REPLACE INTO")
	} else {
		buf.WriteString("INSERT INTO")
	}
	buf.WriteString(" " + dmls[0].tableID + " (")
	for i, column := range dmls[0].columns {
		if i != len(dmls[0].columns)-1 {
			buf.WriteString("`" + strings.ReplaceAll(column.Name.O, "`", "``") + "`,")
		} else {
			buf.WriteString("`" + strings.ReplaceAll(column.Name.O, "`", "``") + "`)")
		}
	}
	buf.WriteString(" VALUES ")

	holder := valuesHolder(len(dmls[0].columns))
	for i := range dmls {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(holder)
	}

	args := make([]interface{}, 0, len(dmls)*len(dmls[0].columns))
	for _, dml := range dmls {
		args = append(args, dml.values...)
	}
	return []string{buf.String()}, [][]interface{}{args}
}

// genMultipleRowsInsert gens insert statement like 'INSERT INTO tb(a,b) VALUES(1,1),(2,2);'.
func genMultipleRowsInsert(dmls []*DML) ([]string, [][]interface{}) {
	if len(dmls) == 0 {
		return nil, nil
	}
	return genMultipleRowsInsertReplace(dmls[0].safeMode, dmls)
}

// genMultipleRowsReplace gens insert statement like 'REPLACE INTO tb(a,b) VALUES(1,1),(2,2);'.
func genMultipleRowsReplace(dmls []*DML) ([]string, [][]interface{}) {
	return genMultipleRowsInsertReplace(true, dmls)
}

// genMultipleRowsDelete gens delete statement like 'DELTE FROM tb WHERE (a,b) IN (1,1),(2,2);'.
func genMultipleRowsDelete(dmls []*DML) ([]string, [][]interface{}) {
	if len(dmls) == 0 {
		return nil, nil
	}

	var buf strings.Builder
	buf.Grow(1024)
	buf.WriteString("DELETE FROM ")
	buf.WriteString(dmls[0].tableID)
	buf.WriteString(" WHERE (")

	whereColumns, _ := dmls[0].whereColumnsAndValues()
	for i, column := range whereColumns {
		if i != len(whereColumns)-1 {
			buf.WriteString("`" + strings.ReplaceAll(column, "`", "``") + "`,")
		} else {
			buf.WriteString("`" + strings.ReplaceAll(column, "`", "``") + "`)")
		}
	}
	buf.WriteString(" IN (")

	holder := valuesHolder(len(whereColumns))
	args := make([]interface{}, 0, len(dmls)*len(dmls[0].columns))
	for i, dml := range dmls {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(holder)
		_, whereValues := dml.whereColumnsAndValues()
		args = append(args, whereValues...)
	}
	buf.WriteString(")")
	return []string{buf.String()}, [][]interface{}{args}
}

// genMultipleRows gens multiple rows SQL with different opType.
func genMultipleRows(op opType, dmls []*DML) (queries []string, args [][]interface{}) {
	switch op {
	case insert:
		return genMultipleRowsInsert(dmls)
	case replace:
		return genMultipleRowsReplace(dmls)
	case del:
		return genMultipleRowsDelete(dmls)
	}
	return
}

// sameColumns check whether two DML have same columns.
func sameColumns(lhs *DML, rhs *DML) bool {
	if lhs.originTable.Schema == rhs.originTable.Schema && lhs.originTable.Name == rhs.originTable.Name {
		return true
	}

	var lhsCols, rhsCols []string
	if lhs.op == del {
		lhsCols, _ = lhs.whereColumnsAndValues()
		rhsCols, _ = rhs.whereColumnsAndValues()
	} else {
		lhsCols = lhs.columnNames()
		rhsCols = rhs.columnNames()
	}
	if len(lhsCols) != len(rhsCols) {
		return false
	}
	for i := 0; i < len(lhsCols); i++ {
		if lhsCols[i] != rhsCols[i] {
			return false
		}
	}
	return true
}

// gendmlsWithSameCols group and gen dmls by same columns.
// in optimistic shard mode, different upstream tables may have different columns.
// e.g.
// insert into tb(a,b,c) values(1,1,1)
// insert into tb(a,b,d) values(2,2,2)
// we can only combine DMLs with same column names.
// all dmls should have same opType and same tableName.
func gendmlsWithSameCols(op opType, dmls []*DML) ([]string, [][]interface{}) {
	queries := make([]string, 0, len(dmls))
	args := make([][]interface{}, 0, len(dmls))
	var lastdmls *DML
	var query []string
	var arg [][]interface{}
	groupdmls := make([]*DML, 0, len(dmls))

	// group dmls by same columns
	for i, dml := range dmls {
		if i == 0 {
			lastdmls = dml
			groupdmls = append(groupdmls, dml)
			continue
		}

		if sameColumns(lastdmls, dml) {
			groupdmls = append(groupdmls, dml)
		} else {
			query, arg = genMultipleRows(op, groupdmls)
			queries = append(queries, query...)
			args = append(args, arg...)

			groupdmls = groupdmls[0:0]
			lastdmls = dml
			groupdmls = append(groupdmls, dml)
		}
	}
	if len(groupdmls) > 0 {
		switch op {
		case insert:
			query, arg = genMultipleRowsInsert(groupdmls)
		case replace:
			query, arg = genMultipleRowsReplace(groupdmls)
		case del:
			query, arg = genMultipleRowsDelete(groupdmls)
		}
		queries = append(queries, query...)
		args = append(args, arg...)
	}
	return queries, args
}

// genSQLsWithSameTable group and gen dmls with same table.
// all the dmls should have same opType.
func gendmlsWithSameTable(op opType, dmls []*DML) ([]string, [][]interface{}) {
	queries := make([]string, 0, len(dmls))
	args := make([][]interface{}, 0, len(dmls))
	var lastTable string
	groupdmls := make([]*DML, 0, len(dmls))

	// for update statement, generate SQLs one by one
	if op == update {
		for _, dml := range dmls {
			query, arg := dml.genUpdateSQL()
			queries = append(queries, query...)
			args = append(args, arg...)
		}
		return queries, args
	}

	// group dmls with same table
	for i, dml := range dmls {
		if i == 0 {
			lastTable = dml.tableID
		}
		if lastTable == dml.tableID {
			groupdmls = append(groupdmls, dml)
		} else {
			query, arg := gendmlsWithSameCols(op, groupdmls)
			queries = append(queries, query...)
			args = append(args, arg...)

			groupdmls = groupdmls[0:0]
			lastTable = dml.tableID
			groupdmls = append(groupdmls, dml)
		}
	}
	if len(groupdmls) > 0 {
		query, arg := gendmlsWithSameCols(op, groupdmls)
		queries = append(queries, query...)
		args = append(args, arg...)
	}
	return queries, args
}

// gendmls group and gen dmls by opType.
// only used for causality jobs.
func gendmlsWithSameTp(dmls []*DML) ([]string, [][]interface{}) {
	queries := make([]string, 0, len(dmls))
	args := make([][]interface{}, 0, len(dmls))
	var lastTp opType
	groupdmls := make([]*DML, 0, len(dmls))

	// group dmls with same opType
	for i, dml := range dmls {
		// if update statement didn't update primary values, regard it as replace
		// if insert statement in safeMode, regard it as replace
		if dml.op == update && !dml.updateIdentify() || dml.op == insert && dml.safeMode {
			dml.op = replace
		}
		if i == 0 {
			lastTp = dml.op
		}
		// now there are 4 situation: [insert, replace, update, delete]
		if lastTp == dml.op {
			groupdmls = append(groupdmls, dml)
		} else {
			query, arg := gendmlsWithSameTable(lastTp, groupdmls)
			queries = append(queries, query...)
			args = append(args, arg...)

			groupdmls = groupdmls[0:0]
			lastTp = dml.op
			groupdmls = append(groupdmls, dml)
		}
	}
	if len(groupdmls) > 0 {
		query, arg := gendmlsWithSameTable(lastTp, groupdmls)
		queries = append(queries, query...)
		args = append(args, arg...)
	}
	return queries, args
}
