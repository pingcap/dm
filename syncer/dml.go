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

func (s *Syncer) genAndFilterInsertDMLParams(param *genDMLParam, filterExprs []expression.Expression) ([]*DMLParam, error) {
	var (
		dataSeq         = param.data
		originalDataSeq = param.originalData
		columns         = param.columns
		ti              = param.originalTableInfo
		dmlParams       = make([]*DMLParam, 0, len(dataSeq))
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

		dmlParams = append(dmlParams, newDMLParam(insert, param.safeMode, param.tableID, nil, value, nil, originalValue, columns, ti))
	}

	return dmlParams, nil
}

func (s *Syncer) genAndFilterUpdateDMLParams(
	param *genDMLParam,
	oldValueFilters []expression.Expression,
	newValueFilters []expression.Expression,
) ([]*DMLParam, error) {
	var (
		data         = param.data
		originalData = param.originalData
		columns      = param.columns
		ti           = param.originalTableInfo
		dmlParams    = make([]*DMLParam, 0, len(data)/2)
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

		dmlParams = append(dmlParams, newDMLParam(update, param.safeMode, param.tableID, oldValues, changedValues, oriOldValues, oriChangedValues, columns, ti))
	}

	return dmlParams, nil
}

func (s *Syncer) genAndFilterDeleteDMLParams(param *genDMLParam, filterExprs []expression.Expression) ([]*DMLParam, error) {
	var (
		dataSeq   = param.originalData
		ti        = param.originalTableInfo
		dmlParams = make([]*DMLParam, 0, len(dataSeq))
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
		dmlParams = append(dmlParams, newDMLParam(del, param.safeMode, param.tableID, nil, value, nil, value, ti.Columns, ti))
	}

	return dmlParams, nil
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

// DMLParam stores param for DML.
type DMLParam struct {
	tableID         string
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

// newDMLParam creates DMLParam.
func newDMLParam(op opType, safeMode bool, tableID string, oldValues, values, originOldValues, originValues []interface{}, columns []*model.ColumnInfo, ti *model.TableInfo) *DMLParam {
	return &DMLParam{
		op:              op,
		safeMode:        safeMode,
		tableID:         tableID,
		oldValues:       oldValues,
		values:          values,
		columns:         columns,
		ti:              ti,
		originOldValues: originOldValues,
		originValues:    originValues,
	}
}

// newDelDMLParam generate delete DMLParam from origin dmlParam.
func (dmlParam *DMLParam) newDelDMLParam() *DMLParam {
	return &DMLParam{
		op:              del,
		safeMode:        dmlParam.safeMode,
		tableID:         dmlParam.tableID,
		oldValues:       nil,
		values:          dmlParam.oldValues,
		columns:         dmlParam.columns,
		ti:              dmlParam.ti,
		originOldValues: nil,
		originValues:    dmlParam.originOldValues,
	}
}

// newInsertDMLParam generate insert DMLParam from origin dmlParam.
func (dmlParam *DMLParam) newInsertDMLParam() *DMLParam {
	return &DMLParam{
		op:              dmlParam.op,
		safeMode:        dmlParam.safeMode,
		tableID:         dmlParam.tableID,
		oldValues:       nil,
		values:          dmlParam.values,
		columns:         dmlParam.columns,
		ti:              dmlParam.ti,
		originOldValues: nil,
		originValues:    dmlParam.originValues,
	}
}

// identifyColumns gets columns of unique not null index.
func (dmlParam *DMLParam) identifyColumns() []string {
	if defaultIndexColumns := findFitIndex(dmlParam.ti); defaultIndexColumns != nil {
		columns := make([]string, 0, len(defaultIndexColumns.Columns))
		for _, column := range defaultIndexColumns.Columns {
			columns = append(columns, column.Name.O)
		}
		return columns
	}
	return nil
}

// identifyValues gets values of unique not null index.
func (dmlParam *DMLParam) identifyValues() []interface{} {
	if defaultIndexColumns := findFitIndex(dmlParam.ti); defaultIndexColumns != nil {
		values := make([]interface{}, 0, len(defaultIndexColumns.Columns))
		for _, column := range defaultIndexColumns.Columns {
			values = append(values, dmlParam.values[column.Offset])
		}
		return values
	}
	return nil
}

// oldIdentifyValues gets old values of unique not null index.
// only for update SQL.
func (dmlParam *DMLParam) oldIdentifyValues() []interface{} {
	if defaultIndexColumns := findFitIndex(dmlParam.ti); defaultIndexColumns != nil {
		values := make([]interface{}, 0, len(defaultIndexColumns.Columns))
		for _, column := range defaultIndexColumns.Columns {
			values = append(values, dmlParam.oldValues[column.Offset])
		}
		return values
	}
	return nil
}

// identifyKey use identifyValues to gen key.
// This is used for compacted.
// PK or (UK + NOT NULL).
func (dmlParam *DMLParam) identifyKey() string {
	return genKey(dmlParam.identifyValues())
}

// identifyKeys gens keys by unique not null value.
// This is used for causality.
// PK or (UK + NOT NULL) or (UK + NULL + NOT NULL VALUE).
func (dmlParam *DMLParam) identifyKeys() []string {
	var keys []string
	if dmlParam.originOldValues != nil {
		keys = append(keys, genMultipleKeys(dmlParam.ti, dmlParam.originOldValues, dmlParam.tableID)...)
	}
	if dmlParam.originValues != nil {
		keys = append(keys, genMultipleKeys(dmlParam.ti, dmlParam.originValues, dmlParam.tableID)...)
	}
	return keys
}

// whereColumnsAndValues gets columns and values of unique column with not null value.
func (dmlParam *DMLParam) whereColumnsAndValues() ([]string, []interface{}) {
	columns, values := dmlParam.ti.Columns, dmlParam.originValues

	if dmlParam.op == update {
		values = dmlParam.originOldValues
	}

	defaultIndexColumns := findFitIndex(dmlParam.ti)

	if defaultIndexColumns == nil {
		defaultIndexColumns = getAvailableIndexColumn(dmlParam.ti, values)
	}
	if defaultIndexColumns != nil {
		columns, values = getColumnData(dmlParam.ti.Columns, defaultIndexColumns, values)
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
func (dmlParam *DMLParam) updateIdentify() bool {
	if len(dmlParam.oldValues) == 0 {
		return false
	}

	values := dmlParam.identifyValues()
	oldValues := dmlParam.oldIdentifyValues()

	for i := 0; i < len(values); i++ {
		if values[i] != oldValues[i] {
			return true
		}
	}

	return false
}

func (dmlParam *DMLParam) genWhere(buf *strings.Builder) []interface{} {
	whereColumns, whereValues := dmlParam.whereColumnsAndValues()

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
func (dmlParam *DMLParam) genUpdateSQL() ([]string, [][]interface{}) {
	if dmlParam.safeMode {
		sqls, args := dmlParam.genDeleteSQL()
		replaceSQLs, replaceArgs := dmlParam.genInsertReplaceSQL()
		sqls = append(sqls, replaceSQLs...)
		args = append(args, replaceArgs...)
		return sqls, args
	}
	var buf strings.Builder
	buf.Grow(2048)
	buf.WriteString("UPDATE ")
	buf.WriteString(dmlParam.tableID)
	buf.WriteString(" SET ")

	for i, column := range dmlParam.columns {
		if i == len(dmlParam.columns)-1 {
			fmt.Fprintf(&buf, "`%s` = ?", strings.ReplaceAll(column.Name.O, "`", "``"))
		} else {
			fmt.Fprintf(&buf, "`%s` = ?, ", strings.ReplaceAll(column.Name.O, "`", "``"))
		}
	}

	buf.WriteString(" WHERE ")
	whereArgs := dmlParam.genWhere(&buf)
	buf.WriteString(" LIMIT 1")

	args := dmlParam.values
	args = append(args, whereArgs...)
	return []string{buf.String()}, [][]interface{}{args}
}

func (dmlParam *DMLParam) genSQL() (sql []string, arg [][]interface{}) {
	switch dmlParam.op {
	case insert:
		return dmlParam.genInsertReplaceSQL()
	case del:
		return dmlParam.genDeleteSQL()
	case update:
		return dmlParam.genUpdateSQL()
	}
	return
}

// genDeleteSQL generates a `DELETE FROM` SQL with `WHERE`.
func (dmlParam *DMLParam) genDeleteSQL() ([]string, [][]interface{}) {
	var buf strings.Builder
	buf.Grow(1024)
	buf.WriteString("DELETE FROM ")
	buf.WriteString(dmlParam.tableID)
	buf.WriteString(" WHERE ")
	whereArgs := dmlParam.genWhere(&buf)
	buf.WriteString(" LIMIT 1")

	return []string{buf.String()}, [][]interface{}{whereArgs}
}

func (dmlParam *DMLParam) genInsertReplaceSQL() ([]string, [][]interface{}) {
	var buf strings.Builder
	buf.Grow(256)
	if dmlParam.safeMode {
		buf.WriteString("REPLACE INTO")
	} else {
		buf.WriteString("INSERT INTO")
	}

	buf.WriteString(" " + dmlParam.tableID + " (")
	for i, column := range dmlParam.columns {
		if i != len(dmlParam.columns)-1 {
			buf.WriteString("`" + strings.ReplaceAll(column.Name.O, "`", "``") + "`,")
		} else {
			buf.WriteString("`" + strings.ReplaceAll(column.Name.O, "`", "``") + "`)")
		}
	}
	buf.WriteString(" VALUES (")

	// placeholders
	for i := range dmlParam.columns {
		if i != len(dmlParam.columns)-1 {
			buf.WriteString("?,")
		} else {
			buf.WriteString("?)")
		}
	}
	return []string{buf.String()}, [][]interface{}{dmlParam.values}
}

func holderString(n int) string {
	builder := new(strings.Builder)
	for i := 0; i < n; i++ {
		if i > 0 {
			builder.WriteString(",")
		}
		builder.WriteString("?")
	}
	return builder.String()
}

func genMultipleRowsInsertReplace(replace bool, dmlParams []*DMLParam) ([]string, [][]interface{}) {
	if len(dmlParams) == 0 {
		return nil, nil
	}

	var buf strings.Builder
	buf.Grow(256)
	if replace {
		buf.WriteString("REPLACE INTO")
	} else {
		buf.WriteString("INSERT INTO")
	}
	buf.WriteString(" " + dmlParams[0].tableID + " (")
	for i, column := range dmlParams[0].columns {
		if i != len(dmlParams[0].columns)-1 {
			buf.WriteString("`" + strings.ReplaceAll(column.Name.O, "`", "``") + "`,")
		} else {
			buf.WriteString("`" + strings.ReplaceAll(column.Name.O, "`", "``") + "`)")
		}
	}
	buf.WriteString(" VALUES ")

	holder := fmt.Sprintf("(%s)", holderString(len(dmlParams[0].columns)))
	for i := range dmlParams {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(holder)
	}

	args := make([]interface{}, 0, len(dmlParams)*len(dmlParams[0].columns))
	for _, dmlParam := range dmlParams {
		args = append(args, dmlParam.values...)
	}
	return []string{buf.String()}, [][]interface{}{args}
}

func genMultipleRowsInsert(dmlParams []*DMLParam) ([]string, [][]interface{}) {
	return genMultipleRowsInsertReplace(false, dmlParams)
}

func genMultipleRowsReplace(dmlParams []*DMLParam) ([]string, [][]interface{}) {
	return genMultipleRowsInsertReplace(true, dmlParams)
}

func genMultipleRowsDelete(dmlParams []*DMLParam) ([]string, [][]interface{}) {
	if len(dmlParams) == 0 {
		return nil, nil
	}

	var buf strings.Builder
	buf.Grow(1024)
	buf.WriteString("DELETE FROM ")
	buf.WriteString(dmlParams[0].tableID)
	buf.WriteString(" WHERE (")

	whereColumns, _ := dmlParams[0].whereColumnsAndValues()
	for i, column := range whereColumns {
		if i != len(whereColumns)-1 {
			buf.WriteString("`" + strings.ReplaceAll(column, "`", "``") + "`,")
		} else {
			buf.WriteString("`" + strings.ReplaceAll(column, "`", "``") + "`)")
		}
	}
	buf.WriteString(" IN (")

	holder := fmt.Sprintf("(%s)", holderString(len(whereColumns)))
	args := make([]interface{}, 0, len(dmlParams)*len(dmlParams[0].columns))
	for i, dmlParam := range dmlParams {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(holder)
		_, whereValues := dmlParam.whereColumnsAndValues()
		args = append(args, whereValues...)
	}
	buf.WriteString(")")
	return []string{buf.String()}, [][]interface{}{args}
}

func genMultipleRowsSQLWithSameTp(dmlParams []*DMLParam) (queries []string, args [][]interface{}) {
	if len(dmlParams) == 0 {
		return
	}

	tp := dmlParams[0].op
	safeMode := dmlParams[0].safeMode
	switch tp {
	case insert:
		if safeMode {
			return genMultipleRowsReplace(dmlParams)
		}
		return genMultipleRowsInsert(dmlParams)
	case replace:
		return genMultipleRowsReplace(dmlParams)
	case del:
		return genMultipleRowsDelete(dmlParams)
	case update:
		for _, dmlParam := range dmlParams {
			query, arg := dmlParam.genUpdateSQL()
			queries = append(queries, query...)
			args = append(args, arg...)
		}
	}
	return
}

func genMultipleRowsSQLWithSameTable(dmlParams []*DMLParam) ([]string, [][]interface{}) {
	queries := make([]string, 0, len(dmlParams))
	args := make([][]interface{}, 0, len(dmlParams))
	var lastTp opType
	var lastSafeMode bool
	tpDMLParams := make([]*DMLParam, 0, len(dmlParams))

	for i, dmlParam := range dmlParams {
		// if update statement didn't update primary values, regard it as replace
		if dmlParam.op == update && !dmlParam.updateIdentify() {
			dmlParam.op = replace
			dmlParam.safeMode = false
		}
		if i == 0 {
			lastTp = dmlParam.op
			lastSafeMode = dmlParam.safeMode
		}
		if lastTp == dmlParam.op && lastSafeMode == dmlParam.safeMode {
			tpDMLParams = append(tpDMLParams, dmlParam)
		} else {
			query, arg := genMultipleRowsSQLWithSameTp(tpDMLParams)
			queries = append(queries, query...)
			args = append(args, arg...)

			tpDMLParams = tpDMLParams[0:0]
			lastTp = dmlParam.op
			lastSafeMode = dmlParam.safeMode
			tpDMLParams = append(tpDMLParams, dmlParam)
		}
	}
	if len(tpDMLParams) > 0 {
		query, arg := genMultipleRowsSQLWithSameTp(tpDMLParams)
		queries = append(queries, query...)
		args = append(args, arg...)
	}
	return queries, args
}

func genMultipleRowsSQL(op opType, dmlParams []*DMLParam) ([]string, [][]interface{}) {
	if len(dmlParams) == 0 {
		return nil, nil
	}

	switch op {
	case del:
		return genMultipleRowsDelete(dmlParams)
	case insert:
		if dmlParams[0].safeMode {
			return genMultipleRowsReplace(dmlParams)
		}
		return genMultipleRowsInsert(dmlParams)
	case update:
		return genMultipleRowsReplace(dmlParams)
	}

	queries := make([]string, 0, len(dmlParams))
	args := make([][]interface{}, 0, len(dmlParams))
	var lastTable string
	tableDMLParams := make([]*DMLParam, 0, len(dmlParams))

	for i, dmlParam := range dmlParams {
		if i == 0 {
			lastTable = dmlParam.tableID
		}
		if lastTable == dmlParam.tableID {
			tableDMLParams = append(tableDMLParams, dmlParam)
		} else {
			query, arg := genMultipleRowsSQLWithSameTable(tableDMLParams)
			queries = append(queries, query...)
			args = append(args, arg...)

			tableDMLParams = tableDMLParams[0:0]
			lastTable = dmlParam.tableID
			tableDMLParams = append(tableDMLParams, dmlParam)
		}
	}
	if len(tableDMLParams) > 0 {
		query, arg := genMultipleRowsSQLWithSameTable(tableDMLParams)
		queries = append(queries, query...)
		args = append(args, arg...)
	}
	return queries, args
}
