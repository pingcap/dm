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

	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/types"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
)

// genDMLParam stores pruned columns, data as well as the original columns, data, index
type genDMLParam struct {
	schema            string
	table             string
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

func genInsertSQLs(param *genDMLParam) ([]string, [][]string, [][]interface{}, error) {
	var (
		qualifiedName   = dbutil.TableName(param.schema, param.table)
		dataSeq         = param.data
		originalDataSeq = param.originalData
		columns         = param.columns
		ti              = param.originalTableInfo
	)
	sqls := make([]string, 0, len(dataSeq))
	keys := make([][]string, 0, len(dataSeq))
	values := make([][]interface{}, 0, len(dataSeq))
	columnList := genColumnList(columns)
	columnPlaceholders := genColumnPlaceholders(len(columns))
	for dataIdx, data := range dataSeq {
		if len(data) != len(columns) {
			return nil, nil, nil, terror.ErrSyncerUnitDMLColumnNotMatch.Generate(len(columns), len(data))
		}

		value := extractValueFromData(data, columns)
		originalData := originalDataSeq[dataIdx]
		var originalValue []interface{}
		if len(columns) == len(ti.Columns) {
			originalValue = value
		} else {
			originalValue = extractValueFromData(originalData, ti.Columns)
		}

		var insertOrReplace string
		if param.safeMode {
			insertOrReplace = "REPLACE"
		} else {
			insertOrReplace = "INSERT"
		}

		sql := fmt.Sprintf("%s INTO %s (%s) VALUES (%s);", insertOrReplace, qualifiedName, columnList, columnPlaceholders)
		ks := genMultipleKeys(ti, originalValue)
		sqls = append(sqls, sql)
		values = append(values, value)
		keys = append(keys, ks)
	}

	return sqls, keys, values, nil
}

func genUpdateSQLs(param *genDMLParam) ([]string, [][]string, [][]interface{}, error) {
	var (
		qualifiedName = dbutil.TableName(param.schema, param.table)
		safeMode      = param.safeMode
		data          = param.data
		originalData  = param.originalData
		columns       = param.columns
		ti            = param.originalTableInfo
	)
	sqls := make([]string, 0, len(data)/2)
	keys := make([][]string, 0, len(data)/2)
	values := make([][]interface{}, 0, len(data)/2)
	columnList := genColumnList(columns)
	columnPlaceholders := genColumnPlaceholders(len(columns))
	defaultIndexColumns := findFitIndex(ti)

	for i := 0; i < len(data); i += 2 {
		oldData := data[i]
		changedData := data[i+1]
		oriOldData := originalData[i]
		oriChangedData := originalData[i+1]

		if len(oldData) != len(changedData) {
			return nil, nil, nil, terror.ErrSyncerUnitDMLOldNewValueMismatch.Generate(len(oldData), len(changedData))
		}

		if len(oldData) != len(columns) {
			return nil, nil, nil, terror.ErrSyncerUnitDMLColumnNotMatch.Generate(len(columns), len(oldData))
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

		if defaultIndexColumns == nil {
			defaultIndexColumns = getAvailableIndexColumn(ti, oriOldValues)
		}

		ks := genMultipleKeys(ti, oriOldValues)
		ks = append(ks, genMultipleKeys(ti, oriChangedValues)...)

		if safeMode {
			// generate delete sql from old data
			sql, value := genDeleteSQL(qualifiedName, oriOldValues, ti.Columns, defaultIndexColumns)
			sqls = append(sqls, sql)
			values = append(values, value)
			keys = append(keys, ks)
			// generate replace sql from new data
			sql = fmt.Sprintf("REPLACE INTO %s (%s) VALUES (%s);", qualifiedName, columnList, columnPlaceholders)
			sqls = append(sqls, sql)
			values = append(values, changedValues)
			keys = append(keys, ks)
			continue
		}

		updateColumns := make([]*model.ColumnInfo, 0, indexColumnsCount(defaultIndexColumns))
		updateValues := make([]interface{}, 0, indexColumnsCount(defaultIndexColumns))
		for j := range oldValues {
			updateColumns = append(updateColumns, columns[j])
			updateValues = append(updateValues, changedValues[j])
		}

		// ignore no changed sql
		if len(updateColumns) == 0 {
			continue
		}

		value := make([]interface{}, 0, len(oldData))
		kvs := genKVs(updateColumns)
		value = append(value, updateValues...)

		whereColumns, whereValues := ti.Columns, oriOldValues
		if defaultIndexColumns != nil {
			whereColumns, whereValues = getColumnData(ti.Columns, defaultIndexColumns, oriOldValues)
		}

		where := genWhere(whereColumns, whereValues)
		value = append(value, whereValues...)

		sql := fmt.Sprintf("UPDATE %s SET %s WHERE %s LIMIT 1;", qualifiedName, kvs, where)
		sqls = append(sqls, sql)
		values = append(values, value)
		keys = append(keys, ks)
	}

	return sqls, keys, values, nil
}

func genDeleteSQLs(param *genDMLParam) ([]string, [][]string, [][]interface{}, error) {
	var (
		qualifiedName = dbutil.TableName(param.schema, param.table)
		dataSeq       = param.originalData
		ti            = param.originalTableInfo
	)
	sqls := make([]string, 0, len(dataSeq))
	keys := make([][]string, 0, len(dataSeq))
	values := make([][]interface{}, 0, len(dataSeq))
	defaultIndexColumns := findFitIndex(ti)

	for _, data := range dataSeq {
		if len(data) != len(ti.Columns) {
			return nil, nil, nil, terror.ErrSyncerUnitDMLColumnNotMatch.Generate(len(ti.Columns), len(data))
		}

		value := extractValueFromData(data, ti.Columns)

		if defaultIndexColumns == nil {
			defaultIndexColumns = getAvailableIndexColumn(ti, value)
		}
		ks := genMultipleKeys(ti, value)

		sql, value := genDeleteSQL(qualifiedName, value, ti.Columns, defaultIndexColumns)
		sqls = append(sqls, sql)
		values = append(values, value)
		keys = append(keys, ks)
	}

	return sqls, keys, values, nil
}

func genDeleteSQL(qualifiedName string, value []interface{}, columns []*model.ColumnInfo, indexColumns *model.IndexInfo) (string, []interface{}) {
	whereColumns, whereValues := columns, value
	if indexColumns != nil {
		whereColumns, whereValues = getColumnData(columns, indexColumns, value)
	}

	where := genWhere(whereColumns, whereValues)
	sql := fmt.Sprintf("DELETE FROM %s WHERE %s LIMIT 1;", qualifiedName, where)

	return sql, whereValues
}

func indexColumnsCount(index *model.IndexInfo) int {
	if index == nil {
		return 0
	}
	return len(index.Columns)
}

func genColumnList(columns []*model.ColumnInfo) string {
	var buf strings.Builder
	for i, column := range columns {
		if i != 0 {
			buf.WriteByte(',')
		}
		buf.WriteByte('`')
		buf.WriteString(strings.ReplaceAll(column.Name.O, "`", "``"))
		buf.WriteByte('`')
	}
	return buf.String()
}

func genColumnPlaceholders(length int) string {
	values := make([]string, length)
	for i := 0; i < length; i++ {
		values[i] = "?"
	}
	return strings.Join(values, ",")
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
		return strconv.FormatUint(uint64(v), 10)
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
		data = strconv.FormatInt(int64(v), 10)
	case uint8:
		data = strconv.FormatUint(uint64(v), 10)
	case uint16:
		data = strconv.FormatUint(uint64(v), 10)
	case uint32:
		data = strconv.FormatUint(uint64(v), 10)
	case uint64:
		data = strconv.FormatUint(uint64(v), 10)
	case float32:
		data = strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		data = strconv.FormatFloat(float64(v), 'f', -1, 64)
	case string:
		data = v
	case []byte:
		data = string(v)
	default:
		data = fmt.Sprintf("%v", v)
	}

	return data
}

func genKeyList(columns []*model.ColumnInfo, dataSeq []interface{}) string {
	values := make([]string, 0, len(dataSeq))
	for i, data := range dataSeq {
		values = append(values, columnValue(data, &columns[i].FieldType))
	}

	return strings.Join(values, ",")
}

func genMultipleKeys(ti *model.TableInfo, value []interface{}) []string {
	multipleKeys := make([]string, 0, len(ti.Indices)+1)
	if ti.PKIsHandle {
		if pk := ti.GetPkColInfo(); pk != nil {
			cols := []*model.ColumnInfo{pk}
			vals := []interface{}{value[pk.Offset]}
			multipleKeys = append(multipleKeys, genKeyList(cols, vals))
		}
	}

	for _, indexCols := range ti.Indices {
		cols, vals := getColumnData(ti.Columns, indexCols, value)
		multipleKeys = append(multipleKeys, genKeyList(cols, vals))
	}
	return multipleKeys
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
	cols := make([]*model.ColumnInfo, 0, len(columns))
	values := make([]interface{}, 0, len(columns))
	for _, column := range indexColumns.Columns {
		cols = append(cols, columns[column.Offset])
		values = append(values, data[column.Offset])
	}

	return cols, values
}

func genWhere(columns []*model.ColumnInfo, data []interface{}) string {
	var kvs strings.Builder
	for i, col := range columns {
		if i != 0 {
			kvs.WriteString(" AND ")
		}
		kvs.WriteByte('`')
		kvs.WriteString(strings.ReplaceAll(col.Name.O, "`", "``"))
		if data[i] == nil {
			kvs.WriteString("` IS ?")
		} else {
			kvs.WriteString("` = ?")
		}
	}

	return kvs.String()
}

func genKVs(columns []*model.ColumnInfo) string {
	var kvs strings.Builder
	for i, col := range columns {
		if i != 0 {
			kvs.WriteString(", ")
		}
		kvs.WriteByte('`')
		kvs.WriteString(strings.ReplaceAll(col.Name.O, "`", "``"))
		kvs.WriteString("` = ?")
	}

	return kvs.String()
}

func (s *Syncer) mappingDML(schema, table string, ti *model.TableInfo, data [][]interface{}) ([][]interface{}, error) {
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
		rows[i], _, err = s.columnMapping.HandleRowValue(schema, table, columns, data[i])
		if err != nil {
			return nil, terror.ErrSyncerUnitDoColumnMapping.Delegate(err, data[i], schema, table)
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
