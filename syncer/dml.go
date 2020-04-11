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
	"io"
	"strconv"
	"strings"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"

	"github.com/pingcap/tidb-tools/pkg/dbutil"
)

type genColumnCacheStatus uint8

const (
	genColumnNoCache genColumnCacheStatus = iota
	hasGenColumn
	noGenColumn
)

// GenColCache stores generated column information for all tables
type GenColCache struct {
	// `schema`.`table` -> whether this table has generated column
	hasGenColumn map[string]bool

	// `schema`.`table` -> column list
	columns map[string][]*column

	// `schema`.`table` -> a bool slice representing whether it is generated for each column
	isGenColumn map[string][]bool
}

// genDMLParam stores pruned columns, data as well as the original columns, data, index
type genDMLParam struct {
	schema               string
	table                string
	safeMode             bool                 // only used in update
	data                 [][]interface{}      // pruned data
	originalData         [][]interface{}      // all data
	columns              []*column            // pruned columns
	originalColumns      []*column            // all columns
	originalIndexColumns map[string][]*column // all index information
}

// NewGenColCache creates a GenColCache.
func NewGenColCache() *GenColCache {
	c := &GenColCache{}
	c.reset()
	return c
}

// status returns `NotFound` if a `schema`.`table` has no generated column
// information cached, otherwise returns `hasGenColumn` if cache found and
// it has generated column and returns `noGenColumn` if it has no generated column.
func (c *GenColCache) status(key string) genColumnCacheStatus {
	val, ok := c.hasGenColumn[key]
	if !ok {
		return genColumnNoCache
	}
	if val {
		return hasGenColumn
	}
	return noGenColumn
}

func (c *GenColCache) clearTable(schema, table string) {
	key := dbutil.TableName(schema, table)
	delete(c.hasGenColumn, key)
	delete(c.columns, key)
	delete(c.isGenColumn, key)
}

func (c *GenColCache) reset() {
	c.hasGenColumn = make(map[string]bool)
	c.columns = make(map[string][]*column)
	c.isGenColumn = make(map[string][]bool)
}

func extractValueFromData(data []interface{}, columns []*column) []interface{} {
	value := make([]interface{}, 0, len(data))
	for i := range data {
		value = append(value, castUnsigned(data[i], columns[i].unsigned, columns[i].tp))
	}
	return value
}

func genInsertSQLs(param *genDMLParam) ([]string, [][]string, [][]interface{}, error) {
	var (
		fullname             = dbutil.TableName(param.schema, param.table)
		dataSeq              = param.data
		originalDataSeq      = param.originalData
		columns              = param.columns
		originalColumns      = param.originalColumns
		originalIndexColumns = param.originalIndexColumns
		sqls                 = make([]string, 0, len(dataSeq))
		keys                 = make([][]string, 0, len(dataSeq))
		values               = make([][]interface{}, 0, len(dataSeq))
	)

	var insertOrReplace = "INSERT INTO"
	if param.safeMode {
		insertOrReplace = "REPLACE INTO"
	}
	sql := genInsertReplace(insertOrReplace, fullname, columns)

	for dataIdx, data := range dataSeq {
		if len(data) != len(columns) {
			return nil, nil, nil, terror.ErrSyncerUnitDMLColumnNotMatch.Generate(len(columns), len(data))
		}

		value := extractValueFromData(data, columns)
		var originalValue = value
		if len(columns) != len(originalColumns) {
			originalValue = extractValueFromData(originalDataSeq[dataIdx], originalColumns)
		}

		ks := genMultipleKeys(originalValue, originalIndexColumns, fullname)
		sqls = append(sqls, sql)
		values = append(values, value)
		keys = append(keys, ks)
	}

	return sqls, keys, values, nil
}

func genUpdateSQLs(param *genDMLParam) ([]string, [][]string, [][]interface{}, error) {
	var (
		fullname             = dbutil.TableName(param.schema, param.table)
		data                 = param.data
		originalData         = param.originalData
		columns              = param.columns
		originalColumns      = param.originalColumns
		originalIndexColumns = param.originalIndexColumns
		defaultIndexColumns  = findFitIndex(originalIndexColumns)
		replaceSQL           string // `REPLACE INTO` SQL
		sqls                 = make([]string, 0, len(data)/2)
		keys                 = make([][]string, 0, len(data)/2)
		values               = make([][]interface{}, 0, len(data)/2)
	)

	if param.safeMode {
		replaceSQL = genInsertReplace("REPLACE INTO", fullname, columns)
	}

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
		if len(columns) == len(originalColumns) {
			oriOldValues = oldValues
			oriChangedValues = changedValues
		} else {
			oriOldValues = extractValueFromData(oriOldData, originalColumns)
			oriChangedValues = extractValueFromData(oriChangedData, originalColumns)
		}

		if len(defaultIndexColumns) == 0 {
			defaultIndexColumns = getAvailableIndexColumn(originalIndexColumns, oriOldValues)
		}

		ks := genMultipleKeys(oriOldValues, originalIndexColumns, fullname)
		ks = append(ks, genMultipleKeys(oriChangedValues, originalIndexColumns, fullname)...)

		if param.safeMode {
			// generate delete sql from old data
			sql, value := genDeleteSQL(fullname, oriOldValues, originalColumns, defaultIndexColumns)
			sqls = append(sqls, sql)
			values = append(values, value)
			keys = append(keys, ks)
			// generate replace sql from new data
			sqls = append(sqls, replaceSQL)
			values = append(values, changedValues)
			keys = append(keys, ks)
			continue
		}

		// NOTE: move these variables outer of `for` if needed (to reuse).
		updateColumns := make([]*column, 0, len(defaultIndexColumns))
		updateValues := make([]interface{}, 0, len(defaultIndexColumns))
		for j := range oldValues {
			updateColumns = append(updateColumns, columns[j])
			updateValues = append(updateValues, changedValues[j])
		}

		// ignore no changed sql
		if len(updateColumns) == 0 {
			continue
		}

		value := make([]interface{}, 0, len(oldData))
		value = append(value, updateValues...)

		whereColumns, whereValues := originalColumns, oriOldValues
		if len(defaultIndexColumns) > 0 {
			whereColumns = defaultIndexColumns
			whereValues = getColumnData(defaultIndexColumns, oriOldValues)
		}

		value = append(value, whereValues...)

		sql := genUpdateSQL(fullname, updateColumns, whereColumns, whereValues)
		sqls = append(sqls, sql)
		values = append(values, value)
		keys = append(keys, ks)
	}

	return sqls, keys, values, nil
}

func genDeleteSQLs(param *genDMLParam) ([]string, [][]string, [][]interface{}, error) {
	var (
		fullname            = dbutil.TableName(param.schema, param.table)
		dataSeq             = param.originalData
		columns             = param.originalColumns
		indexColumns        = param.originalIndexColumns
		defaultIndexColumns = findFitIndex(indexColumns)
		sqls                = make([]string, 0, len(dataSeq))
		keys                = make([][]string, 0, len(dataSeq))
		values              = make([][]interface{}, 0, len(dataSeq))
	)

	for _, data := range dataSeq {
		if len(data) != len(columns) {
			return nil, nil, nil, terror.ErrSyncerUnitDMLColumnNotMatch.Generate(len(columns), len(data))
		}

		value := extractValueFromData(data, columns)

		if len(defaultIndexColumns) == 0 {
			defaultIndexColumns = getAvailableIndexColumn(indexColumns, value)
		}
		ks := genMultipleKeys(value, indexColumns, fullname)

		sql, value := genDeleteSQL(fullname, value, columns, defaultIndexColumns)
		sqls = append(sqls, sql)
		values = append(values, value)
		keys = append(keys, ks)
	}

	return sqls, keys, values, nil
}

// genInsertReplace generates a DML for `INSERT INTO` or `REPLCATE INTO`.
// the returned SQL with placeholders for `VALUES`.
func genInsertReplace(op, table string, columns []*column) string {
	// NOTE: use sync.Pool to hold the builder if needed later.
	var buf strings.Builder
	buf.Grow(256)
	buf.WriteString(op)
	buf.WriteString(" " + table + " (")
	for i, column := range columns {
		if i != len(columns)-1 {
			buf.WriteString("`" + column.name + "`,")
		} else {
			buf.WriteString("`" + column.name + "`)")
		}
	}
	buf.WriteString(" VALUES (")

	// placeholders
	for i := range columns {
		if i != len(columns)-1 {
			buf.WriteString("?,")
		} else {
			buf.WriteString("?)")
		}
	}
	return buf.String()
}

// genUpdateSQL generates a `UPDATE` SQL with `SET` and `WHERE`.
func genUpdateSQL(table string, updateColumns, whereColumns []*column, whereValues []interface{}) string {
	var buf strings.Builder
	buf.Grow(2048)
	buf.WriteString("UPDATE ")
	buf.WriteString(table)
	buf.WriteString(" SET ")

	for i, column := range updateColumns {
		if i == len(updateColumns)-1 {
			fmt.Fprintf(&buf, "`%s` = ?", column.name) // TODO: update `tidb-tools` to use `dbutil.ColumnName`.
		} else {
			fmt.Fprintf(&buf, "`%s` = ?, ", column.name)
		}
	}

	buf.WriteString(" WHERE ")
	genWhere(&buf, whereColumns, whereValues)
	buf.WriteString(" LIMIT 1")
	return buf.String()
}

// genDeleteSQL generates a `DELETE FROM` SQL with `WHERE`.
func genDeleteSQL(table string, value []interface{}, columns []*column, indexColumns []*column) (string, []interface{}) {
	whereColumns, whereValues := columns, value
	if len(indexColumns) > 0 {
		whereColumns = indexColumns
		whereValues = getColumnData(indexColumns, value)
	}

	var buf strings.Builder
	buf.Grow(1024)
	buf.WriteString("DELETE FROM ")
	buf.WriteString(table)
	buf.WriteString(" WHERE ")
	genWhere(&buf, whereColumns, whereValues)
	buf.WriteString(" LIMIT 1")

	return buf.String(), whereValues
}

func castUnsigned(data interface{}, unsigned bool, tp string) interface{} {
	if !unsigned {
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
		if strings.Contains(strings.ToLower(tp), "mediumint") {
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

func columnValue(value interface{}, unsigned bool, tp string) string {
	castValue := castUnsigned(value, unsigned, tp)

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

func findColumn(columns []*column, indexColumn string) *column {
	for _, column := range columns {
		if column.name == indexColumn {
			return column
		}
	}

	return nil
}

func findColumns(columns []*column, indexColumns map[string][]string) map[string][]*column {
	result := make(map[string][]*column)

	for keyName, indexCols := range indexColumns {
		cols := make([]*column, 0, len(indexCols))
		for _, name := range indexCols {
			column := findColumn(columns, name)
			if column != nil {
				cols = append(cols, column)
			}
		}
		result[keyName] = cols
	}

	return result
}

func genKeyList(table string, columns []*column, dataSeq []interface{}) string {
	var buf strings.Builder
	buf.WriteString(table)
	for i, data := range dataSeq {
		// for key, I think no need to add the `,` separator.
		buf.WriteString(columnValue(data, columns[i].unsigned, columns[i].tp))
	}
	return buf.String()
}

func genMultipleKeys(value []interface{}, indexColumns map[string][]*column, table string) []string {
	multipleKeys := make([]string, 0, len(indexColumns))
	for _, indexCols := range indexColumns {
		vals := getColumnData(indexCols, value)
		multipleKeys = append(multipleKeys, genKeyList(table, indexCols, vals))
	}
	return multipleKeys
}

func findFitIndex(indexColumns map[string][]*column) []*column {
	cols, ok := indexColumns["primary"]
	if ok {
		if len(cols) == 0 {
			log.L().Error("cols is empty")
		} else {
			return cols
		}
	}

	// second find not null unique key
	fn := func(c *column) bool {
		return !c.NotNull
	}

	return getSpecifiedIndexColumn(indexColumns, fn)
}

func getAvailableIndexColumn(indexColumns map[string][]*column, data []interface{}) []*column {
	fn := func(c *column) bool {
		return data[c.idx] == nil
	}

	return getSpecifiedIndexColumn(indexColumns, fn)
}

func getSpecifiedIndexColumn(indexColumns map[string][]*column, fn func(col *column) bool) []*column {
	for _, indexCols := range indexColumns {
		if len(indexCols) == 0 {
			continue
		}

		findFitIndex := true
		for _, col := range indexCols {
			if fn(col) {
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

func getColumnData(indexColumns []*column, data []interface{}) []interface{} {
	values := make([]interface{}, 0, len(indexColumns))
	for _, column := range indexColumns {
		values = append(values, data[column.idx])
	}

	return values
}

func genWhere(w io.Writer, columns []*column, data []interface{}) {
	for i := range columns {
		kvSplit := "="
		if data[i] == nil {
			kvSplit = "IS"
		}

		if i == len(columns)-1 {
			fmt.Fprintf(w, "`%s` %s ?", columns[i].name, kvSplit)
		} else {
			fmt.Fprintf(w, "`%s` %s ? AND ", columns[i].name, kvSplit)
		}
	}
}

func (s *Syncer) mappingDML(schema, table string, columns []string, data [][]interface{}) ([][]interface{}, error) {
	if s.columnMapping == nil {
		return data, nil
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
func pruneGeneratedColumnDML(columns []*column, data [][]interface{}, schema, table string, cache *GenColCache) ([]*column, [][]interface{}, error) {
	var (
		cacheKey    = dbutil.TableName(schema, table)
		cacheStatus = cache.status(cacheKey)
	)

	if cacheStatus == noGenColumn {
		return columns, data, nil
	}
	if cacheStatus == hasGenColumn {
		rows := make([][]interface{}, 0, len(data))
		filters, ok1 := cache.isGenColumn[cacheKey]
		if !ok1 {
			return nil, nil, terror.ErrSyncerUnitCacheKeyNotFound.Generate(cacheKey, "isGenColumn")
		}
		cols, ok2 := cache.columns[cacheKey]
		if !ok2 {
			return nil, nil, terror.ErrSyncerUnitCacheKeyNotFound.Generate(cacheKey, "columns")
		}
		for _, row := range data {
			value := make([]interface{}, 0, len(row))
			for i := range row {
				if !filters[i] {
					value = append(value, row[i])
				}
			}
			rows = append(rows, value)
		}
		return cols, rows, nil
	}

	var (
		needPrune       bool
		colIndexfilters = make([]bool, 0, len(columns))
		genColumnNames  = make(map[string]bool)
	)

	for _, c := range columns {
		isGenColumn := c.isGeneratedColumn()
		colIndexfilters = append(colIndexfilters, isGenColumn)
		if isGenColumn {
			needPrune = true
			genColumnNames[c.name] = true
		}
	}

	if !needPrune {
		cache.hasGenColumn[cacheKey] = false
		return columns, data, nil
	}

	var (
		cols = make([]*column, 0, len(columns))
		rows = make([][]interface{}, 0, len(data))
	)

	for i := range columns {
		if !colIndexfilters[i] {
			cols = append(cols, columns[i])
		}
	}
	for _, row := range data {
		if len(row) != len(columns) {
			return nil, nil, terror.ErrSyncerUnitDMLPruneColumnMismatch.Generate(len(columns), len(data))
		}
		value := make([]interface{}, 0, len(row))
		for i := range row {
			if !colIndexfilters[i] {
				value = append(value, row[i])
			}
		}
		rows = append(rows, value)
	}
	cache.hasGenColumn[cacheKey] = true
	cache.columns[cacheKey] = cols
	cache.isGenColumn[cacheKey] = colIndexfilters

	return cols, rows, nil
}
