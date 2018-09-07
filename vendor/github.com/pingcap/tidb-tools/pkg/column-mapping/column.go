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

package column

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-tools/pkg/table-rule-selector"
)

var (
	// for partition ID, ref definition of partitionID
	instanceIDBitSize       = 4
	schemaIDBitSize         = 7
	tableIDBitSize          = 8
	maxOriginID       int64 = 17592186044416
)

// SetPartitionRule sets bit size of schema ID and table ID
func SetPartitionRule(instanceIDSize, schemaIDSize, tableIDSize int) {
	instanceIDBitSize = instanceIDSize
	schemaIDBitSize = schemaIDSize
	tableIDBitSize = tableIDSize
	maxOriginID = 1 << uint(64-instanceIDSize-schemaIDSize-tableIDSize-1)
}

// Expr indicates how to handle column mapping
type Expr string

// poor Expr
const (
	AddPrefix   Expr = "add prefix"
	AddSuffix   Expr = "add suffix"
	PartitionID Expr = "partition id"
)

// Exprs is some built-in expression for column mapping
// only supports some poor expressions now,
// we would unify tableInfo later and support more
var Exprs = map[Expr]func(*mappingInfo, []interface{}) ([]interface{}, error){
	AddPrefix: addPrefix, // arguments contains prefix
	AddSuffix: addSuffix, // arguments contains suffix
	// arguments contains [instance_id, prefix of schema, prefix of table]
	// we would compute a ID like
	// [1:1 bit][2:9 bits][3:10 bits][4:44 bits] int64  (using default bits length)
	// # 1 useless, no reason
	// # 2 schema ID (schema suffix)
	// # 3 table ID (table suffix)
	// # 4 origin ID (>= 0, <= 17592186044415)
	//
	// others: schema = arguments[1] + schema suffix
	//         table = arguments[2] + table suffix
	//  example: schema = schema_1 table = t_1  => arguments[1] = "schema_", arguments[2] = "t_"
	PartitionID: partitionID,
}

// Rule is a rule to map column
// TODO: we will do it later, if we need to implement a real column mapping, we need table structure of source and target system
type Rule struct {
	PatternSchema    string   `yaml:"schema-pattern" json:"schema-pattern" toml:"schema-pattern"`
	PatternTable     string   `yaml:"table-pattern" json:"table-pattern" toml:"table-pattern"`
	SourceColumn     string   `yaml:"source-column" json:"source-column" toml:"source-column"` // modify, add refer column, ignore
	TargetColumn     string   `yaml:"target-column" json:"target-column" toml:"target-column"` // add column, modify
	Expression       Expr     `yaml:"expression" json:"expression" toml:"expression"`
	Arguments        []string `yaml:"arguments" json:"arguments" toml:"arguments"`
	CreateTableQuery string   `yaml:"create-table-query" json:"create-table-query" toml:"create-table-query"`
}

// Valid checks validity of rule.
// add prefix/suffix: it should have target column and one argument
// partition id: it should have 3 arguments
func (r *Rule) Valid() error {
	if _, ok := Exprs[r.Expression]; !ok {
		return errors.NotFoundf("expression %s", r.Expression)
	}

	if r.TargetColumn == "" {
		return errors.NotValidf("rule need to be applied a target column")
	}

	if r.Expression == AddPrefix || r.Expression == AddSuffix {
		if len(r.Arguments) != 1 {
			return errors.NotValidf("arguments %v for add prefix/suffix", r.Arguments)
		}
	}

	if r.Expression == PartitionID {
		if len(r.Arguments) != 3 {
			return errors.NotValidf("arguments %v for patition id", r.Arguments)
		}
	}

	return nil
}

// check source and target position
func (r *Rule) adjustColumnPosition(source, target int) (int, int, error) {
	// if not found target, ignore it
	if target == -1 {
		return source, target, errors.NotFoundf("target column %s", r.TargetColumn)
	}

	return source, target, nil
}

type mappingInfo struct {
	ignore         bool
	sourcePosition int
	targetPosition int
	rule           *Rule

	instanceID int64
	schemaID   int64
	tableID    int64
}

// Mapping maps column to something by rules
type Mapping struct {
	selector.Selector

	cache struct {
		sync.RWMutex
		infos map[string]*mappingInfo
	}
}

// NewMapping returns a column mapping
func NewMapping(rules []*Rule) (*Mapping, error) {
	m := &Mapping{
		Selector: selector.NewTrieSelector(),
	}
	m.resetCache()

	for _, rule := range rules {
		if err := m.AddRule(rule); err != nil {
			return nil, errors.Annotatef(err, "initial rule %+v in mapping", rule)
		}
	}

	return m, nil
}

// AddRule adds a rule into mapping
func (m *Mapping) AddRule(rule *Rule) error {
	if m == nil || rule == nil {
		return nil
	}

	err := rule.Valid()
	if err != nil {
		return errors.Trace(err)
	}

	m.resetCache()
	err = m.Insert(rule.PatternSchema, rule.PatternTable, rule, false)
	if err != nil {
		return errors.Annotatef(err, "add rule %+v into mapping", rule)
	}

	return nil
}

// UpdateRule updates mapping rule
func (m *Mapping) UpdateRule(rule *Rule) error {
	if m == nil || rule == nil {
		return nil
	}

	err := rule.Valid()
	if err != nil {
		return errors.Trace(err)
	}

	m.resetCache()
	err = m.Insert(rule.PatternSchema, rule.PatternTable, rule, true)
	if err != nil {
		return errors.Annotatef(err, "update rule %+v in mapping", rule)
	}

	return nil
}

// RemoveRule removes a rule from mapping
func (m *Mapping) RemoveRule(rule *Rule) error {
	if m == nil || rule == nil {
		return nil
	}

	m.resetCache()
	err := m.Remove(rule.PatternSchema, rule.PatternTable)
	if err != nil {
		return errors.Annotatef(err, "remove rule %+v", rule)
	}

	return nil
}

// HandleRowValue handles row value
func (m *Mapping) HandleRowValue(schema, table string, columns []string, vals []interface{}) ([]interface{}, []int, error) {
	if m == nil {
		return vals, nil, nil
	}

	info, err := m.queryColumnInfo(schema, table, columns)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	if info.ignore == true {
		return vals, nil, nil
	}

	exp, ok := Exprs[info.rule.Expression]
	if !ok {
		return nil, nil, errors.NotFoundf("column mapping expression %s", info.rule.Expression)
	}

	vals, err = exp(info, vals)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	return vals, []int{info.sourcePosition, info.targetPosition}, nil
}

// HandleDDL handles ddl
func (m *Mapping) HandleDDL(schema, table string, columns []string, statement string) (string, []int, error) {
	if m == nil {
		return statement, nil, nil
	}

	info, err := m.queryColumnInfo(schema, table, columns)
	if err != nil {
		return statement, nil, errors.Trace(err)
	}

	if info.ignore == true {
		return statement, nil, nil
	}

	m.resetCache()
	// only output erro now, wait fix it manually
	return statement, nil, errors.NotImplementedf("ddl %s @ column mapping rule %s/%s:%+v", statement, schema, table, info.rule)
}

func (m *Mapping) queryColumnInfo(schema, table string, columns []string) (*mappingInfo, error) {
	m.cache.RLock()
	ci, ok := m.cache.infos[tableName(schema, table)]
	m.cache.RUnlock()
	if ok {
		return ci, nil
	}

	var info = &mappingInfo{
		ignore: true,
	}
	rules := m.Match(schema, table)
	if len(rules) == 0 {
		m.cache.Lock()
		m.cache.infos[tableName(schema, table)] = info
		m.cache.Unlock()

		return info, nil
	}

	var (
		schemaRules []*Rule
		tableRules  = make([]*Rule, 0, 1)
	)
	// classify rules into schema level rules and table level
	// table level rules have highest priority
	for i := range rules {
		rule, ok := rules[i].(*Rule)
		if !ok {
			return nil, errors.NotValidf("column mapping rule %+v", rules[i])
		}

		if len(rule.PatternTable) == 0 {
			schemaRules = append(schemaRules, rule)
		} else {
			tableRules = append(tableRules, rule)
		}
	}

	// only support one expression for one table now, refine it later
	var rule *Rule
	if len(table) == 0 || len(tableRules) == 0 {
		if len(schemaRules) != 1 {
			return nil, errors.NotSupportedf("route %s/%s to rule set(%d)", schema, table, len(schemaRules))
		}

		rule = schemaRules[0]
	} else {
		if len(tableRules) != 1 {
			return nil, errors.NotSupportedf("route %s/%s to rule set(%d)", schema, table, len(tableRules))
		}

		rule = tableRules[0]
	}
	if rule == nil {
		m.cache.Lock()
		m.cache.infos[tableName(schema, table)] = info
		m.cache.Unlock()

		return info, nil
	}

	// compute source and target column position
	sourcePosition := findColumnPosition(columns, rule.SourceColumn)
	targetPosition := findColumnPosition(columns, rule.TargetColumn)

	sourcePosition, targetPosition, err := rule.adjustColumnPosition(sourcePosition, targetPosition)
	if err != nil {
		return nil, errors.Trace(err)
	}

	info = &mappingInfo{
		sourcePosition: sourcePosition,
		targetPosition: targetPosition,
		rule:           rule,
	}

	// if expr is partition ID, compute schema and table ID
	if rule.Expression == PartitionID {
		info.instanceID, info.schemaID, info.tableID, err = computePartitionID(schema, table, rule)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	m.cache.Lock()
	m.cache.infos[tableName(schema, table)] = info
	m.cache.Unlock()

	return info, nil
}

func (m *Mapping) resetCache() {
	m.cache.Lock()
	m.cache.infos = make(map[string]*mappingInfo)
	m.cache.Unlock()
}

func findColumnPosition(cols []string, col string) int {
	for i := range cols {
		if cols[i] == col {
			return i
		}
	}

	return -1
}

func tableName(schema, table string) string {
	return fmt.Sprintf("`%s`.`%s`", schema, table)
}

func addPrefix(info *mappingInfo, vals []interface{}) ([]interface{}, error) {
	prefix := info.rule.Arguments[0]
	originStr, ok := vals[info.targetPosition].(string)
	if !ok {
		return nil, errors.NotValidf("column %d value is not string, but %v", info.targetPosition, vals[info.targetPosition])
	}

	// fast to concatenated string
	rawByte := make([]byte, 0, len(prefix)+len(originStr))
	rawByte = append(rawByte, prefix...)
	rawByte = append(rawByte, originStr...)

	vals[info.targetPosition] = string(rawByte)
	return vals, nil
}

func addSuffix(info *mappingInfo, vals []interface{}) ([]interface{}, error) {
	suffix := info.rule.Arguments[0]
	originStr, ok := vals[info.targetPosition].(string)
	if !ok {
		return nil, errors.NotValidf("column %d value is not string, but %v", info.targetPosition, vals[info.targetPosition])
	}

	rawByte := make([]byte, 0, len(suffix)+len(originStr))
	rawByte = append(rawByte, originStr...)
	rawByte = append(rawByte, suffix...)

	vals[info.targetPosition] = string(rawByte)
	return vals, nil
}

func partitionID(info *mappingInfo, vals []interface{}) ([]interface{}, error) {
	// only int64 now
	var (
		originID int64
		err      error
		isChars  bool
	)

	switch rawID := vals[info.targetPosition].(type) {
	case int:
		originID = int64(rawID)
	case int8:
		originID = int64(rawID)
	case int32:
		originID = int64(rawID)
	case int64:
		originID = rawID
	case uint:
		originID = int64(rawID)
	case uint16:
		originID = int64(rawID)
	case uint32:
		originID = int64(rawID)
	case uint64:
		originID = int64(rawID)
	case string:
		originID, err = strconv.ParseInt(rawID, 10, 64)
		if err != nil {
			return nil, errors.NotValidf("column %d value is not int, but %v", info.targetPosition, vals[info.targetPosition])
		}
		isChars = true
	default:
		return nil, errors.NotValidf("type %T(%v)", vals[info.targetPosition], vals[info.targetPosition])
	}

	if originID >= maxOriginID || originID < 0 {
		return nil, errors.NotValidf("id must less than %d, bigger or equal than 0, but get %d", maxOriginID, originID)
	}

	originID = int64(info.instanceID | info.schemaID | info.tableID | originID)
	if isChars {
		vals[info.targetPosition] = strconv.FormatInt(originID, 10)
	} else {
		vals[info.targetPosition] = originID
	}

	return vals, nil
}

func computePartitionID(schema, table string, rule *Rule) (instanceID int64, schemaID int64, tableID int64, err error) {
	shiftCnt := uint(64 - instanceIDBitSize - 1)
	if instanceIDBitSize > 0 {
		var instanceIDUnsign uint64
		instanceIDUnsign, err = strconv.ParseUint(rule.Arguments[0], 10, instanceIDBitSize)
		if err != nil {
			return
		}
		instanceID = int64(instanceIDUnsign << shiftCnt)
	}

	if schemaIDBitSize > 0 {
		shiftCnt = shiftCnt - uint(schemaIDBitSize)
		schemaID, err = computeID(schema, rule.Arguments[1], schemaIDBitSize, shiftCnt)
		if err != nil {
			return
		}
	}

	if tableIDBitSize > 0 {
		shiftCnt = shiftCnt - uint(tableIDBitSize)
		tableID, err = computeID(table, rule.Arguments[2], tableIDBitSize, shiftCnt)
	}
	return
}

func computeID(name string, prefix string, bitSize int, shiftCount uint) (int64, error) {
	if len(prefix) >= len(name) || prefix != name[:len(prefix)] {
		return 0, errors.NotValidf("%s is not prefix of %s", prefix, name)
	}

	idStr := name[len(prefix):]
	id, err := strconv.ParseUint(idStr, 10, bitSize)
	if err != nil {
		return 0, errors.NotValidf("suffix of %s is not int64", idStr)
	}

	return int64(id << shiftCount), nil
}
