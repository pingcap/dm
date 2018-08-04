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

package router

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb-tools/pkg/table-rule-selector"
)

// TableRule is a rule to route schema/table to target schema/table
// pattern format refers 'pkg/table-rule-selector'
type TableRule struct {
	SchemaPattern string `json:"schema-pattern" toml:"schema-pattern"`
	TablePattern  string `json:"table-pattern" toml:"table-pattern"`
	TargetSchema  string `json:"target-schema" toml:"target-schema"`
	TargetTable   string `json:"target-table" toml:"target-table"`
}

// Valid checks validity of rule
func (t *TableRule) Valid() error {
	if len(t.SchemaPattern) == 0 {
		return errors.New("schema pattern of table route rule should not be empty")
	}

	if len(t.TargetSchema) == 0 {
		return errors.New("target schema of table route rule should not be empty")
	}

	return nil
}

// Table routes schema/table to target schema/table by given route rules
type Table struct {
	selector.Selector
}

// NewTableRouter returns a table router
func NewTableRouter(rules []*TableRule) (*Table, error) {
	r := &Table{
		Selector: selector.NewTrieSelector(),
	}

	for _, rule := range rules {
		if err := r.AddRule(rule); err != nil {
			return nil, errors.Annotatef(err, "initial rule %+v in table router", rule)
		}
	}

	return r, nil
}

// AddRule adds a rule into table router
func (r *Table) AddRule(rule *TableRule) error {
	err := rule.Valid()
	if err != nil {
		return errors.Trace(err)
	}

	err = r.Insert(rule.SchemaPattern, rule.TablePattern, rule, false)
	if err != nil {
		return errors.Annotatef(err, "add rule %+v into table router", rule)
	}

	return nil
}

// UpdateRule updates rule
func (r *Table) UpdateRule(rule *TableRule) error {
	err := rule.Valid()
	if err != nil {
		return errors.Trace(err)
	}

	err = r.Insert(rule.SchemaPattern, rule.TablePattern, rule, true)
	if err != nil {
		return errors.Annotatef(err, "update rule %+v into table router", rule)
	}

	return nil
}

// RemoveRule removes a rule from table router
func (r *Table) RemoveRule(rule *TableRule) error {
	err := r.Remove(rule.SchemaPattern, rule.TablePattern)
	if err != nil {
		return errors.Annotatef(err, "remove rule %+v", rule)
	}

	return nil
}

// Route routes schema/table to target schema/table
// don't support to route schema/table to multiple schema/table
func (r *Table) Route(schema, table string) (string, string, error) {
	rules := r.Match(schema, table)
	var (
		schemaRules = make([]*TableRule, 0, len(rules))
		tableRules  = make([]*TableRule, 0, len(rules))
	)
	// classify rules into schema level rules and table level
	// table level rules have highest priority
	for i := range rules {
		rule, ok := rules[i].(*TableRule)
		if !ok {
			return "", "", errors.NotValidf("table route rule %+v", rules[i])
		}

		if len(rule.TablePattern) == 0 {
			schemaRules = append(schemaRules, rule)
		} else {
			tableRules = append(tableRules, rule)
		}
	}

	var (
		targetSchema string
		targetTable  string
	)
	if len(table) == 0 || len(tableRules) == 0 {
		if len(schemaRules) > 1 {
			return "", "", errors.NotSupportedf("route %s/%s to rule set(%d)", schema, table, len(schemaRules))
		}

		if len(schemaRules) == 1 {
			targetSchema, targetTable = schemaRules[0].TargetSchema, schemaRules[0].TargetTable
		}
	} else {
		if len(tableRules) > 1 {
			return "", "", errors.NotSupportedf("route %s/%s to rule set(%d)", schema, table, len(tableRules))
		}

		targetSchema, targetTable = tableRules[0].TargetSchema, tableRules[0].TargetTable
	}

	if len(targetSchema) == 0 {
		targetSchema = schema
	}

	if len(targetTable) == 0 {
		targetTable = table
	}

	return targetSchema, targetTable, nil
}
