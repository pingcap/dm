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

package filter

import (
	"regexp"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-tools/pkg/table-rule-selector"
)

// ActionType indicates how to handle matched items
type ActionType string

//  show that how to handle rules
const (
	Ignore ActionType = "Ignore"
	Do     ActionType = "Do"
)

// EventType is DML/DDL Event type
type EventType string

// show DML/DDL Events
const (
	// it indicates all dml/ddl events in rule
	AllEvent EventType = "all"
	// it indicates no any dml/ddl events in  rule,
	// and equals empty rule.DDLEvent/DMLEvent
	NoneEvent EventType = "none"

	InsertEvent EventType = "insert"
	UpdateEvent EventType = "update"
	DeleteEvent EventType = "delete"

	CreateDatabase EventType = "create database"
	DropDatabase   EventType = "drop database"
	CreateTable    EventType = "create table"
	DropTable      EventType = "drop table"
	TruncateTable  EventType = "truncate table"
	RenameTable    EventType = "rename table"
	CreateIndex    EventType = "create index"
	DropIndex      EventType = "drop index"
	AlertTable     EventType = "alter table"
	// if need, add more	AlertTableOption     = "alert table option"

	NullEvent EventType = ""
)

// BinlogEventRule is a rule to filter binlog events
type BinlogEventRule struct {
	SchemaPattern string      `json:"schema-pattern" toml:"schema-pattern" yaml:"schema-pattern"`
	TablePattern  string      `json:"table-pattern" toml:"table-pattern" yaml:"table-pattern"`
	DMLEvent      []EventType `json:"dml" toml:"dml" yaml:"dml"`
	DDLEvent      []EventType `json:"ddl" toml:"ddl" yaml:"ddl"`
	SQLPattern    []string    `json:"sql-pattern" toml:"sql-pattern" yaml:"sql-pattern"` // regular expression
	sqlRegularExp *regexp.Regexp

	Action ActionType `json:"action" toml:"action" yaml:"action"`
}

// Valid checks validity of rule.
// TODO: check validity of dml/ddl event.
func (b *BinlogEventRule) Valid() error {
	if len(b.SQLPattern) > 0 {
		reg, err := regexp.Compile("(?i)" + strings.Join(b.SQLPattern, "|"))
		if err != nil {
			return errors.Annotatef(err, "compile regular expression %+v", b.SQLPattern)
		}
		b.sqlRegularExp = reg
	}

	if b.Action != Do && b.Action != Ignore {
		return errors.Errorf("action of binlog event rule %+v should not be empty", b)
	}

	return nil
}

// BinlogEvent filters binlog events by given rules
type BinlogEvent struct {
	selector.Selector
}

// NewBinlogEvent returns a binlog event filter
func NewBinlogEvent(rules []*BinlogEventRule) (*BinlogEvent, error) {
	b := &BinlogEvent{
		Selector: selector.NewTrieSelector(),
	}

	for _, rule := range rules {
		if err := b.AddRule(rule); err != nil {
			return nil, errors.Annotatef(err, "initial rule %+v in binlog event filter", rule)
		}
	}

	return b, nil
}

// AddRule adds a rule into binlog event filter
func (b *BinlogEvent) AddRule(rule *BinlogEventRule) error {
	if b == nil || rule == nil {
		return nil
	}

	err := rule.Valid()
	if err != nil {
		return errors.Trace(err)
	}

	err = b.Insert(rule.SchemaPattern, rule.TablePattern, rule, false)
	if err != nil {
		return errors.Annotatef(err, "add rule %+v into binlog event filter", rule)
	}

	return nil
}

// UpdateRule updates binlog event filter rule
func (b *BinlogEvent) UpdateRule(rule *BinlogEventRule) error {
	if b == nil || rule == nil {
		return nil
	}

	err := rule.Valid()
	if err != nil {
		return errors.Trace(err)
	}

	err = b.Insert(rule.SchemaPattern, rule.TablePattern, rule, true)
	if err != nil {
		return errors.Annotatef(err, "update rule %+v into binlog event filter", rule)
	}

	return nil
}

// RemoveRule removes a rule from binlog event filter
func (b *BinlogEvent) RemoveRule(rule *BinlogEventRule) error {
	if b == nil || rule == nil {
		return nil
	}

	err := b.Remove(rule.SchemaPattern, rule.TablePattern)
	if err != nil {
		return errors.Annotatef(err, "remove rule %+v", rule)
	}

	return nil
}

// Filter filters events or queries by given rules
// returns action and error
func (b *BinlogEvent) Filter(schema, table string, dml, ddl EventType, rawQuery string) (ActionType, error) {
	if b == nil {
		return Do, nil
	}

	rules := b.Match(schema, table)
	if len(rules) == 0 {
		return Do, nil
	}

	for _, rule := range rules {
		binlogEventRule, ok := rule.(*BinlogEventRule)
		if !ok {
			return "", errors.NotValidf("rule %+v", rule)
		}

		matched := false
		if len(dml) > 0 {
			matched = b.matchEvent(dml, binlogEventRule.DMLEvent)
		} else if len(ddl) > 0 {
			matched = b.matchEvent(ddl, binlogEventRule.DDLEvent)
		} else if len(rawQuery) > 0 {
			if len(binlogEventRule.SQLPattern) == 0 {
				// sql pattern is disabled , just continue
				continue
			}

			matched = binlogEventRule.sqlRegularExp.FindStringIndex(rawQuery) != nil
		} else {
			if binlogEventRule.Action == Ignore { // Ignore has highest priority
				return Ignore, nil
			}
		}

		// ignore has highest priority
		if matched {
			if binlogEventRule.Action == Ignore {
				return Ignore, nil
			}
		} else {
			if binlogEventRule.Action == Do {
				return Ignore, nil
			}
		}
	}

	return Do, nil
}

func (b *BinlogEvent) matchEvent(event EventType, rules []EventType) bool {
	for _, rule := range rules {
		if rule == AllEvent {
			return true
		}

		if rule == NoneEvent {
			return false
		}

		if rule == event {
			return true
		}
	}

	return false
}
