// Copyright 2016 PingCAP, Inc.
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

package route

import (
	"fmt"
	"sync"

	"github.com/juju/errors"
)

// 1. asterisk character (*, also called "star") matches zero or more characters,
//    for example, doc* matches doc and document but not dodo;
//    asterisk character must be in the end of wildcard word,
//    and there is only one asterisk in one wildcard word
// 2. the question mark ? matches exactly one character
const (
	// asterisk [ * ]
	asterisk = '*'
	// question mark [ ? ]
	question = '?'
)

const maxCacheNum = 1024

// TableRouter routes schema/table to target schema/table according to it's pattern
type TableRouter interface {
	// Insert will inserts one [patternSchema, patternTable, targetSchema, targetTable] rule pair into Router
	Insert(patternSchema, patternTable, targetSchema, targetTable string) error
	// Match will match all items that matched to the schema/table
	Match(schema, table string) (string, string)
	// Remove will remove the matched rule
	Remove(schema, table string) error
	// AllRules will returns all rules
	AllRules() map[string]map[string][]string
}

type itemList struct {
	items []*item
}

type trieRouter struct {
	sync.RWMutex

	cache map[string][]string
	root  *node

	isEmpty bool
}

type node struct {
	characters         map[byte]*item
	asterisk, question *item
}

type item struct {
	next   *node
	schema string
	table  string
	child  *node
	isLeaf bool
}

func newNode() *node {
	return &node{characters: make(map[byte]*item)}
}

// NewTrieRouter returns a trie Router
func NewTrieRouter() TableRouter {
	return &trieRouter{cache: make(map[string][]string), root: newNode(), isEmpty: true}
}

// Insert implements Router's Insert()
func (t *trieRouter) Insert(patternSchema, patternTable, targetSchema, targetTable string) error {
	if len(patternSchema) == 0 || len(targetSchema) == 0 {
		return errors.Errorf("pattern schema %s and target schema %s can't be empty", patternSchema, targetSchema)
	}

	t.Lock()
	// insert schame pattern
	schema, err := t.insert(t.root, patternSchema)
	if err != nil {
		t.Unlock()
		return errors.Trace(err)
	}
	if len(schema.schema) > 0 && targetSchema != schema.schema {
		t.Unlock()
		return errors.Errorf("can't overwrite target %s", schema.schema)
	}
	schema.schema = targetSchema

	// insert table pattern
	if len(patternTable) > 0 && len(targetTable) > 0 {
		if schema.child == nil {
			schema.child = newNode()
		}
		item, err := t.insert(schema.child, patternTable)
		if err != nil {
			t.Unlock()
			return errors.Trace(err)
		}
		if len(item.table) > 0 && item.schema != targetSchema && item.table != targetTable {
			t.Unlock()
			return errors.Errorf("can't overwrite target %s/%s", item.schema, item.table)
		}
		item.schema = targetSchema
		item.table = targetTable
	}
	t.Unlock()
	return nil
}

func (t *trieRouter) insert(root *node, pattern string) (*item, error) {
	n := root
	hadAsterisk := false
	var entity *item
	for i := range pattern {
		if hadAsterisk {
			return nil, errors.Errorf("pattern %s is invaild", pattern)
		}

		switch pattern[i] {
		case asterisk:
			entity = n.asterisk
			hadAsterisk = true
		case question:
			entity = n.question
		default:
			entity = n.characters[pattern[i]]
		}
		if entity == nil {
			entity = &item{}
			switch pattern[i] {
			case asterisk:
				n.asterisk = entity
			case question:
				n.question = entity
			default:
				n.characters[pattern[i]] = entity
			}
		}
		if entity.next == nil {
			entity.next = newNode()
		}
		n = entity.next
	}

	t.isEmpty = false
	entity.isLeaf = true

	return entity, nil
}

// Match implements Router's Match()
// if there are more than one matchs, just return one of them
func (t *trieRouter) Match(schema, table string) (string, string) {
	if len(schema) == 0 {
		return "", ""
	}

	t.RLock()
	if t.isEmpty {
		t.RUnlock()
		return "", ""
	}

	// try to find schema/table in cache
	targetStr := fmt.Sprintf("`%s`", schema)
	if len(table) > 0 {
		targetStr = fmt.Sprintf("`%s`.`%s`", schema, table)
	}
	targets, ok := t.cache[targetStr]
	t.RUnlock()
	if ok {
		return targets[0], targets[1]
	}

	t.Lock()
	// find matched schemas
	targetSchemas := &itemList{}
	targetSchema := ""
	t.matchNode(t.root, schema, targetSchemas)
	for _, s := range targetSchemas.items {
		targetTables := &itemList{}
		// if table is empty, just return first matched schema
		if len(table) == 0 {
			t.addToCache(targetStr, []string{s.schema, ""})
			t.Unlock()
			return s.schema, ""
		}
		targetSchema = s.schema
		// find matched tables
		t.matchNode(s.child, table, targetTables)
		if len(targetTables.items) > 0 {
			t.addToCache(targetStr, []string{targetTables.items[0].schema, targetTables.items[0].table})
			t.Unlock()
			return targetTables.items[0].schema, targetTables.items[0].table
		}
	}
	// not found table
	t.addToCache(targetStr, []string{targetSchema, ""})
	t.Unlock()
	return targetSchema, ""
}

// Remove implements Router's Remove(), but it do nothing now
func (t *trieRouter) Remove(schema, table string) error {
	return nil
}

// AllRules implements Router's AllRules
func (t *trieRouter) AllRules() map[string]map[string][]string {
	rules := make(map[string]map[string][]string)
	schemas := make(map[string]*item)
	var characters []byte
	t.RLock()
	t.travel(t.root, characters, schemas)
	for ks, schema := range schemas {
		rule, ok := rules[ks]
		if !ok {
			rule = make(map[string][]string)
		}
		tables := make(map[string]*item)
		characters = characters[:0]
		t.travel(schema.child, characters, tables)
		for kt, table := range tables {
			rule[kt] = []string{table.schema, table.table}
		}
		if len(tables) == 0 {
			rule[""] = []string{schema.schema, ""}
		}
		rules[ks] = rule
	}
	t.RUnlock()
	return rules
}

func (t *trieRouter) addToCache(key string, targets []string) {
	t.cache[key] = targets
	if len(t.cache) > maxCacheNum {
		for literal := range t.cache {
			delete(t.cache, literal)
			break
		}
	}
}

func (t *trieRouter) travel(n *node, characters []byte, rules map[string]*item) {
	if n == nil {
		return
	}

	if n.asterisk != nil {
		if n.asterisk.isLeaf {
			pattern := append(characters, asterisk)
			rules[string(pattern)] = n.asterisk
		}
	}

	if n.question != nil {
		pattern := append(characters, question)
		if n.question.isLeaf {
			rules[string(pattern)] = n.question
		}
		t.travel(n.question.next, pattern, rules)
	}

	for char, item := range n.characters {
		pattern := append(characters, char)
		if item.isLeaf {
			rules[string(pattern)] = item
		}
		t.travel(item.next, pattern, rules)
	}
}

func (t *trieRouter) matchNode(n *node, s string, res *itemList) {
	if n == nil {
		return
	}

	var (
		ok     bool
		entity *item
	)
	for i := range s {
		if n.asterisk != nil && n.asterisk.isLeaf {
			res.items = append(res.items, n.asterisk)
		}

		if n.question != nil {
			if i == len(s)-1 && n.question.isLeaf {
				res.items = append(res.items, n.question)
			}

			t.matchNode(n.question.next, s[i+1:], res)
		}

		entity, ok = n.characters[s[i]]
		if !ok {
			return
		}
		n = entity.next
	}

	if entity != nil && entity.isLeaf {
		res.items = append(res.items, entity)
	}

	if n.asterisk != nil && n.asterisk.isLeaf {
		res.items = append(res.items, n.asterisk)
	}
}
