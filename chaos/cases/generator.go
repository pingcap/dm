// Copyright 2021 PingCAP, Inc.
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

package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	config2 "github.com/pingcap/dm/dm/config"
)

const (
	source1 = iota
	source2
	source3
)

// SQL represents sql statement for a source.
type SQL struct {
	statement string
	source    int
}

// SQLs represents sql statements in a group.
type SQLs []SQL

// Case represents a sqls test case.
type Case []SQLs

var (
	// ALL ADD COLUMN, ALL DROP COLUMN
	case1 = Case{
		{{"ALTER TABLE %s.%s ADD COLUMN case1 INT;", source1}},
		{{"ALTER TABLE %s.%s ADD COLUMN case1 INT;", source2}},
		{{"ALTER TABLE %s.%s ADD COLUMN case1 INT;", source3}},
		{{"ALTER TABLE %s.%s DROP COLUMN case1;", source1}},
		{{"ALTER TABLE %s.%s DROP COLUMN case1;", source2}},
		{{"ALTER TABLE %s.%s DROP COLUMN case1;", source3}},
	}
	// ADD COLUMN, DROP COLUMN for one source
	case2 = Case{
		{{"ALTER TABLE %s.%s ADD COLUMN case2 INT;", source1}},
		{{"ALTER TABLE %s.%s DROP COLUMN case2;", source1}},
	}
	// ADD columns out of order
	case3 = Case{
		{
			{"ALTER TABLE %s.%s DROP COLUMN case3_1;", source1},
			{"ALTER TABLE %s.%s DROP COLUMN case3_2;", source1},
			{"ALTER TABLE %s.%s DROP COLUMN case3_3;", source1},
			{"ALTER TABLE %s.%s DROP COLUMN case3_flag;", source1},
			{"ALTER TABLE %s.%s DROP COLUMN case3_1;", source2},
			{"ALTER TABLE %s.%s DROP COLUMN case3_2;", source2},
			{"ALTER TABLE %s.%s DROP COLUMN case3_3;", source2},
			{"ALTER TABLE %s.%s DROP COLUMN case3_flag;", source2},
			{"ALTER TABLE %s.%s DROP COLUMN case3_1;", source3},
			{"ALTER TABLE %s.%s DROP COLUMN case3_2;", source3},
			{"ALTER TABLE %s.%s DROP COLUMN case3_3;", source3},
			{"ALTER TABLE %s.%s DROP COLUMN case3_flag;", source3},
		},
		{
			{"ALTER TABLE %s.%s ADD COLUMN case3_flag INT;", source1},
			{"ALTER TABLE %s.%s ADD COLUMN case3_flag INT;", source2},
			{"ALTER TABLE %s.%s ADD COLUMN case3_flag INT;", source3},
		},
		{{"ALTER TABLE %s.%s ADD COLUMN case3_1 INT FIRST;", source1}},
		{{"ALTER TABLE %s.%s ADD COLUMN case3_2 INT AFTER id;", source2}},
		{{"ALTER TABLE %s.%s ADD COLUMN case3_3 INT AFTER case3_flag;", source3}},
		{{"ALTER TABLE %s.%s ADD COLUMN case3_2 INT AFTER id;", source1}},
		{{"ALTER TABLE %s.%s ADD COLUMN case3_3 INT AFTER case3_flag;", source2}},
		{{"ALTER TABLE %s.%s ADD COLUMN case3_1 INT FIRST;", source3}},
		{{"ALTER TABLE %s.%s ADD COLUMN case3_3 INT AFTER case3_flag;", source1}},
		{{"ALTER TABLE %s.%s ADD COLUMN case3_1 INT FIRST;", source2}},
		{{"ALTER TABLE %s.%s ADD COLUMN case3_2 INT AFTER id;", source3}},
	}
	// MULTIPLE ADD COLUMN out of order
	case4 = Case{
		{
			{"ALTER TABLE %s.%s DROP COLUMN case4_1, DROP COLUMN case4_2, DROP COLUMN case4_3;", source1},
			{"ALTER TABLE %s.%s DROP COLUMN case4_flag;", source1},
			{"ALTER TABLE %s.%s DROP COLUMN case4_1, DROP COLUMN case4_2, DROP COLUMN case4_3;", source2},
			{"ALTER TABLE %s.%s DROP COLUMN case4_flag;", source2},
			{"ALTER TABLE %s.%s DROP COLUMN case4_1, DROP COLUMN case4_2, DROP COLUMN case4_3;", source3},
			{"ALTER TABLE %s.%s DROP COLUMN case4_flag;", source3},
		},
		{
			{"ALTER TABLE %s.%s ADD COLUMN case4_flag INT;", source1},
			{"ALTER TABLE %s.%s ADD COLUMN case4_flag INT;", source2},
			{"ALTER TABLE %s.%s ADD COLUMN case4_flag INT;", source3},
		},
		{{"ALTER TABLE %s.%s ADD COLUMN case4_1 INT FIRST, ADD COLUMN case4_2 INT AFTER id, ADD COLUMN case4_3 INT AFTER case4_flag;", source1}},
		{{"ALTER TABLE %s.%s ADD COLUMN case4_2 INT AFTER id, ADD COLUMN case4_3 INT AFTER case4_flag, ADD COLUMN case4_1 INT FIRST;", source2}},
		{{"ALTER TABLE %s.%s ADD COLUMN case4_3 INT AFTER case4_flag, ADD COLUMN case4_1 INT FIRST, ADD COLUMN case4_2 INT AFTER id;", source3}},
	}
	// MULTIPLE ADD COLUMN vs ADD columns
	case5 = Case{
		{
			{"ALTER TABLE %s.%s DROP COLUMN case5_1;", source1},
			{"ALTER TABLE %s.%s DROP COLUMN case5_2;", source1},
			{"ALTER TABLE %s.%s DROP COLUMN case5_3;", source1},
			{"ALTER TABLE %s.%s DROP COLUMN case5_flag;", source1},
			{"ALTER TABLE %s.%s DROP COLUMN case5_1;", source2},
			{"ALTER TABLE %s.%s DROP COLUMN case5_2;", source2},
			{"ALTER TABLE %s.%s DROP COLUMN case5_3;", source2},
			{"ALTER TABLE %s.%s DROP COLUMN case5_flag;", source2},
			{"ALTER TABLE %s.%s DROP COLUMN case5_1;", source3},
			{"ALTER TABLE %s.%s DROP COLUMN case5_2;", source3},
			{"ALTER TABLE %s.%s DROP COLUMN case5_3;", source3},
			{"ALTER TABLE %s.%s DROP COLUMN case5_flag;", source3},
		},
		{
			{"ALTER TABLE %s.%s ADD COLUMN case5_flag INT;", source1},
			{"ALTER TABLE %s.%s ADD COLUMN case5_flag INT;", source2},
			{"ALTER TABLE %s.%s ADD COLUMN case5_flag INT;", source3},
		},
		{{"ALTER TABLE %s.%s ADD COLUMN case5_1 INT FIRST;", source1}},
		{{"ALTER TABLE %s.%s ADD COLUMN case5_2 INT AFTER id, ADD COLUMN case5_1 INT FIRST;", source2}},
		{{"ALTER TABLE %s.%s ADD COLUMN case5_3 INT AFTER case5_flag, ADD COLUMN case5_1 INT FIRST, ADD COLUMN case5_2 INT AFTER id;", source3}},
		{{"ALTER TABLE %s.%s ADD COLUMN case5_3 INT AFTER case5_flag;", source2}},
		{{"ALTER TABLE %s.%s ADD COLUMN case5_2 INT AFTER id, ADD COLUMN case5_3 INT AFTER case5_flag;", source1}},
	}

	cases = map[string][]Case{
		config2.ShardOptimistic: {
			case1,
			case2,
			case3,
			case4,
			case5,
		},
		config2.ShardPessimistic: {},
		"":                       {},
	}
)

// CaseGenerator generator test cases.
type CaseGenerator struct {
	testCases []Case
	sqlsChan  chan SQLs
	schema    string
	tables    []string
}

// NewCaseGenerator creates a new CaseGenerator instance.
func NewCaseGenerator(shardMode string) *CaseGenerator {
	g := &CaseGenerator{
		testCases: cases[shardMode],
		sqlsChan:  make(chan SQLs),
	}
	return g
}

// Start starts to generate sqls case
func (g *CaseGenerator) Start(ctx context.Context, schema string, tables []string) {
	rand.Seed(time.Now().UTC().UnixNano())
	g.schema = schema
	g.tables = tables
	go g.genSQLs(ctx)
}

func (g *CaseGenerator) genSQLs(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			for _, table := range g.tables {
				rand.Shuffle(len(g.testCases), func(i, j int) { g.testCases[i], g.testCases[j] = g.testCases[j], g.testCases[i] })
				casesNum := rand.Intn(len(g.testCases) + 1)
				for i := 0; i < casesNum; i++ {
					for _, sqls := range g.testCases[i] {
						fullSqls := make(SQLs, len(sqls))
						copy(fullSqls, sqls)
						for idx, sql := range fullSqls {
							fullSqls[idx].statement = fmt.Sprintf(sql.statement, g.schema, table)
						}
						g.sqlsChan <- fullSqls
					}
				}
			}
			g.sqlsChan <- nil
		}
	}
}

// GetSQLs gets sql from CaseGenerator
func (g *CaseGenerator) GetSQLs() SQLs {
	return <-g.sqlsChan
}
