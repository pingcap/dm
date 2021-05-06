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

package utils

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"

	"github.com/pingcap/failpoint"
	tmysql "github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"go.uber.org/zap"
)

// TrimCtrlChars returns a slice of the string s with all leading
// and trailing control characters removed.
func TrimCtrlChars(s string) string {
	f := func(r rune) bool {
		// All entries in the ASCII table below code 32 (technically the C0 control code set) are of this kind,
		// including CR and LF used to separate lines of text. The code 127 (DEL) is also a control character.
		// Reference: https://en.wikipedia.org/wiki/Control_character
		return r < 32 || r == 127
	}

	return strings.TrimFunc(s, f)
}

// TrimQuoteMark tries to trim leading and tailing quote(") mark if exists
// only trim if leading and tailing quote matched as a pair.
func TrimQuoteMark(s string) string {
	if len(s) > 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}

// FetchAllDoTables returns all need to do tables after filtered (fetches from upstream MySQL).
func FetchAllDoTables(ctx context.Context, db *sql.DB, bw *filter.Filter) (map[string][]string, error) {
	schemas, err := dbutil.GetSchemas(ctx, db)

	failpoint.Inject("FetchAllDoTablesFailed", func(val failpoint.Value) {
		err = tmysql.NewErr(uint16(val.(int)))
		log.L().Warn("FetchAllDoTables failed", zap.String("failpoint", "FetchAllDoTablesFailed"), zap.Error(err))
	})

	if err != nil {
		return nil, terror.WithScope(err, terror.ScopeUpstream)
	}

	ftSchemas := make([]*filter.Table, 0, len(schemas))
	for _, schema := range schemas {
		if filter.IsSystemSchema(schema) {
			continue
		}
		ftSchemas = append(ftSchemas, &filter.Table{
			Schema: schema,
			Name:   "", // schema level
		})
	}
	ftSchemas = bw.ApplyOn(ftSchemas)
	if len(ftSchemas) == 0 {
		log.L().Warn("no schema need to sync")
		return nil, nil
	}

	schemaToTables := make(map[string][]string)
	for _, ftSchema := range ftSchemas {
		schema := ftSchema.Schema
		// use `GetTables` from tidb-tools, no view included
		tables, err := dbutil.GetTables(ctx, db, schema)
		if err != nil {
			return nil, terror.WithScope(terror.DBErrorAdapt(err, terror.ErrDBDriverError), terror.ScopeUpstream)
		}
		ftTables := make([]*filter.Table, 0, len(tables))
		for _, table := range tables {
			ftTables = append(ftTables, &filter.Table{
				Schema: schema,
				Name:   table,
			})
		}
		ftTables = bw.ApplyOn(ftTables)
		if len(ftTables) == 0 {
			log.L().Info("no tables need to sync", zap.String("schema", schema))
			continue // NOTE: should we still keep it as an empty elem?
		}
		tables = tables[:0]
		for _, ftTable := range ftTables {
			tables = append(tables, ftTable.Name)
		}
		schemaToTables[schema] = tables
	}

	return schemaToTables, nil
}

// FetchTargetDoTables returns all need to do tables after filtered and routed (fetches from upstream MySQL).
func FetchTargetDoTables(ctx context.Context, db *sql.DB, bw *filter.Filter, router *router.Table) (map[string][]*filter.Table, error) {
	// fetch tables from source and filter them
	sourceTables, err := FetchAllDoTables(ctx, db, bw)

	failpoint.Inject("FetchTargetDoTablesFailed", func(val failpoint.Value) {
		err = tmysql.NewErr(uint16(val.(int)))
		log.L().Warn("FetchTargetDoTables failed", zap.String("failpoint", "FetchTargetDoTablesFailed"), zap.Error(err))
	})

	if err != nil {
		return nil, err
	}

	mapper := make(map[string][]*filter.Table)
	for schema, tables := range sourceTables {
		for _, table := range tables {
			targetSchema, targetTable, err := router.Route(schema, table)
			if err != nil {
				return nil, terror.ErrGenTableRouter.Delegate(err)
			}

			targetTableName := dbutil.TableName(targetSchema, targetTable)
			mapper[targetTableName] = append(mapper[targetTableName], &filter.Table{
				Schema: schema,
				Name:   table,
			})
		}
	}

	return mapper, nil
}

// CompareShardingDDLs compares s and t ddls
// only concern in content, ignore order of ddl.
func CompareShardingDDLs(s, t []string) bool {
	if len(s) != len(t) {
		return false
	}

	ddls := make(map[string]struct{})
	for _, ddl := range s {
		ddls[ddl] = struct{}{}
	}

	for _, ddl := range t {
		if _, ok := ddls[ddl]; !ok {
			return false
		}
	}

	return true
}

// GenDDLLockID returns lock ID used in shard-DDL.
func GenDDLLockID(task, schema, table string) string {
	return fmt.Sprintf("%s-%s", task, dbutil.TableName(schema, table))
}

var lockIDPattern = regexp.MustCompile("(.*)\\-\\`(.*)\\`.\\`(.*)\\`")

// ExtractTaskFromLockID extract task from lockID.
func ExtractTaskFromLockID(lockID string) string {
	strs := lockIDPattern.FindStringSubmatch(lockID)
	// strs should be [full-lock-ID, task, db, table] if successful matched
	if len(strs) < 4 {
		return ""
	}
	return strs[1]
}

// ExtractDBAndTableFromLockID extract schema and table from lockID
func ExtractDBAndTableFromLockID(lockID string) (string, string) {
	strs := lockIDPattern.FindStringSubmatch(lockID)
	// strs should be [full-lock-ID, task, db, table] if successful matched
	if len(strs) < 4 {
		return "", ""
	}
	return strs[2], strs[3]
}

// NonRepeatStringsEqual is used to compare two un-ordered, non-repeat-element string slice is equal.
func NonRepeatStringsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[string]struct{}, len(a))
	for _, s := range a {
		m[s] = struct{}{}
	}
	for _, s := range b {
		if _, ok := m[s]; !ok {
			return false
		}
	}
	return true
}
