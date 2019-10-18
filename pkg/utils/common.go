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
	"strings"
	"time"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"

	"github.com/pingcap/failpoint"
	tmysql "github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"go.uber.org/zap"
)

// move to tidb-tools later

const (
	maxRetryCount = 3
	retryTimeout  = 3 * time.Second
)

// ExtractTable extracts schema and table from `schema`.`table`
func ExtractTable(name string) (string, string, error) {
	parts := strings.Split(name, "`.`")
	if len(parts) != 2 {
		return "", "", terror.ErrSchemaTableNameNotValid.Generate(name)
	}

	return strings.TrimLeft(parts[0], "`"), strings.TrimRight(parts[1], "`"), nil
}

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

// FetchAllDoTables returns all need to do tables after filtered (fetches from upstream MySQL)
func FetchAllDoTables(db *sql.DB, bw *filter.Filter) (map[string][]string, error) {
	schemas, err := getSchemas(db, maxRetryCount)

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
		tables, err := dbutil.GetTables(context.Background(), db, schema)
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

// FetchTargetDoTables returns all need to do tables after filtered and routed (fetches from upstream MySQL)
func FetchTargetDoTables(db *sql.DB, bw *filter.Filter, router *router.Table) (map[string][]*filter.Table, error) {
	// fetch tables from source and filter them
	sourceTables, err := FetchAllDoTables(db, bw)

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

func getSchemas(db *sql.DB, maxRetry int) ([]string, error) {
	query := "SHOW DATABASES"
	rows, err := querySQL(db, query, maxRetry)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// show an example.
	/*
		mysql> SHOW DATABASES;
		+--------------------+
		| Database           |
		+--------------------+
		| information_schema |
		| mysql              |
		| performance_schema |
		| sys                |
		| test_db            |
		+--------------------+
	*/
	schemas := make([]string, 0, 10)
	for rows.Next() {
		var schema string
		err = rows.Scan(&schema)
		if err != nil {
			return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
		}
		schemas = append(schemas, schema)
	}
	return schemas, terror.DBErrorAdapt(rows.Err(), terror.ErrDBDriverError)
}

func querySQL(db *sql.DB, query string, maxRetry int) (*sql.Rows, error) {
	var (
		err  error
		rows *sql.Rows
	)

	for i := 0; i < maxRetry; i++ {
		if i > 0 {
			log.L().Warn("query retry", zap.Int("retry number", i), zap.String("query", TruncateString(query, -1)))
			time.Sleep(retryTimeout)
		} else {
			log.L().Debug("query sql", zap.String("query", TruncateString(query, -1)))
		}

		rows, err = db.Query(query)
		if err != nil {
			log.L().Warn("query retry", zap.String("query", TruncateString(query, -1)), log.ShortError(err))
			continue
		}

		return rows, nil
	}

	log.L().Error("query failed", zap.String("query", TruncateString(query, -1)), zap.Error(err))
	return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
}

// CompareShardingDDLs compares s and t ddls
// only concern in content, ignore order of ddl
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
