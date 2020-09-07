// Copyright 2020 PingCAP, Inc.
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

	"github.com/pingcap/tidb-tools/pkg/dbutil"

	"github.com/pingcap/dm/pkg/conn"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/log"
)

// dropDatabase drops the database if exists.
func dropDatabase(ctx context.Context, conn2 *conn.BaseConn, name string) error {
	query := fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbutil.ColumnName(name))
	return execSQLs(ctx, conn2, query)
}

// createDatabase creates a database if not exists.
func createDatabase(ctx context.Context, conn2 *conn.BaseConn, name string) error {
	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbutil.ColumnName(name))
	return execSQLs(ctx, conn2, query)
}

// execSQLs executes DDL or DML queries.
func execSQLs(ctx context.Context, conn2 *conn.BaseConn, queries ...string) error {
	_, err := conn2.ExecuteSQL(tcontext.NewContext(ctx, log.L()), nil, "chaos-cases", queries)
	return err
}
