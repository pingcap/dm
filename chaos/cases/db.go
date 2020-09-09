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
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb/errno"

	"github.com/pingcap/dm/pkg/conn"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/retry"
)

// dbConn holds a connection to a database and supports to reset the connection.
type dbConn struct {
	baseConn  *conn.BaseConn
	resetFunc func(ctx context.Context, baseConn *conn.BaseConn) (*conn.BaseConn, error)
}

// createDBConn creates a dbConn instance.
func createDBConn(ctx context.Context, db *conn.BaseDB) (*dbConn, error) {
	c, err := db.GetBaseConn(ctx)
	if err != nil {
		return nil, err
	}

	return &dbConn{
		baseConn: c,
		resetFunc: func(ctx context.Context, baseConn *conn.BaseConn) (*conn.BaseConn, error) {
			db.CloseBaseConn(baseConn)
			return db.GetBaseConn(ctx)
		},
	}, nil
}

// resetConn resets the underlying connection.
func (c *dbConn) resetConn(ctx context.Context) error {
	baseConn, err := c.resetFunc(ctx, c.baseConn)
	if err != nil {
		return err
	}
	c.baseConn = baseConn
	return nil
}

// execSQLs executes DDL or DML queries.
func (c *dbConn) execSQLs(ctx context.Context, queries ...string) error {
	params := retry.Params{
		RetryCount:         3,
		FirstRetryDuration: time.Second,
		BackoffStrategy:    retry.Stable,
		IsRetryableFn: func(retryTime int, err error) bool {
			if retry.IsConnectionError(err) {
				err = c.resetConn(ctx)
				if err != nil {
					return false
				}
				return true
			}
			return false
		},
	}

	_, _, err := c.baseConn.ApplyRetryStrategy(tcontext.NewContext(ctx, log.L()), params,
		func(tctx *tcontext.Context) (interface{}, error) {
			ret, err2 := c.baseConn.ExecuteSQLWithIgnoreError(tctx, nil, "chaos-cases", ignoreExecSQLError, queries)
			return ret, err2
		})
	return err
}

// dropDatabase drops the database if exists.
func dropDatabase(ctx context.Context, conn2 *dbConn, name string) error {
	query := fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbutil.ColumnName(name))
	return conn2.execSQLs(ctx, query)
}

// createDatabase creates a database if not exists.
func createDatabase(ctx context.Context, conn2 *dbConn, name string) error {
	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbutil.ColumnName(name))
	return conn2.execSQLs(ctx, query)
}

func ignoreExecSQLError(err error) bool {
	err = errors.Cause(err) // check the original error
	mysqlErr, ok := err.(*mysql.MySQLError)
	if !ok {
		return false
	}

	switch mysqlErr.Number {
	case errno.ErrParse: // HACK: the query generated by `go-sqlsmith` may be invalid, so we just ignore them.
		return true
	default:
		return false
	}
}
