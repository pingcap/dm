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

package baseconn

import (
	"database/sql"

	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/retry"
	"github.com/pingcap/dm/pkg/terror"

	"go.uber.org/zap"
)

// BaseConn wraps a connection to DB
type BaseConn struct {
	DB *sql.DB

	// for reset
	DSN string

	RetryStrategy retry.Strategy
}

// SQL is format sql job
type SQL struct {
	Query string
	Args  []interface{}
}

// NewBaseConn builds BaseConn to connect real DB
func NewBaseConn(dbDSN string, strategy retry.Strategy) (*BaseConn, error) {
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, terror.ErrDBDriverError.Delegate(err)
	}
	err = db.Ping()
	if err != nil {
		return nil, terror.ErrDBDriverError.Delegate(err)
	}
	return &BaseConn{db, dbDSN, strategy}, nil
}

// SetRetryStrategy set retry strategy for baseConn
func (conn *BaseConn) SetRetryStrategy(strategy retry.Strategy) error {
	if conn == nil {
		return terror.ErrDBUnExpect.Generate("database connection not valid")
	}
	conn.RetryStrategy = strategy
	return nil
}

// ResetConn generates new *DB with new connection pool to take place old one
func (conn *BaseConn) ResetConn() error {
	if conn == nil {
		return terror.ErrDBUnExpect.Generate("database connection not valid")
	}
	db, err := sql.Open("mysql", conn.DSN)
	if err != nil {
		return terror.ErrDBDriverError.Delegate(err)
	}
	err = db.Ping()
	if err != nil {
		return terror.ErrDBDriverError.Delegate(err)
	}
	if conn.DB != nil {
		conn.DB.Close()
	}
	conn.DB = db
	return nil
}

// QuerySQL defines query statement, and connect to real DB
func (conn *BaseConn) QuerySQL(tctx *tcontext.Context, query string) (*sql.Rows, error) {
	if conn == nil || conn.DB == nil {
		return nil, terror.ErrDBUnExpect.Generate("database connection not valid")
	}
	tctx.L().Debug("query statement", zap.String("query", query))

	rows, err := conn.DB.QueryContext(tctx.Context(), query)

	if err != nil {
		tctx.L().Error("query statement failed", zap.String("query", query), log.ShortError(err))
		return nil, terror.ErrDBQueryFailed.Delegate(err, query)
	}
	return rows, nil
}

// ExecuteSQL executes sql on real DB,
// return
// 1. failed: (the index of sqls executed error, error)
// 2. succeed: (len(sqls), nil)
func (conn *BaseConn) ExecuteSQL(tctx *tcontext.Context, sqls []SQL) (int, error) {
	if len(sqls) == 0 {
		return 0, nil
	}
	if conn == nil || conn.DB == nil {
		return 0, terror.ErrDBUnExpect.Generate("database connection not valid")
	}

	txn, err := conn.DB.Begin()

	if err != nil {
		return 0, terror.ErrDBExecuteFailed.Delegate(err, "begin")
	}

	l := len(sqls)

	for i := range sqls {
		tctx.L().Debug("execute statement", zap.String("query", sqls[i].Query), zap.Reflect("argument", sqls[i].Args))

		_, err = txn.ExecContext(tctx.Context(), sqls[i].Query, sqls[i].Args...)
		if err != nil {
			tctx.L().Error("execute statement failed", zap.String("query", sqls[i].Query), zap.Reflect("argument", sqls[i].Args), log.ShortError(err))
			rerr := txn.Rollback()
			if rerr != nil {
				tctx.L().Error("rollback failed", zap.String("query", sqls[i].Query), zap.Reflect("argument", sqls[i].Args), log.ShortError(rerr))
			}
			// we should return the exec err, instead of the rollback rerr.
			return i, terror.ErrDBExecuteFailed.Delegate(err, sqls[i].Query)
		}
	}
	err = txn.Commit()
	if err != nil {
		return l, terror.ErrDBExecuteFailed.Delegate(err, "commit")
	}
	return l, nil
}

// Close release DB resource
func (conn *BaseConn) Close() error {
	if conn == nil || conn.DB == nil {
		return nil
	}
	return terror.ErrDBUnExpect.Delegate(conn.DB.Close(), "close")
}
