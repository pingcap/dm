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
	"fmt"

	"github.com/pingcap/failpoint"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/config"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/retry"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

// BaseConn wraps a connection to DB
type BaseConn struct {
	DB *sql.DB

	// for reset
	DSN string

	RetryStrategy retry.Strategy

	RawDBCfg *RawDBConfig
}

// RawDBConfig contains some low level database config
type RawDBConfig struct {
	MaxIdleConns int
}

// DefaultRawDBConfig returns a default raw database config
func DefaultRawDBConfig() *RawDBConfig {
	return &RawDBConfig{
		MaxIdleConns: 2,
	}
}

// NewBaseConn builds BaseConn to connect real DB
func NewBaseConn(dbDSN string, strategy retry.Strategy, rawDBCfg *RawDBConfig) (*BaseConn, error) {
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, terror.ErrDBDriverError.Delegate(err)
	}
	// set max idle connection limit before any database call
	db.SetMaxIdleConns(rawDBCfg.MaxIdleConns)
	err = db.Ping()
	if err != nil {
		db.Close()
		return nil, terror.ErrDBDriverError.Delegate(err)
	}
	if strategy == nil {
		strategy = &retry.FiniteRetryStrategy{}
	}
	return &BaseConn{db, dbDSN, strategy, rawDBCfg}, nil
}

// CreateBaseConn builds BaseConn through DBConfig to connect real DB
func CreateBaseConn(dbCfg config.DBConfig, timeout string, rawDBCfg *RawDBConfig) (*BaseConn, error) {
	failpoint.Inject("createEmptyBaseConn", func(_ failpoint.Value) {
		log.L().Info("create mock baseConn which is nil", zap.String("failpoint", "createEmptyBaseConn"))
		failpoint.Return(&BaseConn{}, nil)
	})

	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&interpolateParams=true&readTimeout=%s&maxAllowedPacket=%d",
		dbCfg.User, dbCfg.Password, dbCfg.Host, dbCfg.Port, timeout, *dbCfg.MaxAllowedPacket)
	baseConn, err := NewBaseConn(dbDSN, &retry.FiniteRetryStrategy{}, rawDBCfg)
	if err != nil {
		return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	return baseConn, nil
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
func (conn *BaseConn) ResetConn(tctx *tcontext.Context) error {
	if conn == nil {
		return terror.ErrDBUnExpect.Generate("database connection not valid")
	}
	db, err := sql.Open("mysql", conn.DSN)
	if err != nil {
		return terror.ErrDBDriverError.Delegate(err)
	}
	// set max idle connection limit before any database call
	db.SetMaxIdleConns(conn.RawDBCfg.MaxIdleConns)
	err = db.Ping()
	if err != nil {
		db.Close()
		return terror.ErrDBDriverError.Delegate(err)
	}
	if conn.DB != nil {
		err := conn.DB.Close()
		if err != nil {
			tctx.L().Warn("reset connection", log.ShortError(err))
		}
	}
	conn.DB = db
	return nil
}

// QuerySQL defines query statement, and connect to real DB
func (conn *BaseConn) QuerySQL(tctx *tcontext.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if conn == nil || conn.DB == nil {
		return nil, terror.ErrDBUnExpect.Generate("database connection not valid")
	}
	tctx.L().Debug("query statement",
		zap.String("query", utils.TruncateString(query, -1)),
		zap.String("argument", utils.TruncateInterface(args, -1)))

	rows, err := conn.DB.QueryContext(tctx.Context(), query, args...)

	if err != nil {
		tctx.L().Error("query statement failed",
			zap.String("query", utils.TruncateString(query, -1)),
			zap.String("argument", utils.TruncateInterface(args, -1)),
			log.ShortError(err))
		return nil, terror.ErrDBQueryFailed.Delegate(err, utils.TruncateString(query, -1))
	}
	return rows, nil
}

// ExecuteSQLWithIgnoreError executes sql on real DB, and will ignore some error and continue execute the next query.
// return
// 1. failed: (the index of sqls executed error, error)
// 2. succeed: (len(sqls), nil)
func (conn *BaseConn) ExecuteSQLWithIgnoreError(tctx *tcontext.Context, ignoreErr func(error) bool, queries []string, args ...[]interface{}) (int, error) {
	if len(queries) == 0 {
		return 0, nil
	}
	if conn == nil || conn.DB == nil {
		return 0, terror.ErrDBUnExpect.Generate("database connection not valid")
	}

	txn, err := conn.DB.Begin()

	if err != nil {
		return 0, terror.ErrDBExecuteFailed.Delegate(err, "begin")
	}

	l := len(queries)

	for i, query := range queries {
		var arg []interface{}
		if len(args) > i {
			arg = args[i]
		}

		tctx.L().Debug("execute statement",
			zap.String("query", utils.TruncateString(query, -1)),
			zap.String("argument", utils.TruncateInterface(arg, -1)))

		_, err = txn.ExecContext(tctx.Context(), query, arg...)
		if err != nil {
			if ignoreErr != nil && ignoreErr(err) {
				tctx.L().Warn("execute statement failed and will ignore this error",
					zap.String("query", utils.TruncateString(query, -1)),
					zap.String("argument", utils.TruncateInterface(arg, -1)),
					log.ShortError(err))
				continue
			}

			tctx.L().Error("execute statement failed",
				zap.String("query", utils.TruncateString(query, -1)),
				zap.String("argument", utils.TruncateInterface(arg, -1)), log.ShortError(err))

			rerr := txn.Rollback()
			if rerr != nil {
				tctx.L().Error("rollback failed",
					zap.String("query", utils.TruncateString(query, -1)),
					zap.String("argument", utils.TruncateInterface(arg, -1)),
					log.ShortError(rerr))
			}
			// we should return the exec err, instead of the rollback rerr.
			return i, terror.ErrDBExecuteFailed.Delegate(err, utils.TruncateString(query, -1))
		}
	}
	err = txn.Commit()
	if err != nil {
		return l - 1, terror.ErrDBExecuteFailed.Delegate(err, "commit") // mark failed on the last one
	}
	return l, nil
}

// ExecuteSQL executes sql on real DB,
// return
// 1. failed: (the index of sqls executed error, error)
// 2. succeed: (len(sqls), nil)
func (conn *BaseConn) ExecuteSQL(tctx *tcontext.Context, queries []string, args ...[]interface{}) (int, error) {
	return conn.ExecuteSQLWithIgnoreError(tctx, nil, queries, args...)
}

// ApplyRetryStrategy apply specify strategy for BaseConn
func (conn *BaseConn) ApplyRetryStrategy(tctx *tcontext.Context, params retry.Params,
	operateFn func(*tcontext.Context) (interface{}, error)) (interface{}, int, error) {
	return conn.RetryStrategy.Apply(tctx, params, operateFn)
}

// Close release DB resource
func (conn *BaseConn) Close() error {
	if conn == nil || conn.DB == nil {
		return nil
	}
	return terror.ErrDBUnExpect.Delegate(conn.DB.Close(), "close")
}
