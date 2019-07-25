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

package loader

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/pingcap/dm/dm/config"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	tmysql "github.com/pingcap/parser/mysql"
	"go.uber.org/zap"
)

// Conn represents a live DB connection
type Conn struct {
	cfg *config.SubTaskConfig

	scope terror.ErrScope

	db *sql.DB
}

func (conn *Conn) querySQL(ctx *tcontext.Context, query string, maxRetry int, args ...interface{}) (*sql.Rows, error) {
	if conn == nil || conn.db == nil {
		return nil, terror.ErrDBUnExpect.Generate("database connection not valid")
	}

	var (
		err  error
		rows *sql.Rows
	)

	ctx.L().Debug("query statement", zap.String("sql", query), zap.Reflect("arguments", args))
	startTime := time.Now()
	defer func() {
		if err == nil {
			cost := time.Since(startTime)
			queryHistogram.WithLabelValues(conn.cfg.Name).Observe(cost.Seconds())
			if cost > 1 {
				ctx.L().Warn("query statement", zap.String("sql", query), zap.Reflect("arguments", args), zap.Duration("cost time", cost))
			}
		}
	}()

	for i := 0; i < maxRetry; i++ {
		if i > 0 {
			time.Sleep(time.Duration(i) * time.Second)
			ctx.L().Warn("query statement", zap.Int("retry", i), zap.String("sql", query), zap.Reflect("arguments", args))
		}
		rows, err = conn.db.QueryContext(ctx.Context(), query, args...)
		if err != nil {
			if isRetryableError(err) {
				ctx.L().Warn("query statement", zap.Int("retry", i), zap.String("sql", query), zap.Reflect("arguments", args), log.ShortError(err))
				continue
			}
			ctx.L().Error("query statement", zap.String("sql", query), zap.Reflect("arguments", args), log.ShortError(err))
			return nil, terror.DBErrorAdapt(err, terror.ErrDBQueryFailed)
		}
		return rows, nil
	}
	ctx.L().Error("query statement", zap.String("sql", query), zap.Reflect("arguments", args), log.ShortError(err))
	return nil, terror.DBErrorAdapt(err, terror.ErrDBQueryFailed)
}

func (conn *Conn) executeCheckpointSQL(ctx *tcontext.Context, stmt string, maxRetry int, args ...interface{}) error {
	if conn == nil || conn.db == nil {
		return terror.ErrDBUnExpect.Generate("database connection not valid")
	}

	var err error
	for i := 0; i < maxRetry; i++ {
		if i > 0 {
			ctx.L().Warn("execute statement", zap.Int("retry", i), zap.String("sql", stmt), zap.Reflect("arguments", args))
			time.Sleep(time.Duration(i) * time.Second)
		}
		_, err = conn.db.ExecContext(ctx.Context(), stmt, args...)
		if err != nil {
			if isRetryableError(err) {
				ctx.L().Warn("execute statement", zap.Int("retry", i), zap.String("sql", stmt), zap.Reflect("arguments", args), log.ShortError(err))
				continue
			}
			ctx.L().Error("execute statement", zap.String("sql", stmt), zap.Reflect("arguments", args), log.ShortError(err))
			return terror.DBErrorAdapt(err, terror.ErrDBExecuteFailed)
		}
		return nil
	}
	ctx.L().Error("execute statement", zap.String("sql", stmt), zap.Reflect("arguments", args), log.ShortError(err))
	return terror.DBErrorAdapt(err, terror.ErrDBExecuteFailed)
}

func (conn *Conn) executeSQL(ctx *tcontext.Context, sqls []string, enableRetry bool) error {
	return conn.executeSQLCustomRetry(ctx, sqls, enableRetry, isRetryableError)
}

func (conn *Conn) executeDDL(ctx *tcontext.Context, sqls []string, enableRetry bool) error {
	return conn.executeSQLCustomRetry(ctx, sqls, enableRetry, isDDLRetryableError)
}

func (conn *Conn) executeSQLCustomRetry(ctx *tcontext.Context, sqls []string, enableRetry bool, isRetryableFn func(err error) bool) error {
	if len(sqls) == 0 {
		return nil
	}

	if conn == nil || conn.db == nil {
		return terror.ErrDBUnExpect.Generate("database connection not valid")
	}

	var err error

	retryCount := 1
	if enableRetry {
		retryCount = maxRetryCount
	}

	for i := 0; i < retryCount; i++ {
		if i > 0 {
			ctx.L().Debug("execute statement", zap.Int("retry", i), zap.String("sqls", fmt.Sprintf("%-.200v", sqls)))
			time.Sleep(2 * time.Duration(i) * time.Second)
		}

		startTime := time.Now()
		err = executeSQLImp(ctx, conn.db, sqls)
		if err != nil {
			tidbExecutionErrorCounter.WithLabelValues(conn.cfg.Name).Inc()
			if isRetryableFn(err) {
				continue
			}
			return terror.DBErrorAdapt(err, terror.ErrDBExecuteFailed)
		}

		// update metrics
		cost := time.Since(startTime)
		txnHistogram.WithLabelValues(conn.cfg.Name).Observe(cost.Seconds())
		if cost > 1 {
			ctx.L().Warn("transaction execute successfully", zap.Duration("cost time", cost))
		}

		return nil
	}

	return terror.DBErrorAdapt(err, terror.ErrDBExecuteFailed)
}

func executeSQLImp(ctx *tcontext.Context, db *sql.DB, sqls []string) error {
	var (
		err error
		txn *sql.Tx
		res sql.Result
	)

	txn, err = db.Begin()
	if err != nil {
		ctx.L().Error("fail to begin a transaction", zap.String("sqls", fmt.Sprintf("%-.200v", sqls)), zap.Error(err))
		return terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}

	for i := range sqls {
		ctx.L().Debug("execute statement", zap.String("sqls", fmt.Sprintf("%-.200v", sqls[i])))
		res, err = txn.ExecContext(ctx.Context(), sqls[i])
		if err != nil {
			ctx.L().Warn("execute statement", zap.String("sqls", fmt.Sprintf("%-.200v", sqls[i])), log.ShortError(err))
			rerr := txn.Rollback()
			if rerr != nil {
				ctx.L().Error("fail rollback", log.ShortError(rerr))
			}
			return terror.DBErrorAdapt(err, terror.ErrDBExecuteFailed)
		}
		// check update checkpoint successful or not
		if i == 2 {
			row, err1 := res.RowsAffected()
			if err1 != nil {
				ctx.L().Warn("fail to get rows affected", zap.String("sqls", fmt.Sprintf("%-.200v", sqls[i])), log.ShortError(err1))
				continue
			}
			if row != 1 {
				ctx.L().Warn("update checkpoint", zap.Int64("affected rows", row))
			}
		}
	}

	err = txn.Commit()
	if err != nil {
		return terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}

	return nil
}

func createConn(cfg *config.SubTaskConfig) (*Conn, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&maxAllowedPacket=%d",
		cfg.To.User, cfg.To.Password, cfg.To.Host, cfg.To.Port, *cfg.To.MaxAllowedPacket)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, terror.WithScope(terror.DBErrorAdapt(err, terror.ErrDBDriverError), terror.ScopeDownstream)
	}

	return &Conn{db: db, cfg: cfg, scope: terror.ScopeDownstream}, nil
}

func closeConn(conn *Conn) error {
	if conn.db == nil {
		return nil
	}

	return terror.WithScope(terror.DBErrorAdapt(conn.db.Close(), terror.ErrDBDriverError), conn.scope)
}

func isErrDBExists(err error) bool {
	return isMySQLError(err, tmysql.ErrDBCreateExists)
}

func isErrTableExists(err error) bool {
	return isMySQLError(err, tmysql.ErrTableExists)
}

func isErrDupEntry(err error) bool {
	return isMySQLError(err, tmysql.ErrDupEntry)
}

func isRetryableError(err error) bool {
	err = errors.Cause(err)
	if isMySQLError(err, tmysql.ErrDupEntry) {
		return false
	}
	if isMySQLError(err, tmysql.ErrDataTooLong) {
		return false
	}
	return true
}

func isDDLRetryableError(err error) bool {
	if isErrTableExists(err) || isErrDBExists(err) {
		return false
	}
	return isRetryableError(err)
}

func isMySQLError(err error, code uint16) bool {
	err = errors.Cause(err)
	e, ok := err.(*mysql.MySQLError)
	return ok && e.Number == code
}
