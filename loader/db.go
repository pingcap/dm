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
	"github.com/pingcap/dm/pkg/log"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	tmysql "github.com/pingcap/parser/mysql"
	"go.uber.org/zap"
)

// Conn represents a live DB connection
type Conn struct {
	cfg *config.SubTaskConfig

	db *sql.DB
}

func (conn *Conn) querySQL(logger log.Logger, query string, maxRetry int, args ...interface{}) (*sql.Rows, error) {
	if conn == nil || conn.db == nil {
		return nil, errors.NotValidf("database connection")
	}

	var (
		err  error
		rows *sql.Rows
	)

	logger.Debug("query statement", zap.String("sql", query), zap.Reflect("arguments", args))
	startTime := time.Now()
	defer func() {
		if err == nil {
			cost := time.Since(startTime).Seconds()
			queryHistogram.WithLabelValues(conn.cfg.Name).Observe(cost)
			if cost > 1 {
				logger.Warn("query statement", zap.String("sql", query), zap.Reflect("arguments", args), zap.Float64("cost time", cost))
			}
		}
	}()

	for i := 0; i < maxRetry; i++ {
		if i > 0 {
			time.Sleep(time.Duration(i) * time.Second)
			logger.Warn("query statement", zap.Int("retry", i), zap.String("sql", query), zap.Reflect("arguments", args))
		}
		rows, err = conn.db.Query(query, args...)
		if err != nil {
			if isRetryableError(err) {
				logger.Warn("query statement", zap.Int("retry", i), zap.String("sql", query), zap.Reflect("arguments", args), log.ShortError(err))
				continue
			}
			return nil, errors.Trace(err)
		}
		return rows, nil
	}
	return nil, errors.Annotatef(err, "query statement, sql [%s] args [%v] failed", query, args)
}

func (conn *Conn) executeSQL2(logger log.Logger, stmt string, maxRetry int, args ...interface{}) error {
	if conn == nil || conn.db == nil {
		return errors.NotValidf("database connection")
	}

	var err error
	for i := 0; i < maxRetry; i++ {
		if i > 0 {
			logger.Warn("execute statement", zap.Int("retry", i), zap.String("sql", stmt), zap.Reflect("arguments", args))
			time.Sleep(time.Duration(i) * time.Second)
		}
		_, err = conn.db.Exec(stmt, args...)
		if err != nil {
			if isRetryableError(err) {
				logger.Warn("execute statement", zap.Int("retry", i), zap.String("sql", stmt), zap.Reflect("arguments", args), log.ShortError(err))
				continue
			}
			return errors.Trace(err)
		}
		return nil
	}
	return errors.Annotatef(err, "exec sql[%s] args[%v] failed", stmt, args)
}

func (conn *Conn) executeSQL(logger log.Logger, sqls []string, enableRetry bool) error {
	return conn.executeSQLCustomRetry(logger, sqls, enableRetry, isRetryableError)
}

func (conn *Conn) executeDDL(logger log.Logger, sqls []string, enableRetry bool) error {
	return conn.executeSQLCustomRetry(logger, sqls, enableRetry, isDDLRetryableError)
}

func (conn *Conn) executeSQLCustomRetry(logger log.Logger, sqls []string, enableRetry bool, isRetryableFn func(err error) bool) error {
	if len(sqls) == 0 {
		return nil
	}

	if conn == nil || conn.db == nil {
		return errors.NotValidf("database connection")
	}

	var err error

	retryCount := 1
	if enableRetry {
		retryCount = maxRetryCount
	}

	for i := 0; i < retryCount; i++ {
		if i > 0 {
			logger.Debug("execute statement", zap.Int("retry", i), zap.String("sqls", fmt.Sprintf("%-.200v", sqls)))
			time.Sleep(2 * time.Duration(i) * time.Second)
		}

		startTime := time.Now()
		err = executeSQLImp(logger, conn.db, sqls)
		if err != nil {
			tidbExecutionErrorCounter.WithLabelValues(conn.cfg.Name).Inc()
			if isRetryableFn(err) {
				continue
			}
			return errors.Trace(err)
		}

		// update metrics
		cost := time.Since(startTime).Seconds()
		txnHistogram.WithLabelValues(conn.cfg.Name).Observe(cost)
		if cost > 1 {
			logger.Warn("transaction execute successfully", zap.Float64("cost time", cost))
		}

		return nil
	}

	return errors.Trace(err)
}

func executeSQLImp(logger log.Logger, db *sql.DB, sqls []string) error {
	var (
		err error
		txn *sql.Tx
		res sql.Result
	)

	txn, err = db.Begin()
	if err != nil {
		logger.Error("fail to begin a transaction", zap.String("sqls", fmt.Sprintf("%-.200v", sqls)), zap.Error(err))
		return err
	}

	for i := range sqls {
		logger.Debug("execute statement", zap.String("sqls", fmt.Sprintf("%-.200v", sqls[i])))
		res, err = txn.Exec(sqls[i])
		if err != nil {
			logger.Warn("execute statement", zap.String("sqls", fmt.Sprintf("%-.200v", sqls[i])), log.ShortError(err))
			rerr := txn.Rollback()
			if rerr != nil {
				logger.Error("fail rollback", log.ShortError(rerr))
			}
			return err
		}
		// check update checkpoint successful or not
		if i == 2 {
			row, err1 := res.RowsAffected()
			if err1 != nil {
				logger.Warn("fail to get rows affected", zap.String("sqls", fmt.Sprintf("%-.200v", sqls[i])), log.ShortError(err1))
				continue
			}
			if row != 1 {
				logger.Warn("update checkpoint", zap.Int64("affected rows", row))
			}
		}
	}

	err = txn.Commit()
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func createConn(cfg *config.SubTaskConfig) (*Conn, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&maxAllowedPacket=%d",
		cfg.To.User, cfg.To.Password, cfg.To.Host, cfg.To.Port, *cfg.To.MaxAllowedPacket)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Conn{db: db, cfg: cfg}, nil
}

func closeConn(conn *Conn) error {
	if conn.db == nil {
		return nil
	}

	return errors.Trace(conn.db.Close())
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
