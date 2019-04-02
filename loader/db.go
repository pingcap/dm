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

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/errors"
	tmysql "github.com/pingcap/parser/mysql"
)

// Conn represents a live DB connection
type Conn struct {
	cfg *config.SubTaskConfig

	db *sql.DB
}

func (conn *Conn) querySQL(query string, maxRetry int, args ...interface{}) (*sql.Rows, error) {
	if conn == nil || conn.db == nil {
		return nil, errors.NotValidf("database connection")
	}

	var (
		err  error
		rows *sql.Rows
	)

	log.Debugf("[query][sql]%s, args %v", query, args)
	startTime := time.Now()
	defer func() {
		if err == nil {
			cost := time.Since(startTime).Seconds()
			queryHistogram.WithLabelValues(conn.cfg.Name).Observe(cost)
			if cost > 1 {
				log.Warnf("query %s [args: %v] costs %f seconds", query, args, cost)
			}
		}
	}()

	for i := 0; i < maxRetry; i++ {
		if i > 0 {
			time.Sleep(time.Duration(i) * time.Second)
			log.Warnf("sql query retry: %d sql: [%s] args: [%v]", i, query, args)
		}
		rows, err = conn.db.Query(query, args...)
		if err != nil {
			if isRetryableError(err) {
				log.Warnf("sql: [%s] args: [%v] query error: [%v]", query, args, err)
				continue
			}
			return nil, errors.Trace(err)
		}
		return rows, nil
	}
	return nil, errors.Annotatef(err, "query sql [%s] args [%v] failed", query, args)
}

func (conn *Conn) executeSQL2(stmt string, maxRetry int, args ...interface{}) error {
	if conn == nil || conn.db == nil {
		return errors.NotValidf("database connection")
	}

	var err error
	for i := 0; i < maxRetry; i++ {
		if i > 0 {
			log.Warnf("sql exec retry %d: sql: [%s] args: [%v]", i, stmt, args)
			time.Sleep(time.Duration(i) * time.Second)
		}
		_, err = conn.db.Exec(stmt, args...)
		if err != nil {
			if isRetryableError(err) {
				log.Warnf("sql: [%s] args: [%v] query error: [%v]", stmt, args, err)
				continue
			}
			return errors.Trace(err)
		}
		return nil
	}
	return errors.Annotatef(err, "exec sql[%s] args[%v] failed", stmt, args)
}

func (conn *Conn) executeSQL(sqls []string, enableRetry bool) error {
	return conn.executeSQLCustomRetry(sqls, enableRetry, isRetryableError)
}

func (conn *Conn) executeDDL(sqls []string, enableRetry bool) error {
	return conn.executeSQLCustomRetry(sqls, enableRetry, isDDLRetryableError)
}

func (conn *Conn) executeSQLCustomRetry(sqls []string, enableRetry bool, isRetryableFn func(err error) bool) error {
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
			log.Warnf("exec sql retry %d - %-.100v", i, sqls)
			time.Sleep(2 * time.Duration(i) * time.Second)
		}

		startTime := time.Now()
		err = executeSQLImp(conn.db, sqls)
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
			log.Warnf("transaction execution costs %f seconds", cost)
		}

		return nil
	}

	return errors.Trace(err)
}

func executeSQLImp(db *sql.DB, sqls []string) error {
	var (
		err error
		txn *sql.Tx
		res sql.Result
	)

	txn, err = db.Begin()
	if err != nil {
		log.Errorf("exec sqls[%-.100v] begin failed %v", sqls, errors.ErrorStack(err))
		return err
	}

	for i := range sqls {
		log.Debugf("[exec][sql]%-.200v", sqls[i])
		res, err = txn.Exec(sqls[i])
		if err != nil {
			log.Warnf("[exec][sql]%-.100v[error]%v", sqls[i], err)
			rerr := txn.Rollback()
			if rerr != nil {
				log.Errorf("[exec][sql]%-.100s[error]%v", sqls, rerr)
			}
			return err
		}
		// check update checkpoint successful or not
		if i == 2 {
			row, err1 := res.RowsAffected()
			if err1 != nil {
				log.Warnf("exec sql %s get rows affected error %s", sqls[i], err1)
				continue
			}
			if row != 1 {
				log.Warnf("update checkpoint affected rows %d", row)
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

func isErrTableNotExists(err error) bool {
	return isMySQLError(err, tmysql.ErrNoSuchTable)
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
