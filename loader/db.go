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
	"github.com/pingcap/dm/pkg/utils"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/dm/dm/config"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	tmysql "github.com/pingcap/parser/mysql"
	"go.uber.org/zap"
)

// Conn represents a live DB connection
type Conn struct {
	cfg      *config.SubTaskConfig
	baseConn *utils.BaseConn
}

func (conn *Conn) querySQL(ctx *tcontext.Context, query string, maxRetry int) (*sql.Rows, error) {
	if conn == nil || conn.baseConn == nil {
		return nil, terror.ErrDBUnExpect.Generate("database connection not valid")
	}

	var (
		err  error
		rows *sql.Rows
	)

	startTime := time.Now()
	defer func() {
		if err == nil {
			cost := time.Since(startTime)
			queryHistogram.WithLabelValues(conn.cfg.Name).Observe(cost.Seconds())
			if cost > 1 {
				ctx.L().Warn("query statement", zap.String("sql", query), zap.Duration("cost time", cost))
			}
		}
	}()

	for i := 0; i < maxRetry; i++ {
		if i > 0 {
			time.Sleep(time.Duration(i) * time.Second)
			ctx.L().Warn("query statement", zap.Int("retry", i), zap.String("sql", query))
		}
		rows, err = conn.baseConn.QuerySQL(ctx, query)
		if err != nil {
			if isRetryableError(err) {
				ctx.L().Warn("query statement", zap.Int("retry", i), zap.String("sql", query), log.ShortError(err))
				continue
			}
			return nil, terror.DBErrorAdapt(err, terror.ErrDBQueryFailed, query)
		}
		return rows, nil
	}
	return nil, terror.DBErrorAdapt(err, terror.ErrDBQueryFailed, query)
}

func (conn *Conn) executeSQL(ctx *tcontext.Context, queries []string, args ...[]interface{}) error {
	sqls := make([]utils.SQL, 0, len(queries))
	for i, query := range queries {
		if i >= len(args) {
			sqls = append(sqls, utils.SQL{Query: query, Args: []interface{}{}})
		} else {
			sqls = append(sqls, utils.SQL{Query: query, Args: args[i]})
		}
	}
	return conn.executeSQLCustomRetry(ctx, sqls, isRetryableError)
}

func (conn *Conn) executeDDL(ctx *tcontext.Context, queries []string) error {
	sqls := make([]utils.SQL, 0, len(queries))
	for _, query := range queries {
		sqls = append(sqls, utils.SQL{Query: query, Args: []interface{}{}})
	}
	return conn.executeSQLCustomRetry(ctx, sqls, isDDLRetryableError)
}

func (conn *Conn) executeSQLCustomRetry(ctx *tcontext.Context, sqls []utils.SQL, isRetryableFn func(err error) bool) error {
	if len(sqls) == 0 {
		return nil
	}

	if conn == nil || conn.baseConn == nil {
		return terror.ErrDBUnExpect.Generate("database connection not valid")
	}

	var err error

	for i := 0; i < maxRetryCount; i++ {
		if i > 0 {
			ctx.L().Debug("execute statement", zap.Int("retry", i), zap.String("sqls", fmt.Sprintf("%-.200v", sqls)))
			time.Sleep(2 * time.Duration(i) * time.Second)
		}

		startTime := time.Now()
		err = executeSQLImp(ctx, conn.baseConn.DB, sqls)

		failpoint.Inject("LoadExecCreateTableFailed", func(val failpoint.Value) {
			items := strings.Split(val.(string), ",")
			if len(items) != 2 {
				ctx.L().Fatal("failpoint LoadExecCreateTableFailed's value is invalid", zap.String("val", val.(string)))
			}

			errCode, err1 := strconv.ParseUint(items[0], 10, 16)
			errNum, err2 := strconv.ParseInt(items[1], 10, 16)
			if err1 != nil || err2 != nil {
				ctx.L().Fatal("failpoint LoadExecCreateTableFailed's value is invalid", zap.String("val", val.(string)), zap.Strings("items", items), zap.Error(err1), zap.Error(err2))
			}

			if i < int(errNum) && len(sqls) == 1 && strings.Contains(sqls[0].Query, "CREATE TABLE") {
				err = tmysql.NewErr(uint16(errCode))
				ctx.L().Warn("executeSQLCustomRetry failed", zap.String("failpoint", "LoadExecCreateTableFailed"), zap.Error(err))
			}
		})

		if err != nil {
			tidbExecutionErrorCounter.WithLabelValues(conn.cfg.Name).Inc()
			if isRetryableFn(err) {
				continue
			}
			return terror.DBErrorAdapt(err, terror.ErrDBExecuteFailed, strings.Join([]string{}, ";"))
		}

		// update metrics
		cost := time.Since(startTime)
		txnHistogram.WithLabelValues(conn.cfg.Name).Observe(cost.Seconds())
		if cost > 1 {
			ctx.L().Warn("transaction execute successfully", zap.Duration("cost time", cost))
		}

		return nil
	}

	return terror.DBErrorAdapt(err, terror.ErrDBExecuteFailed, strings.Join([]string{}, ";"))
}

func executeSQLImp(ctx *tcontext.Context, db *sql.DB, sqls []utils.SQL) error {
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
		res, err = txn.ExecContext(ctx.Context(), sqls[i].Query, sqls[i].Args...)
		if err != nil {
			ctx.L().Warn("execute statement", zap.String("sqls", fmt.Sprintf("%-.200v", sqls[i])), log.ShortError(err))
			rerr := txn.Rollback()
			if rerr != nil {
				ctx.L().Error("fail rollback", log.ShortError(rerr))
			}
			return terror.DBErrorAdapt(err, terror.ErrDBExecuteFailed, sqls[i])
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
	baseConn := &utils.BaseConn{}
	err := baseConn.Init(dbDSN)
	if err != nil {
		return nil, terror.WithScope(terror.DBErrorAdapt(err, terror.ErrDBDriverError), terror.ScopeDownstream)
	}

	return &Conn{baseConn: baseConn, cfg: cfg}, nil
}

func closeConn(conn *Conn) error {
	if conn.baseConn == nil {
		return nil
	}

	return terror.DBErrorAdapt(conn.baseConn.Close(), terror.ErrDBDriverError)
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
