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
	"github.com/pingcap/dm/pkg/baseconn"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/dm/dm/config"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/retry"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	tmysql "github.com/pingcap/parser/mysql"
	"go.uber.org/zap"
)

// Conn represents a live DB connection
type Conn struct {
	cfg      *config.SubTaskConfig
	baseConn *baseconn.BaseConn
}

func (conn *Conn) querySQL(ctx *tcontext.Context, query string) (*sql.Rows, error) {
	if conn == nil || conn.baseConn == nil {
		return nil, terror.ErrDBUnExpect.Generate("database connection not valid")
	}

	startTime := time.Now()

	params := retry.Params{
		RetryCount:         10,
		FirstRetryDuration: time.Second,
		RetryInterval:      retry.Stable,
		IsRetryableFn: func(retryTime int, err error) bool {
			if retry.IsLoaderRetryableError(err) {
				ctx.L().Warn("query statement", zap.Int("retry", retryTime), zap.String("sql", query), log.ShortError(err))
				return true
			}
			return false
		},
	}

	ret, _, err := conn.baseConn.RetryStrategy.DefaultRetryStrategy(
		ctx,
		params,
		func(ctx *tcontext.Context, _ int) (interface{}, error) {
			rows, err := conn.baseConn.QuerySQL(ctx, query)
			return rows, err
		})

	defer func() {
		if err == nil {
			cost := time.Since(startTime)
			queryHistogram.WithLabelValues(conn.cfg.Name).Observe(cost.Seconds())
			if cost > 1 {
				ctx.L().Warn("query statement", zap.String("sql", query), zap.Duration("cost time", cost))
			}
		}
	}()

	if err != nil {
		return nil, err
	}
	return ret.(*sql.Rows), nil
}

func (conn *Conn) executeDDL(ctx *tcontext.Context, queries []string, args ...[]interface{}) error {
	if len(queries) == 0 {
		return nil
	}

	sqls := make([]baseconn.SQL, 0, len(queries))
	for i, query := range queries {
		if i >= len(args) {
			sqls = append(sqls, baseconn.SQL{Query: query, Args: []interface{}{}})
		} else {
			sqls = append(sqls, baseconn.SQL{Query: query, Args: args[i]})
		}
	}

	return conn.executeSQLCustomRetry(ctx, sqls, retry.IsLoaderDDLRetryableError)
}

func (conn *Conn) executeSQL(ctx *tcontext.Context, queries []string, args ...[]interface{}) error {
	if len(queries) == 0 {
		return nil
	}

	sqls := make([]baseconn.SQL, 0, len(queries))
	for i, query := range queries {
		if i >= len(args) {
			sqls = append(sqls, baseconn.SQL{Query: query, Args: []interface{}{}})
		} else {
			sqls = append(sqls, baseconn.SQL{Query: query, Args: args[i]})
		}
	}

	return conn.executeSQLCustomRetry(ctx, sqls, retry.IsLoaderRetryableError)
}

func (conn *Conn) executeSQLCustomRetry(ctx *tcontext.Context, sqls []baseconn.SQL, retryFn func(err error) bool) error {
	if conn == nil || conn.baseConn == nil {
		return terror.ErrDBUnExpect.Generate("database connection not valid")
	}

	params := retry.Params{
		RetryCount:         10,
		FirstRetryDuration: 2 * time.Second,
		RetryInterval:      retry.LinearIncrease,
		IsRetryableFn: func(retryTime int, err error) bool {
			ctx.L().Debug("execute statement", zap.Int("retry", retryTime), zap.String("sqls", fmt.Sprintf("%-.200v", sqls)))
			tidbExecutionErrorCounter.WithLabelValues(conn.cfg.Name).Inc()
			if retryFn(err) {
				return true
			}
			return false
		},
	}
	_, _, err := conn.baseConn.RetryStrategy.DefaultRetryStrategy(
		ctx,
		params,
		func(ctx *tcontext.Context, retryTime int) (interface{}, error) {
			startTime := time.Now()
			_, err := conn.baseConn.ExecuteSQL(ctx, sqls)
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

				if retryTime < int(errNum) && len(sqls) == 1 && strings.Contains(sqls[0].Query, "CREATE TABLE") {
					err = tmysql.NewErr(uint16(errCode))
					ctx.L().Warn("executeSQLCustomRetry failed", zap.String("failpoint", "LoadExecCreateTableFailed"), zap.Error(err))
				}
			})
			cost := time.Since(startTime)
			txnHistogram.WithLabelValues(conn.cfg.Name).Observe(cost.Seconds())
			if cost > 1 {
				ctx.L().Warn("transaction execute successfully", zap.Duration("cost time", cost))
			}
			return nil, err
		})

	return err
}

func createConn(cfg *config.SubTaskConfig) (*Conn, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&maxAllowedPacket=%d",
		cfg.To.User, cfg.To.Password, cfg.To.Host, cfg.To.Port, *cfg.To.MaxAllowedPacket)
	baseConn, err := baseconn.NewBaseConn(dbDSN, &retry.FiniteRetryStrategy{})
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

func isMySQLError(err error, code uint16) bool {
	err = errors.Cause(err)
	e, ok := err.(*mysql.MySQLError)
	return ok && e.Number == code
}
