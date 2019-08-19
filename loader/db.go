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
	"github.com/pingcap/failpoint"
	"strconv"
	"strings"
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
	cfg      *config.SubTaskConfig
	baseConn *utils.BaseConn
}

func (conn *Conn) querySQL(ctx *tcontext.Context, query string) (*sql.Rows, error) {
	if conn == nil || conn.baseConn == nil {
		return nil, terror.ErrDBUnExpect.Generate("database connection not valid")
	}

	startTime := time.Now()

	ret, err := conn.baseConn.NormalRetryOperation(
		ctx,
		func(ctx *tcontext.Context, _ int) (interface{}, error) {
			rows, err := conn.baseConn.QuerySQL(ctx, query)
			return rows, err
		},
		func(retryTime int, err error) bool {
			if isRetryableError(err) {
				ctx.L().Warn("query statement", zap.Int("retry", retryTime), zap.String("sql", query), log.ShortError(err))
				return true
			}
			return false
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

func (conn *Conn) executeSQL(ctx *tcontext.Context, ddl bool, queries []string, args ...[]interface{}) error {
	if len(queries) == 0 {
		return nil
	}

	sqls := make([]utils.SQL, 0, len(queries))
	for i, query := range queries {
		if i >= len(args) {
			sqls = append(sqls, utils.SQL{Query: query, Args: []interface{}{}})
		} else {
			sqls = append(sqls, utils.SQL{Query: query, Args: args[i]})
		}
	}
	retry := isRetryableError
	if ddl {
		retry = isDDLRetryableError
	}

	return conn.executeSQLCustomRetry(ctx, sqls, retry)
}

func (conn *Conn) executeSQLCustomRetry(ctx *tcontext.Context, sqls []utils.SQL, retryFn func(err error) bool) error {
	if conn == nil || conn.baseConn == nil {
		return terror.ErrDBUnExpect.Generate("database connection not valid")
	}

	_, err := conn.baseConn.NormalRetryOperation(
		ctx,
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
		},
		func(retryTime int, err error) bool {
			ctx.L().Debug("execute statement", zap.Int("retry", retryTime), zap.String("sqls", fmt.Sprintf("%-.200v", sqls)))
			time.Sleep(2 * time.Duration(retryTime) * time.Second)

			tidbExecutionErrorCounter.WithLabelValues(conn.cfg.Name).Inc()
			if retryFn(err) {
				return true
			}
			return false
		})
	return err
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
