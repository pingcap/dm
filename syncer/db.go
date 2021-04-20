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

package syncer

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/conn"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/retry"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser"
	tmysql "github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"go.uber.org/zap"
)

// in MySQL, we can set `max_binlog_size` to control the max size of a binlog file.
// but this is not absolute:
// > A transaction is written in one chunk to the binary log, so it is never split between several binary logs.
// > Therefore, if you have big transactions, you might see binary log files larger than max_binlog_size.
// ref: https://dev.mysql.com/doc/refman/5.7/en/replication-options-binary-log.html#sysvar_max_binlog_size
// The max value of `max_binlog_size` is 1073741824 (1GB)
// but the actual file size still can be larger, and it may exceed the range of an uint32
// so, if we use go-mysql.Position(with uint32 Pos) to store the binlog size, it may become out of range.
// ps, use go-mysql.Position to store a position of binlog event (position of the next event) is enough.
type binlogSize struct {
	name string
	size int64
}

func createBaseDB(dbCfg config.DBConfig) (*conn.BaseDB, error) {
	db, err := conn.DefaultDBProvider.Apply(dbCfg)
	if err != nil {
		return nil, terror.WithScope(err, terror.ScopeDownstream)
	}
	return db, nil
}

// close baseDB to release all connection generated by this baseDB and this baseDB.
func closeBaseDB(logCtx *tcontext.Context, baseDB *conn.BaseDB) {
	if baseDB != nil {
		err := baseDB.Close()
		if err != nil {
			logCtx.L().Error("fail to close baseDB", log.ShortError(err))
		}
	}
}

// UpStreamConn connect to upstream DB
// Normally, we need to get some upstream information through some helper functions
// these helper functions are all easy query functions, so we use a pool of connections here
// maybe change to one connection some day.
type UpStreamConn struct {
	BaseDB *conn.BaseDB
}

func (conn *UpStreamConn) getMasterStatus(ctx context.Context, flavor string) (mysql.Position, gtid.Set, error) {
	pos, gtidSet, err := utils.GetMasterStatus(ctx, conn.BaseDB.DB, flavor)

	failpoint.Inject("GetMasterStatusFailed", func(val failpoint.Value) {
		err = tmysql.NewErr(uint16(val.(int)))
		log.L().Warn("GetMasterStatus failed", zap.String("failpoint", "GetMasterStatusFailed"), zap.Error(err))
	})

	return pos, gtidSet, err
}

func (conn *UpStreamConn) getServerUUID(ctx context.Context, flavor string) (string, error) {
	return utils.GetServerUUID(ctx, conn.BaseDB.DB, flavor)
}

func (conn *UpStreamConn) getParser(ctx context.Context) (*parser.Parser, error) {
	return utils.GetParser(ctx, conn.BaseDB.DB)
}

func (conn *UpStreamConn) killConn(ctx context.Context, connID uint32) error {
	return utils.KillConn(ctx, conn.BaseDB.DB, connID)
}

func (conn *UpStreamConn) fetchAllDoTables(ctx context.Context, bw *filter.Filter) (map[string][]string, error) {
	return utils.FetchAllDoTables(ctx, conn.BaseDB.DB, bw)
}

func (conn *UpStreamConn) countBinaryLogsSize(pos mysql.Position) (int64, error) {
	return countBinaryLogsSize(pos, conn.BaseDB.DB)
}

func createUpStreamConn(dbCfg config.DBConfig) (*UpStreamConn, error) {
	baseDB, err := createBaseDB(dbCfg)
	if err != nil {
		return nil, terror.WithScope(terror.DBErrorAdapt(err, terror.ErrDBDriverError), terror.ScopeUpstream)
	}
	return &UpStreamConn{BaseDB: baseDB}, nil
}

func closeUpstreamConn(tctx *tcontext.Context, conn *UpStreamConn) {
	if conn != nil {
		closeBaseDB(tctx, conn.BaseDB)
	}
}

// DBConn represents a live DB connection
// it's not thread-safe.
type DBConn struct {
	cfg      *config.SubTaskConfig
	baseConn *conn.BaseConn

	// generate new BaseConn and close old one
	resetBaseConnFn func(*tcontext.Context, *conn.BaseConn) (*conn.BaseConn, error)
}

// resetConn reset one worker connection from specify *BaseDB.
func (conn *DBConn) resetConn(tctx *tcontext.Context) error {
	baseConn, err := conn.resetBaseConnFn(tctx, conn.baseConn)
	if err != nil {
		return err
	}
	conn.baseConn = baseConn
	return nil
}

func (conn *DBConn) querySQL(tctx *tcontext.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if conn == nil || conn.baseConn == nil {
		return nil, terror.ErrDBUnExpect.Generate("database base connection not valid")
	}
	// nolint:dupl
	params := retry.Params{
		RetryCount:         10,
		FirstRetryDuration: retryTimeout,
		BackoffStrategy:    retry.Stable,
		IsRetryableFn: func(retryTime int, err error) bool {
			if retry.IsConnectionError(err) {
				err = conn.resetConn(tctx)
				if err != nil {
					tctx.L().Error("reset connection failed", zap.Int("retry", retryTime),
						zap.String("query", utils.TruncateInterface(query, -1)),
						zap.String("arguments", utils.TruncateInterface(args, -1)),
						log.ShortError(err))
					return false
				}
				sqlRetriesTotal.WithLabelValues("query", conn.cfg.Name).Add(1)
				return true
			}
			if dbutil.IsRetryableError(err) {
				tctx.L().Warn("query statement", zap.Int("retry", retryTime),
					zap.String("query", utils.TruncateString(query, -1)),
					zap.String("argument", utils.TruncateInterface(args, -1)),
					log.ShortError(err))
				sqlRetriesTotal.WithLabelValues("query", conn.cfg.Name).Add(1)
				return true
			}
			return false
		},
	}

	ret, _, err := conn.baseConn.ApplyRetryStrategy(
		tctx,
		params,
		func(ctx *tcontext.Context) (interface{}, error) {
			startTime := time.Now()
			ret, err := conn.baseConn.QuerySQL(ctx, query, args...)
			if err == nil {
				if ret.Err() != nil {
					return err, ret.Err()
				}
				cost := time.Since(startTime)
				queryHistogram.WithLabelValues(conn.cfg.Name).Observe(cost.Seconds())
				if cost.Seconds() > 1 {
					ctx.L().Warn("query statement",
						zap.String("query", utils.TruncateString(query, -1)),
						zap.String("argument", utils.TruncateInterface(args, -1)),
						zap.Duration("cost time", cost))
				}
			}
			return ret, err
		},
	)
	if err != nil {
		tctx.L().ErrorFilterContextCanceled("query statement failed after retry",
			zap.String("query", utils.TruncateString(query, -1)),
			zap.String("argument", utils.TruncateInterface(args, -1)),
			log.ShortError(err))
		return nil, err
	}
	return ret.(*sql.Rows), nil
}

func (conn *DBConn) executeSQLWithIgnore(tctx *tcontext.Context, ignoreError func(error) bool, queries []string, args ...[]interface{}) (int, error) {
	failpoint.Inject("ExecuteSQLWithIgnoreFailed", func(val failpoint.Value) {
		queryPattern := val.(string)
		if len(queries) == 1 && strings.Contains(queries[0], queryPattern) {
			tctx.L().Warn("executeSQLWithIgnore failed", zap.String("failpoint", "ExecuteSQLWithIgnoreFailed"))
			failpoint.Return(0, terror.ErrDBUnExpect.Generate("invalid connection"))
		}
	})

	if len(queries) == 0 {
		return 0, nil
	}

	if conn == nil || conn.baseConn == nil {
		return 0, terror.ErrDBUnExpect.Generate("database base connection not valid")
	}

	// nolint:dupl
	params := retry.Params{
		RetryCount:         100,
		FirstRetryDuration: retryTimeout,
		BackoffStrategy:    retry.Stable,
		IsRetryableFn: func(retryTime int, err error) bool {
			if retry.IsConnectionError(err) {
				err = conn.resetConn(tctx)
				if err != nil {
					tctx.L().Error("reset connection failed", zap.Int("retry", retryTime),
						zap.String("queries", utils.TruncateInterface(queries, -1)),
						zap.String("arguments", utils.TruncateInterface(args, -1)),
						log.ShortError(err))
					return false
				}
				sqlRetriesTotal.WithLabelValues("stmt_exec", conn.cfg.Name).Add(1)
				return true
			}
			if dbutil.IsRetryableError(err) {
				tctx.L().Warn("execute statements", zap.Int("retry", retryTime),
					zap.String("queries", utils.TruncateInterface(queries, -1)),
					zap.String("arguments", utils.TruncateInterface(args, -1)),
					log.ShortError(err))
				sqlRetriesTotal.WithLabelValues("stmt_exec", conn.cfg.Name).Add(1)
				return true
			}
			return false
		},
	}

	ret, _, err := conn.baseConn.ApplyRetryStrategy(
		tctx,
		params,
		func(ctx *tcontext.Context) (interface{}, error) {
			startTime := time.Now()
			ret, err := conn.baseConn.ExecuteSQLWithIgnoreError(ctx, stmtHistogram, conn.cfg.Name, ignoreError, queries, args...)
			if err == nil {
				cost := time.Since(startTime)
				txnHistogram.WithLabelValues(conn.cfg.Name).Observe(cost.Seconds())
				if cost.Seconds() > 1 {
					ctx.L().Warn("execute transaction",
						zap.String("query", utils.TruncateInterface(queries, -1)),
						zap.String("argument", utils.TruncateInterface(args, -1)),
						zap.Duration("cost time", cost))
				}
			}
			return ret, err
		})
	if err != nil {
		tctx.L().ErrorFilterContextCanceled("execute statements failed after retry",
			zap.String("queries", utils.TruncateInterface(queries, -1)),
			zap.String("arguments", utils.TruncateInterface(args, -1)),
			log.ShortError(err))
		return ret.(int), err
	}
	return ret.(int), nil
}

func (conn *DBConn) executeSQL(tctx *tcontext.Context, queries []string, args ...[]interface{}) (int, error) {
	return conn.executeSQLWithIgnore(tctx, nil, queries, args...)
}

func createConns(tctx *tcontext.Context, cfg *config.SubTaskConfig, dbCfg config.DBConfig, count int) (*conn.BaseDB, []*DBConn, error) {
	conns := make([]*DBConn, 0, count)
	baseDB, err := createBaseDB(dbCfg)
	if err != nil {
		return nil, nil, err
	}
	for i := 0; i < count; i++ {
		baseConn, err := baseDB.GetBaseConn(tctx.Context())
		if err != nil {
			closeBaseDB(tctx, baseDB)
			return nil, nil, terror.WithScope(err, terror.ScopeDownstream)
		}
		resetBaseConnFn := func(tctx *tcontext.Context, baseConn *conn.BaseConn) (*conn.BaseConn, error) {
			err := baseDB.CloseBaseConn(baseConn)
			if err != nil {
				tctx.L().Warn("failed to close baseConn in reset")
			}
			return baseDB.GetBaseConn(tctx.Context())
		}
		conns = append(conns, &DBConn{baseConn: baseConn, cfg: cfg, resetBaseConnFn: resetBaseConnFn})
	}
	return baseDB, conns, nil
}

func countBinaryLogsSize(fromFile mysql.Position, db *sql.DB) (int64, error) {
	files, err := getBinaryLogs(db)
	if err != nil {
		return 0, err
	}

	var total int64
	for _, file := range files {
		switch {
		case file.name < fromFile.Name:
			continue
		case file.name > fromFile.Name:
			total += file.size
		case file.name == fromFile.Name:
			if file.size > int64(fromFile.Pos) {
				total += file.size - int64(fromFile.Pos)
			}
		}
	}

	return total, nil
}

func getBinaryLogs(db *sql.DB) ([]binlogSize, error) {
	query := "SHOW BINARY LOGS"
	rows, err := db.Query(query)
	if err != nil {
		return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	files := make([]binlogSize, 0, 10)
	for rows.Next() {
		var file string
		var pos int64
		var nullPtr interface{}
		if len(rowColumns) == 2 {
			err = rows.Scan(&file, &pos)
		} else {
			err = rows.Scan(&file, &pos, &nullPtr)
		}
		if err != nil {
			return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
		}
		files = append(files, binlogSize{name: file, size: pos})
	}
	if rows.Err() != nil {
		return nil, terror.DBErrorAdapt(rows.Err(), terror.ErrDBDriverError)
	}
	return files, nil
}
