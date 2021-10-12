package dbconn

import (
	"database/sql"
	"strings"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/conn"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/retry"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/pingcap/dm/syncer/metrics"
)

var retryTimeout = 3 * time.Second

// CreateBaseDB creates a db from config.
func CreateBaseDB(dbCfg *config.DBConfig) (*conn.BaseDB, error) {
	db, err := conn.DefaultDBProvider.Apply(dbCfg)
	if err != nil {
		return nil, terror.WithScope(err, terror.ScopeDownstream)
	}
	return db, nil
}

// CloseBaseDB closes baseDB to release all connection generated by this baseDB and this baseDB.
func CloseBaseDB(logCtx *tcontext.Context, baseDB *conn.BaseDB) {
	if baseDB != nil {
		err := baseDB.Close()
		if err != nil {
			logCtx.L().Error("fail to close baseDB", log.ShortError(err))
		}
	}
}

// DBConn represents a live DB connection
// it's not thread-safe.
type DBConn struct {
	Cfg      *config.SubTaskConfig
	BaseConn *conn.BaseConn

	// generate new BaseConn and close old one
	ResetBaseConnFn func(*tcontext.Context, *conn.BaseConn) (*conn.BaseConn, error)
}

// ResetConn reset one worker connection from specify *BaseDB.
func (conn *DBConn) ResetConn(tctx *tcontext.Context) error {
	baseConn, err := conn.ResetBaseConnFn(tctx, conn.BaseConn)
	if err != nil {
		return err
	}
	conn.BaseConn = baseConn
	return nil
}

// QuerySQL does one query.
func (conn *DBConn) QuerySQL(tctx *tcontext.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if conn == nil || conn.BaseConn == nil {
		return nil, terror.ErrDBUnExpect.Generate("database base connection not valid")
	}
	// nolint:dupl
	params := retry.Params{
		RetryCount:         10,
		FirstRetryDuration: retryTimeout,
		BackoffStrategy:    retry.Stable,
		IsRetryableFn: func(retryTime int, err error) bool {
			if retry.IsConnectionError(err) {
				err = conn.ResetConn(tctx)
				if err != nil {
					tctx.L().Error("reset connection failed", zap.Int("retry", retryTime),
						zap.String("query", utils.TruncateInterface(query, -1)),
						zap.String("arguments", utils.TruncateInterface(args, -1)),
						log.ShortError(err))
					return false
				}
				metrics.SQLRetriesTotal.WithLabelValues("query", conn.Cfg.Name).Add(1)
				return true
			}
			if dbutil.IsRetryableError(err) {
				tctx.L().Warn("query statement", zap.Int("retry", retryTime),
					zap.String("query", utils.TruncateString(query, -1)),
					zap.String("argument", utils.TruncateInterface(args, -1)),
					log.ShortError(err))
				metrics.SQLRetriesTotal.WithLabelValues("query", conn.Cfg.Name).Add(1)
				return true
			}
			return false
		},
	}

	ret, _, err := conn.BaseConn.ApplyRetryStrategy(
		tctx,
		params,
		func(ctx *tcontext.Context) (interface{}, error) {
			startTime := time.Now()
			ret, err := conn.BaseConn.QuerySQL(ctx, query, args...)
			if err == nil {
				if ret.Err() != nil {
					return err, ret.Err()
				}
				cost := time.Since(startTime)
				// duration seconds
				ds := cost.Seconds()
				metrics.QueryHistogram.WithLabelValues(conn.Cfg.Name, conn.Cfg.WorkerName, conn.Cfg.SourceID).Observe(ds)
				if ds > 1 {
					ctx.L().Warn("query statement too slow",
						zap.Duration("cost time", cost),
						zap.String("query", utils.TruncateString(query, -1)),
						zap.String("argument", utils.TruncateInterface(args, -1)))
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

// ExecuteSQLWithIgnore do some SQL executions and can ignore some error by `ignoreError`.
func (conn *DBConn) ExecuteSQLWithIgnore(tctx *tcontext.Context, ignoreError func(error) bool, queries []string, args ...[]interface{}) (int, error) {
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

	if conn == nil || conn.BaseConn == nil {
		return 0, terror.ErrDBUnExpect.Generate("database base connection not valid")
	}

	// nolint:dupl
	params := retry.Params{
		RetryCount:         100,
		FirstRetryDuration: retryTimeout,
		BackoffStrategy:    retry.Stable,
		IsRetryableFn: func(retryTime int, err error) bool {
			if retry.IsConnectionError(err) {
				err = conn.ResetConn(tctx)
				if err != nil {
					tctx.L().Error("reset connection failed", zap.Int("retry", retryTime),
						zap.String("queries", utils.TruncateInterface(queries, -1)),
						zap.String("arguments", utils.TruncateInterface(args, -1)),
						log.ShortError(err))
					return false
				}
				tctx.L().Warn("execute sql failed by connection error", zap.Int("retry", retryTime),
					zap.Error(err))
				metrics.SQLRetriesTotal.WithLabelValues("stmt_exec", conn.Cfg.Name).Add(1)
				return true
			}
			if dbutil.IsRetryableError(err) {
				tctx.L().Warn("execute statements", zap.Int("retry", retryTime),
					zap.String("queries", utils.TruncateInterface(queries, -1)),
					zap.String("arguments", utils.TruncateInterface(args, -1)),
					log.ShortError(err))
				tctx.L().Warn("execute sql failed by retryable error", zap.Int("retry", retryTime),
					zap.Error(err))
				metrics.SQLRetriesTotal.WithLabelValues("stmt_exec", conn.Cfg.Name).Add(1)
				return true
			}
			return false
		},
	}

	ret, _, err := conn.BaseConn.ApplyRetryStrategy(
		tctx,
		params,
		func(ctx *tcontext.Context) (interface{}, error) {
			startTime := time.Now()
			ret, err := conn.BaseConn.ExecuteSQLWithIgnoreError(ctx, metrics.StmtHistogram, conn.Cfg.Name, ignoreError, queries, args...)
			if err == nil {
				cost := time.Since(startTime)
				// duration seconds
				ds := cost.Seconds()
				metrics.TxnHistogram.WithLabelValues(conn.Cfg.Name, conn.Cfg.WorkerName, conn.Cfg.SourceID).Observe(ds)
				// calculate idealJobCount metric: connection count * 1 / (one sql cost time)
				qps := float64(conn.Cfg.WorkerCount) / (cost.Seconds() / float64(len(queries)))
				metrics.IdealQPS.WithLabelValues(conn.Cfg.Name, conn.Cfg.WorkerName, conn.Cfg.SourceID).Set(qps)
				if ds > 1 {
					ctx.L().Warn("execute transaction too slow",
						zap.Duration("cost time", cost),
						zap.String("query", utils.TruncateInterface(queries, -1)),
						zap.String("argument", utils.TruncateInterface(args, -1)))
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

// ExecuteSQL does some SQL executions.
func (conn *DBConn) ExecuteSQL(tctx *tcontext.Context, queries []string, args ...[]interface{}) (int, error) {
	return conn.ExecuteSQLWithIgnore(tctx, nil, queries, args...)
}

// CreateConns returns a opened DB from dbCfg and number of `count` connections of that DB.
func CreateConns(tctx *tcontext.Context, cfg *config.SubTaskConfig, dbCfg *config.DBConfig, count int) (*conn.BaseDB, []*DBConn, error) {
	conns := make([]*DBConn, 0, count)
	baseDB, err := CreateBaseDB(dbCfg)
	if err != nil {
		return nil, nil, err
	}
	for i := 0; i < count; i++ {
		baseConn, err := baseDB.GetBaseConn(tctx.Context())
		if err != nil {
			CloseBaseDB(tctx, baseDB)
			return nil, nil, terror.WithScope(err, terror.ScopeDownstream)
		}
		resetBaseConnFn := func(tctx *tcontext.Context, baseConn *conn.BaseConn) (*conn.BaseConn, error) {
			err := baseDB.CloseBaseConn(baseConn)
			if err != nil {
				tctx.L().Warn("failed to close BaseConn in reset")
			}
			return baseDB.GetBaseConn(tctx.Context())
		}
		conns = append(conns, &DBConn{BaseConn: baseConn, Cfg: cfg, ResetBaseConnFn: resetBaseConnFn})
	}
	return baseDB, conns, nil
}
