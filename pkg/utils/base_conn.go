package utils

import (
	"database/sql"
	"fmt"
	"time"

	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/retry"
	"github.com/pingcap/dm/pkg/terror"

	"github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
)

// BaseConn wraps a connection to DB
type BaseConn struct {
	DB *sql.DB

	// for reset
	DSN string
}

var _ retry.Strategy = new(BaseConn)

// SQL is format sql job
type SQL struct {
	Query string
	Args  []interface{}
}

// NewBaseConn builds BaseConn to connect real DB
func NewBaseConn(dbDSN string) (*BaseConn, error) {
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	return &BaseConn{db, dbDSN}, nil
}

// ResetConn generates new *DB with new connection pool to take place old one
func (conn *BaseConn) ResetConn() error {
	db, err := sql.Open("mysql", conn.DSN)
	if err != nil {
		return terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	if conn.DB != nil {
		conn.DB.Close()
	}
	conn.DB = db
	return nil
}

// FiniteRetryStrategy define in pkg/retry/strategy.go
// ErrInvalidConn is a special error, need a public retry strategy, and need wait more time, so put it before retryFn.
func (conn *BaseConn) FiniteRetryStrategy(ctx *tcontext.Context,
	retryCount int,
	firstRetryDuration time.Duration,
	retrySpeed retry.Speed,
	operateFn func(*tcontext.Context, int) (interface{}, error),
	retryFn func(int, error) bool) (interface{}, error) {
	var err error
	var ret interface{}
	for i := 0; i < retryCount; i++ {
		ret, err = operateFn(ctx, i)
		if err != nil {
			if terr, ok := err.(*terror.Error); ok {
				switch terr.Cause() {
				case mysql.ErrInvalidConn:
					ctx.L().Warn(fmt.Sprintf("Met invalid connection error, next retry %d, will retry in %d seconds", i, i*5))
					time.Sleep(time.Duration(i) * 5 * time.Second)
					continue
				default:
				}
			}
			if retryFn(i, err) {
				duration := firstRetryDuration
				if i > 0 {
					switch retrySpeed {
					case retry.SpeedSlow:
						duration = time.Duration(i) * firstRetryDuration
					default:
					}
				}
				time.Sleep(duration)
				continue
			}
		}
		break
	}
	return ret, err
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
		return nil, terror.DBErrorAdapt(err, terror.ErrDBQueryFailed, query)
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
		return 0, terror.DBErrorAdapt(err, terror.ErrDBExecuteFailed, "begin")
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
			return i, terror.DBErrorAdapt(err, terror.ErrDBExecuteFailed, sqls[i].Query)
		}
	}
	err = txn.Commit()
	if err != nil {
		return l, terror.DBErrorAdapt(err, terror.ErrDBExecuteFailed)
	}
	return l, nil
}

// Close release DB resource
func (conn *BaseConn) Close() error {
	if conn == nil || conn.DB == nil {
		return nil
	}

	return terror.DBErrorAdapt(conn.DB.Close(), terror.ErrDBUnExpect, "close")
}
