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
	"database/sql"
	"fmt"
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

	"github.com/pingcap/parser"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/siddontang/go-mysql/mysql"
	"go.uber.org/zap"
)

type column struct {
	idx      int
	name     string
	NotNull  bool
	unsigned bool
	tp       string
	extra    string
}

func (c *column) isGeneratedColumn() bool {
	return strings.Contains(c.extra, "VIRTUAL GENERATED") || strings.Contains(c.extra, "STORED GENERATED")
}

type table struct {
	schema string
	name   string

	columns      []*column
	indexColumns map[string][]*column
}

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
		return nil, err
	}
	return db, nil
}

// UpStreamConn connect to upstream DB, use *sql.DB instead of *sql.Conn
type UpStreamConn struct {
	cfg    *config.SubTaskConfig
	BaseDB *conn.BaseDB
}

func (conn *UpStreamConn) getMasterStatus(flavor string) (mysql.Position, gtid.Set, error) {
	return utils.GetMasterStatus(conn.BaseDB.DB, flavor)
}

func (conn *UpStreamConn) getServerUUID(flavor string) (string, error) {
	return utils.GetServerUUID(conn.BaseDB.DB, flavor)
}

func (conn *UpStreamConn) getParser(ansiQuotesMode bool) (*parser.Parser, error) {
	return utils.GetParser(conn.BaseDB.DB, ansiQuotesMode)
}

func (conn *UpStreamConn) killConn(connID uint32) error {
	return utils.KillConn(conn.BaseDB.DB, connID)
}

func (conn *UpStreamConn) fetchAllDoTables(bw *filter.Filter) (map[string][]string, error) {
	return utils.FetchAllDoTables(conn.BaseDB.DB, bw)
}

func (conn *UpStreamConn) countBinaryLogsSize(pos mysql.Position) (int64, error) {
	return countBinaryLogsSize(pos, conn.BaseDB.DB)
}

func createUpStreamConn(cfg *config.SubTaskConfig, dbCfg config.DBConfig) (*UpStreamConn, error) {
	baseDB, err := createBaseDB(dbCfg)
	if err != nil {
		return nil, terror.WithScope(terror.DBErrorAdapt(err, terror.ErrDBDriverError), terror.ScopeUpstream)
	}
	return &UpStreamConn{BaseDB: baseDB, cfg: cfg}, nil
}

func closeUpstreamConn(tctx *tcontext.Context, conn *UpStreamConn) {
	err := conn.BaseDB.Close()
	if err != nil {
		tctx.L().Error("fail to close baseConn connection", log.ShortError(err))
	}
}

// WorkerConn represents a live DB connection
type WorkerConn struct {
	cfg      *config.SubTaskConfig
	baseConn *conn.BaseConn

	// for workerConn to reset itself
	baseDB *conn.BaseDB
}

// ResetConn reset one worker connection from specify *BaseDB
func (conn *WorkerConn) ResetConn(tctx *tcontext.Context) error {
	if conn == nil || conn.baseDB == nil {
		return terror.ErrDBDriverError.Generate("database not valid")
	}
	dbConn, err := conn.baseDB.GetBaseConn(tctx.Context())
	if err != nil {
		return err
	}
	if conn.baseConn != nil {
		err = conn.baseConn.Close()
		if err != nil {
			return err
		}
	}
	conn.baseConn = dbConn
	return nil
}

func (conn *WorkerConn) querySQL(tctx *tcontext.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if conn == nil || conn.baseConn == nil {
		return nil, terror.ErrDBUnExpect.Generate("database base connection not valid")
	}
	params := retry.Params{
		RetryCount:         10,
		FirstRetryDuration: retryTimeout,
		BackoffStrategy:    retry.Stable,
		IsRetryableFn: func(retryTime int, err error) bool {
			if retry.IsRetryableError(err) {
				tctx.L().Warn("query statement", zap.Int("retry", retryTime),
					zap.String("query", utils.TruncateString(query, -1)),
					zap.String("argument", utils.TruncateInterface(args, -1)))
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
			return conn.baseConn.QuerySQL(ctx, query, args...)
		},
	)

	if err != nil {
		tctx.L().Error("query statement failed after retry",
			zap.String("query", utils.TruncateString(query, -1)),
			zap.String("argument", utils.TruncateInterface(args, -1)),
			log.ShortError(err))
		return nil, err
	}
	return ret.(*sql.Rows), nil
}

func (conn *WorkerConn) executeSQLWithIgnore(tctx *tcontext.Context, ignoreError func(error) bool, queries []string, args ...[]interface{}) (int, error) {
	if len(queries) == 0 {
		return 0, nil
	}

	if conn == nil || conn.baseConn == nil {
		return 0, terror.ErrDBUnExpect.Generate("database base connection not valid")
	}

	params := retry.Params{
		RetryCount:         100,
		FirstRetryDuration: retryTimeout,
		BackoffStrategy:    retry.Stable,
		IsRetryableFn: func(retryTime int, err error) bool {
			if retry.IsConnectionError(err) {
				err := conn.ResetConn(tctx)
				if err != nil {
					tctx.L().Warn("reset connection failed", zap.Int("retry", retryTime),
						zap.String("queries", utils.TruncateInterface(queries, -1)),
						zap.String("arguments", utils.TruncateInterface(args, -1)))
					return false
				}
				return true
			}
			if retry.IsRetryableError(err) {
				tctx.L().Warn("execute statements", zap.Int("retry", retryTime),
					zap.String("queries", utils.TruncateInterface(queries, -1)),
					zap.String("arguments", utils.TruncateInterface(args, -1)))
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
			ret, err := conn.baseConn.ExecuteSQLWithIgnoreError(ctx, ignoreError, queries, args...)

			if err == nil {
				cost := time.Since(startTime)
				txnHistogram.WithLabelValues(conn.cfg.Name).Observe(cost.Seconds())
				if cost.Seconds() > 1 {
					ctx.L().Warn("transaction execute successfully", zap.Duration("cost time", cost))
				}
			}
			return ret, err
		})

	if err != nil {
		tctx.L().Error("execute statements failed after retry",
			zap.String("queries", utils.TruncateInterface(queries, -1)),
			zap.String("arguments", utils.TruncateInterface(args, -1)),
			log.ShortError(err))
		return ret.(int), err
	}
	return ret.(int), nil
}

func (conn *WorkerConn) executeSQL(tctx *tcontext.Context, queries []string, args ...[]interface{}) (int, error) {
	return conn.executeSQLWithIgnore(tctx, nil, queries, args...)
}

func createConn(tctx *tcontext.Context, cfg *config.SubTaskConfig, dbCfg config.DBConfig) (*WorkerConn, error) {
	baseDB, err := createBaseDB(dbCfg)
	if err != nil {
		return nil, err
	}
	baseConn, err := baseDB.GetBaseConn(tctx.Context())
	if err != nil {
		return nil, terror.WithScope(terror.DBErrorAdapt(err, terror.ErrDBDriverError), terror.ScopeDownstream)
	}
	return &WorkerConn{baseDB: baseDB, baseConn: baseConn, cfg: cfg}, nil
}

func createConns(tctx *tcontext.Context, cfg *config.SubTaskConfig, dbCfg config.DBConfig, count int) ([]*WorkerConn, error) {
	conns := make([]*WorkerConn, 0, count)
	db, err := createBaseDB(dbCfg)
	if err != nil {
		return nil, err
	}
	for i := 0; i < count; i++ {
		dbConn, err := db.GetBaseConn(tctx.Context())
		if err != nil {
			closeConns(tctx, conns...)
			return nil, terror.WithScope(terror.ErrDBBadConn.Delegate(err), terror.ScopeDownstream)
		}
		conns = append(conns, &WorkerConn{baseDB: db, baseConn: dbConn, cfg: cfg})
	}
	return conns, nil
}

func closeConns(tctx *tcontext.Context, conns ...*WorkerConn) {
	var db *conn.BaseDB
	for _, conn := range conns {
		if db == nil {
			db = conn.baseDB
		}
		err := conn.baseConn.Close()
		if err != nil {
			tctx.L().Error("fail to close baseConn connection", log.ShortError(err))
		}
	}
	if db != nil {
		err := db.Close()
		if err != nil {
			tctx.L().Error("fail to close baseDB", log.ShortError(err))
		}
	}
}

func getTableIndex(tctx *tcontext.Context, conn *WorkerConn, table *table) error {
	if table.schema == "" || table.name == "" {
		return terror.ErrDBUnExpect.Generate("schema/table is empty")
	}

	query := fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", table.schema, table.name)
	rows, err := conn.querySQL(tctx, query)
	if err != nil {
		return terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}

	// Show an example.
	/*
		mysql> show index from test.t;
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
		| Table | Non_unique | Key_name | Seq_in_index | Column_name | Collation | Cardinality | Sub_part | Packed | Null | Index_type | Comment | Index_comment |
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
		| t     |          0 | PRIMARY  |            1 | a           | A         |           0 |     NULL | NULL   |      | BTREE      |         |               |
		| t     |          0 | PRIMARY  |            2 | b           | A         |           0 |     NULL | NULL   |      | BTREE      |         |               |
		| t     |          0 | ucd      |            1 | c           | A         |           0 |     NULL | NULL   | YES  | BTREE      |         |               |
		| t     |          0 | ucd      |            2 | d           | A         |           0 |     NULL | NULL   | YES  | BTREE      |         |               |
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
	*/
	var columns = make(map[string][]string)
	for rows.Next() {
		data := make([]sql.RawBytes, len(rowColumns))
		values := make([]interface{}, len(rowColumns))

		for i := range values {
			values[i] = &data[i]
		}

		err = rows.Scan(values...)
		if err != nil {
			return terror.DBErrorAdapt(err, terror.ErrDBDriverError)
		}

		nonUnique := string(data[1])
		if nonUnique == "0" {
			keyName := strings.ToLower(string(data[2]))
			columns[keyName] = append(columns[keyName], string(data[4]))
		}
	}
	if rows.Err() != nil {
		return terror.DBErrorAdapt(rows.Err(), terror.ErrDBDriverError)
	}

	table.indexColumns = findColumns(table.columns, columns)
	return nil
}

func getTableColumns(tctx *tcontext.Context, conn *WorkerConn, table *table) error {
	if table.schema == "" || table.name == "" {
		return terror.ErrDBUnExpect.Generate("schema/table is empty")
	}

	query := fmt.Sprintf("SHOW COLUMNS FROM `%s`.`%s`", table.schema, table.name)
	rows, err := conn.querySQL(tctx, query)
	if err != nil {
		return terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}

	// Show an example.
	/*
	   mysql> show columns from test.t;
	   +-------+---------+------+-----+---------+-------------------+
	   | Field | Type    | Null | Key | Default | Extra             |
	   +-------+---------+------+-----+---------+-------------------+
	   | a     | int(11) | NO   | PRI | NULL    |                   |
	   | b     | int(11) | NO   | PRI | NULL    |                   |
	   | c     | int(11) | YES  | MUL | NULL    |                   |
	   | d     | int(11) | YES  |     | NULL    |                   |
	   | d     | json    | YES  |     | NULL    | VIRTUAL GENERATED |
	   +-------+---------+------+-----+---------+-------------------+
	*/

	idx := 0
	for rows.Next() {
		data := make([]sql.RawBytes, len(rowColumns))
		values := make([]interface{}, len(rowColumns))

		for i := range values {
			values[i] = &data[i]
		}

		err = rows.Scan(values...)
		if err != nil {
			return terror.DBErrorAdapt(err, terror.ErrDBDriverError)
		}

		column := &column{}
		column.idx = idx
		column.name = string(data[0])
		column.tp = string(data[1])
		column.extra = string(data[5])

		if strings.ToLower(string(data[2])) == "no" {
			column.NotNull = true
		}

		// Check whether column has unsigned flag.
		if strings.Contains(strings.ToLower(string(data[1])), "unsigned") {
			column.unsigned = true
		}

		table.columns = append(table.columns, column)
		idx++
	}

	if rows.Err() != nil {
		return terror.DBErrorAdapt(rows.Err(), terror.ErrDBDriverError)
	}

	return nil
}

func countBinaryLogsSize(fromFile mysql.Position, db *sql.DB) (int64, error) {
	files, err := getBinaryLogs(db)
	if err != nil {
		return 0, err
	}

	var total int64
	for _, file := range files {
		if file.name < fromFile.Name {
			continue
		} else if file.name > fromFile.Name {
			total += file.size
		} else if file.name == fromFile.Name {
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
