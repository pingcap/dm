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

	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/mysql"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/config"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/log"
)

var (
	// ErrNotRowFormat defines an error which means binlog format is not ROW format.
	ErrNotRowFormat = errors.New("binlog format is not ROW format")
)

type column struct {
	idx      int
	name     string
	NotNull  bool
	unsigned bool
	tp       string
	extra    string
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

// Conn represents a live DB connection
type Conn struct {
	cfg *config.SubTaskConfig

	db *sql.DB
}

func (conn *Conn) querySQL(tctx *tcontext.Context, query string, maxRetry int) (*sql.Rows, error) {
	if conn == nil || conn.db == nil {
		return nil, errors.NotValidf("database connection")
	}

	var (
		err  error
		rows *sql.Rows
	)
	tctx.L().Debug("query statement", zap.String("sql", query))
	for i := 0; i < maxRetry; i++ {
		if i > 0 {
			sqlRetriesTotal.WithLabelValues("query", conn.cfg.Name).Add(1)
			tctx.L().Warn("query statement", zap.Int("retry", i), zap.String("sql", query))
			time.Sleep(retryTimeout)
		}

		rows, err = conn.db.QueryContext(tctx.Context(), query)
		if err != nil {
			if !isRetryableError(err) {
				return rows, errors.Trace(err)
			}
			tctx.L().Warn("query statement failed and retry", zap.String("sql", query), log.ShortError(err))
			continue
		}

		return rows, nil
	}

	if err != nil {
		tctx.L().Error("query statement failed", zap.String("sql", query), log.ShortError(err))
		return nil, errors.Trace(err)
	}

	return nil, errors.Errorf("query sql[%s] failed", query)
}

// Note: keep it for later use?
func (conn *Conn) executeSQL(tctx *tcontext.Context, sqls []string, args [][]interface{}, maxRetry int) error {
	if len(sqls) == 0 {
		return nil
	}

	if conn == nil || conn.db == nil {
		return errors.NotValidf("database connection")
	}

	var err error
	for i := 0; i < maxRetry; i++ {
		if i > 0 {
			sqlRetriesTotal.WithLabelValues("stmt_exec", conn.cfg.Name).Add(1)
			tctx.L().Warn("execute statements", zap.Int("retry", i), zap.Strings("sqls", sqls), zap.Reflect("arguments", args))
			time.Sleep(retryTimeout)
		}

		if err = conn.executeSQLImp(tctx, sqls, args); err != nil {
			if isRetryableError(err) {
				continue
			}
			tctx.L().Error("execute statements", zap.Strings("sqls", sqls), zap.Reflect("arguments", args), log.ShortError(err))
			return errors.Trace(err)
		}

		return nil
	}

	return errors.Errorf("exec sqls[%v] failed, err:%s", sqls, err.Error())
}

// Note: keep it for later use?
func (conn *Conn) executeSQLImp(tctx *tcontext.Context, sqls []string, args [][]interface{}) error {
	if conn == nil || conn.db == nil {
		return errors.NotValidf("database connection")
	}

	startTime := time.Now()
	defer func() {
		txnHistogram.WithLabelValues(conn.cfg.Name).Observe(time.Since(startTime).Seconds())
	}()

	txn, err := conn.db.Begin()
	if err != nil {
		tctx.L().Error("begin transaction", zap.Strings("sqls", sqls), zap.Reflect("arguments", args), zap.Error(err))
		return errors.Trace(err)
	}

	for i := range sqls {
		tctx.L().Debug("execute statement", zap.String("sql", sqls[i]), zap.Reflect("argument", args[i]))

		_, err = txn.ExecContext(tctx.Context(), sqls[i], args[i]...)
		if err != nil {
			tctx.L().Error("execute statement failed", zap.String("sql", sqls[i]), zap.Reflect("argument", args[i]), log.ShortError(err))
			rerr := txn.Rollback()
			if rerr != nil {
				tctx.L().Error("rollback failed", zap.String("sql", sqls[i]), zap.Reflect("argument", args[i]), log.ShortError(rerr))
			}
			// we should return the exec err, instead of the rollback rerr.
			return errors.Trace(err)
		}
	}
	err = txn.Commit()
	if err != nil {
		tctx.L().Error("transaction commit failed", zap.Strings("sqls", sqls), zap.Reflect("arguments", args), zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

func (conn *Conn) executeSQLJob(tctx *tcontext.Context, jobs []*job, maxRetry int) *ExecErrorContext {
	if len(jobs) == 0 {
		return nil
	}

	var errCtx *ExecErrorContext

	for i := 0; i < maxRetry; i++ {
		if i > 0 {
			sqlRetriesTotal.WithLabelValues("stmt_exec", conn.cfg.Name).Add(1)
			tctx.L().Warn("execute jobs", zap.Int("retry", i))
			time.Sleep(retryTimeout)
		}

		if errCtx = conn.executeSQLJobImp(tctx, jobs); errCtx != nil {
			err := errCtx.err
			if isRetryableError(err) {
				continue
			}
			tctx.L().Error("execute jobs", log.ShortError(err))
			errCtx.err = errors.Trace(errCtx.err)
			return errCtx
		}

		return nil
	}

	errCtx.err = errors.Errorf("exec jobs failed, err:%s", errCtx.err.Error())
	return errCtx
}

func (conn *Conn) executeSQLJobImp(tctx *tcontext.Context, jobs []*job) *ExecErrorContext {
	startTime := time.Now()
	defer func() {
		cost := time.Since(startTime).Seconds()
		txnHistogram.WithLabelValues(conn.cfg.Name).Observe(cost)
	}()

	txn, err := conn.db.Begin()
	if err != nil {
		tctx.L().Error("begin transaction in executing job", zap.Error(err))
		return &ExecErrorContext{err: errors.Trace(err), jobs: fmt.Sprintf("%v", jobs)}
	}

	for i := range jobs {
		tctx.L().Debug("execute job", log.WrapStringerField("job", jobs[i]))

		_, err = txn.ExecContext(tctx.Context(), jobs[i].sql, jobs[i].args...)
		if err != nil {
			tctx.L().Error("execute job", log.WrapStringerField("job", jobs[i]), log.ShortError(err))
			rerr := txn.Rollback()
			if rerr != nil {
				tctx.L().Error("rollback job", log.WrapStringerField("job", jobs[i]), log.ShortError(rerr))
			}
			// error in ExecErrorContext should be the exec err, instead of the rollback rerr.
			return &ExecErrorContext{err: errors.Trace(err), pos: jobs[i].currentPos, jobs: fmt.Sprintf("%v", jobs)}
		}
	}
	err = txn.Commit()
	if err != nil {
		tctx.L().Error("commit in executing job", zap.Error(err))
		return &ExecErrorContext{err: errors.Trace(err), pos: jobs[0].currentPos, jobs: fmt.Sprintf("%v", jobs)}
	}
	return nil
}

func createDB(cfg *config.SubTaskConfig, dbCfg config.DBConfig, timeout string) (*Conn, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&interpolateParams=true&readTimeout=%s&maxAllowedPacket=%d",
		dbCfg.User, dbCfg.Password, dbCfg.Host, dbCfg.Port, timeout, *dbCfg.MaxAllowedPacket)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Conn{db: db, cfg: cfg}, nil
}

func (conn *Conn) close() error {
	if conn == nil || conn.db == nil {
		return nil
	}

	return errors.Trace(conn.db.Close())
}

func createDBs(cfg *config.SubTaskConfig, dbCfg config.DBConfig, count int, timeout string) ([]*Conn, error) {
	dbs := make([]*Conn, 0, count)
	for i := 0; i < count; i++ {
		db, err := createDB(cfg, dbCfg, timeout)
		if err != nil {
			return nil, errors.Trace(err)
		}

		dbs = append(dbs, db)
	}

	return dbs, nil
}

func closeDBs(tctx *tcontext.Context, dbs ...*Conn) {
	for _, db := range dbs {
		err := db.close()
		if err != nil {
			tctx.L().Error("fail to close db connection", log.ShortError(err))
		}
	}
}

func getTableIndex(tctx *tcontext.Context, db *Conn, table *table, maxRetry int) error {
	if table.schema == "" || table.name == "" {
		return errors.New("schema/table is empty")
	}

	query := fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", table.schema, table.name)
	rows, err := db.querySQL(tctx, query, maxRetry)
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return errors.Trace(err)
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
			return errors.Trace(err)
		}

		nonUnique := string(data[1])
		if nonUnique == "0" {
			keyName := strings.ToLower(string(data[2]))
			columns[keyName] = append(columns[keyName], string(data[4]))
		}
	}
	if rows.Err() != nil {
		return errors.Trace(rows.Err())
	}

	table.indexColumns = findColumns(table.columns, columns)
	return nil
}

func getTableColumns(tctx *tcontext.Context, db *Conn, table *table, maxRetry int) error {
	if table.schema == "" || table.name == "" {
		return errors.New("schema/table is empty")
	}

	query := fmt.Sprintf("SHOW COLUMNS FROM `%s`.`%s`", table.schema, table.name)
	rows, err := db.querySQL(tctx, query, maxRetry)
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return errors.Trace(err)
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
			return errors.Trace(err)
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
		return errors.Trace(rows.Err())
	}

	return nil
}

func countBinaryLogsSize(fromFile mysql.Position, db *sql.DB) (int64, error) {
	files, err := getBinaryLogs(db)
	if err != nil {
		return 0, errors.Trace(err)
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
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return nil, errors.Trace(err)
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
			return nil, errors.Trace(err)
		}
		files = append(files, binlogSize{name: file, size: pos})
	}
	if rows.Err() != nil {
		return nil, errors.Trace(rows.Err())
	}
	return files, nil
}

func (c *column) isGeneratedColumn() bool {
	return strings.Contains(c.extra, "VIRTUAL GENERATED") || strings.Contains(c.extra, "STORED GENERATED")
}
