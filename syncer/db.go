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
	"github.com/pingcap/dm/pkg/gtid"
	"strings"

	"github.com/pingcap/dm/dm/config"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"

	"github.com/siddontang/go-mysql/mysql"
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

// Conn represents a live DB connection
type Conn struct {
	cfg      *config.SubTaskConfig
	baseConn *utils.BaseConn
}

// ResetConn reset baseConn.*DB's connection pool
func (conn *Conn) ResetConn() error {
	if conn.baseConn == nil {
		return terror.ErrDBUnExpect.Generate("database base connection not valid")
	}
	return conn.baseConn.ResetConn()
}

func (conn *Conn) getMasterStatus(flavor string) (mysql.Position, gtid.Set, error) {
	return utils.GetMasterStatus(conn.baseConn.DB, flavor)
}

func (conn *Conn) querySQL(tctx *tcontext.Context, query string) (*sql.Rows, error) {
	if conn == nil || conn.baseConn == nil {
		return nil, terror.ErrDBUnExpect.Generate("database base connection not valid")
	}

	ret, err := conn.baseConn.FiniteRetryStrategy(
		tctx,
		10,
		retryTimeout,
		utils.RetrySpeedStable,
		func(ctx *tcontext.Context, _ int) (interface{}, error) {
			rows, err := conn.baseConn.QuerySQL(ctx, query)
			return rows, err
		},
		func(retryTime int, err error) bool {
			if isRetryableError(err) {
				sqlRetriesTotal.WithLabelValues("query", conn.cfg.Name).Add(1)
				return true
			}
			return false
		})

	if err != nil {
		return nil, terror.DBErrorAdapt(err, terror.ErrDBQueryFailed, query)
	}
	return ret.(*sql.Rows), nil
}

func (conn *Conn) executeSQL(tctx *tcontext.Context, queries []string, args [][]interface{}) (int, error) {
	if len(queries) == 0 {
		return 0, nil
	}

	if conn == nil || conn.baseConn == nil {
		return 0, terror.ErrDBUnExpect.Generate("database base connection not valid")
	}

	sqls := make([]utils.SQL, 0, len(queries))
	for i, query := range queries {
		sqls = append(sqls, utils.SQL{query, args[i]})
	}
	ret, err := conn.baseConn.FiniteRetryStrategy(
		tctx,
		100,
		retryTimeout,
		utils.RetrySpeedStable,
		func(ctx *tcontext.Context, _ int) (interface{}, error) {
			affected, err := conn.baseConn.ExecuteSQL(ctx, sqls)
			return affected, err
		},
		func(retryTime int, err error) bool {
			if isRetryableError(err) {
				sqlRetriesTotal.WithLabelValues("stmt_exec", conn.cfg.Name).Add(1)
				return true
			}
			return false
		})
	if err != nil {
		return ret.(int), terror.DBErrorAdapt(err, terror.ErrDBExecuteFailed, queries)
	}
	return ret.(int), nil
}

func createBaseConn(dbCfg config.DBConfig, timeout string) (*utils.BaseConn, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&interpolateParams=true&readTimeout=%s&maxAllowedPacket=%d",
		dbCfg.User, dbCfg.Password, dbCfg.Host, dbCfg.Port, timeout, *dbCfg.MaxAllowedPacket)
	baseConn, err := utils.NewBaseConn(dbDSN)
	if err != nil {
		return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	return baseConn, nil
}

func createConn(cfg *config.SubTaskConfig, dbCfg config.DBConfig, timeout string) (*Conn, error) {
	baseConn, err := createBaseConn(dbCfg, timeout)
	if err != nil {
		return nil, err
	}
	return &Conn{baseConn: baseConn, cfg: cfg}, nil
}

func createConns(cfg *config.SubTaskConfig, dbCfg config.DBConfig, count int, timeout string) ([]*Conn, error) {
	dbs := make([]*Conn, 0, count)

	var db *sql.DB
	for i := 0; i < count; i++ {
		baseConn, err := createBaseConn(dbCfg, timeout)
		if db == nil {
			db = baseConn.DB
		}
		// TODO use *sql.Conn instead of *sql.DB
		// share db by all conns
		baseConn.DB = db
		if err != nil {
			return nil, err
		}
		dbs = append(dbs, &Conn{baseConn: baseConn, cfg: cfg})
	}
	return dbs, nil
}

func closeConns(tctx *tcontext.Context, conns ...*Conn) {
	for _, conn := range conns {
		err := conn.baseConn.Close()
		if err != nil {
			tctx.L().Error("fail to close baseConn connection", log.ShortError(err))
		}
	}
}

func getTableIndex(tctx *tcontext.Context, conn *Conn, table *table) error {
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

func getTableColumns(tctx *tcontext.Context, conn *Conn, table *table) error {
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
