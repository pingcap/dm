// Copyright 2016 PingCAP, Inc.
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

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-enterprise-tools/dm/config"
	"github.com/siddontang/go-mysql/mysql"
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
}

type table struct {
	schema string
	name   string

	columns      []*column
	indexColumns map[string][]*column
}

// Conn represents a live DB connection
type Conn struct {
	cfg *config.SubTaskConfig

	db *sql.DB
}

func (conn *Conn) querySQL(query string, maxRetry int) (*sql.Rows, error) {
	if conn == nil || conn.db == nil {
		return nil, errors.NotValidf("database connection")
	}

	var (
		err  error
		rows *sql.Rows
	)

	for i := 0; i < maxRetry; i++ {
		if i > 0 {
			sqlRetriesTotal.WithLabelValues("query", conn.cfg.Name).Add(1)
			log.Warnf("sql query retry %d: %s", i, query)
			time.Sleep(retryTimeout)
		}

		log.Debugf("[query][sql]%s", query)

		rows, err = conn.db.Query(query)
		if err != nil {
			if !isRetryableError(err) {
				return rows, errors.Trace(err)
			}
			log.Warnf("[query][sql]%s[error]%v", query, err)
			continue
		}

		return rows, nil
	}

	if err != nil {
		log.Errorf("query sql[%s] failed %v", query, errors.ErrorStack(err))
		return nil, errors.Trace(err)
	}

	return nil, errors.Errorf("query sql[%s] failed", query)
}

// Note: keep it for later use?
func (conn *Conn) executeSQL(sqls []string, args [][]interface{}, maxRetry int) error {
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
			log.Warnf("sql stmt_exec retry %d: %v - %v", i, sqls, args)
			time.Sleep(retryTimeout)
		}

		if err = conn.executeSQLImp(sqls, args); err != nil {
			if isRetryableError(err) {
				continue
			}
			log.Errorf("[exec][sql]%s[args]%v[error]%v", sqls, args, err)
			return errors.Trace(err)
		}

		return nil
	}

	return errors.Errorf("exec sqls[%v] failed, err:%s", sqls, err.Error())
}

// Note: keep it for later use?
func (conn *Conn) executeSQLImp(sqls []string, args [][]interface{}) error {
	if conn == nil || conn.db == nil {
		return errors.NotValidf("database connection")
	}

	startTime := time.Now()
	defer func() {
		txnHistogram.WithLabelValues(conn.cfg.Name).Observe(time.Since(startTime).Seconds())
	}()

	txn, err := conn.db.Begin()
	if err != nil {
		log.Errorf("exec sqls[%v] begin failed %v", sqls, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	for i := range sqls {
		log.Debugf("[exec][sql]%s[args]%v", sqls[i], args[i])

		_, err = txn.Exec(sqls[i], args[i]...)
		if err != nil {
			log.Warnf("[exec][sql]%s[args]%v[error]%v", sqls[i], args[i], err)
			rerr := txn.Rollback()
			if rerr != nil {
				log.Errorf("[exec][sql]%s[args]%v[error]%v", sqls[i], args[i], rerr)
			}
			// we should return the exec err, instead of the rollback rerr.
			return errors.Trace(err)
		}
	}
	err = txn.Commit()
	if err != nil {
		log.Errorf("exec sqls[%v] commit failed %v", sqls, errors.ErrorStack(err))
		return errors.Trace(err)
	}
	return nil
}

func (conn *Conn) executeSQLJob(jobs []*job, maxRetry int) error {
	if len(jobs) == 0 {
		return nil
	}

	var err error

	for i := 0; i < maxRetry; i++ {
		if i > 0 {
			sqlRetriesTotal.WithLabelValues("stmt_exec", conn.cfg.Name).Add(1)
			log.Warnf("sql stmt_exec retry %d: %v", i, jobs)
			time.Sleep(retryTimeout)
		}

		if err = conn.executeSQLJobImp(jobs); err != nil {
			if isRetryableError(err) {
				continue
			}
			log.Errorf("[exec][sql]%v[error]%v", jobs, err)
			return errors.Trace(err)
		}

		return nil
	}

	return errors.Errorf("exec jobs[%v] failed, err:%s", jobs, err.Error())

}

func (conn *Conn) executeSQLJobImp(jobs []*job) error {
	startTime := time.Now()
	defer func() {
		cost := time.Since(startTime).Seconds()
		txnHistogram.WithLabelValues(conn.cfg.Name).Observe(cost)
	}()

	txn, err := conn.db.Begin()
	if err != nil {
		log.Errorf("exec sqls[%v] begin failed %v", jobs, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	for i := range jobs {
		log.Debugf("[exec][checkpoint]%s[sql]%s[args]%v", jobs[i].pos, jobs[i].sql, jobs[i].args)

		_, err = txn.Exec(jobs[i].sql, jobs[i].args...)
		if err != nil {
			log.Warnf("[exec][checkpoint]%s[sql]%s[args]%v[error]%v", jobs[i].pos, jobs[i].sql, jobs[i].args, err)
			rerr := txn.Rollback()
			if rerr != nil {
				log.Errorf("[exec][checkpoint]%s[sql]%s[args]%v[error]%v", jobs[i].pos, jobs[i].sql, jobs[i].args, rerr)
			}
			// we should return the exec err, instead of the rollback rerr.
			return errors.Trace(err)
		}
	}
	err = txn.Commit()
	if err != nil {
		log.Errorf("exec jobs[%v] commit failed %v", jobs, errors.ErrorStack(err))
		return errors.Trace(err)
	}
	return nil
}

func createDB(cfg *config.SubTaskConfig, dbCfg config.DBConfig, timeout string) (*Conn, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8&interpolateParams=true&readTimeout=%s", dbCfg.User, dbCfg.Password, dbCfg.Host, dbCfg.Port, timeout)
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

func closeDBs(dbs ...*Conn) {
	for _, db := range dbs {
		err := db.close()
		if err != nil {
			log.Errorf("close db failed: %v", err)
		}
	}
}

func getTableIndex(db *Conn, table *table, maxRetry int) error {
	if table.schema == "" || table.name == "" {
		return errors.New("schema/table is empty")
	}

	query := fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", table.schema, table.name)
	rows, err := db.querySQL(query, maxRetry)
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

func getTableColumns(db *Conn, table *table, maxRetry int) error {
	if table.schema == "" || table.name == "" {
		return errors.New("schema/table is empty")
	}

	query := fmt.Sprintf("SHOW COLUMNS FROM `%s`.`%s`", table.schema, table.name)
	rows, err := db.querySQL(query, maxRetry)
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
	   +-------+---------+------+-----+---------+-------+
	   | Field | Type    | Null | Key | Default | Extra |
	   +-------+---------+------+-----+---------+-------+
	   | a     | int(11) | NO   | PRI | NULL    |       |
	   | b     | int(11) | NO   | PRI | NULL    |       |
	   | c     | int(11) | YES  | MUL | NULL    |       |
	   | d     | int(11) | YES  |     | NULL    |       |
	   +-------+---------+------+-----+---------+-------+
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
		if file.Name < fromFile.Name {
			continue
		} else if file.Name > fromFile.Name {
			total += int64(file.Pos)
		} else if file.Name == fromFile.Name {
			if file.Pos > fromFile.Pos {
				total += int64(file.Pos - fromFile.Pos)
			}
		}
	}

	return total, nil
}

func getBinaryLogs(db *sql.DB) ([]mysql.Position, error) {
	query := "SHOW BINARY LOGS"
	rows, err := db.Query(query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	files := make([]mysql.Position, 0, 10)
	for rows.Next() {
		var file string
		var pos uint32
		err = rows.Scan(&file, &pos)
		if err != nil {
			return nil, errors.Trace(err)
		}
		files = append(files, mysql.Position{Name: file, Pos: pos})
	}
	if rows.Err() != nil {
		return nil, errors.Trace(rows.Err())
	}
	return files, nil
}
