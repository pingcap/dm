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

package loader

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-enterprise-tools/dm/config"
	"github.com/pingcap/tidb-enterprise-tools/pkg/tableroute"
	tmysql "github.com/pingcap/tidb/mysql"
)

// Conn represents a live DB connection
type Conn struct {
	db *sql.DB
}

func querySQL(db *sql.DB, query string, args ...interface{}) (*sql.Rows, error) {
	var (
		err  error
		rows *sql.Rows
	)

	log.Debugf("[query][sql]%s, args %v", query, args)

	rows, err = db.Query(query, args...)
	if err != nil {
		if !(isErrTableNotExists(err) || isErrDBExists(err)) {
			log.Errorf("query sql[%s] failed %v", query, errors.ErrorStack(err))
		}
		return nil, errors.Trace(err)
	}

	return rows, nil
}

func executeSQL(conn *Conn, sqls []string, enableRetry bool) error {
	if len(sqls) == 0 {
		return nil
	}

	var err error

	retryCount := 1
	if enableRetry {
		retryCount = maxRetryCount
	}

	for i := 0; i < retryCount; i++ {
		if i > 0 {
			log.Warnf("exec sql retry %d - %-.100v", i, sqls)
			time.Sleep(2 * time.Duration(i) * time.Second)
		}

		if err = executeSQLImp(conn.db, sqls); err != nil {
			if isRetryableError(err) {
				continue
			}
		}

		return nil
	}

	return errors.Trace(err)
}

func executeSQLImp(db *sql.DB, sqls []string) error {
	var (
		err error
		txn *sql.Tx
		res sql.Result
	)

	startTime := time.Now()
	defer func() {
		cost := time.Since(startTime).Seconds()
		txnHistogram.Observe(cost)
		if cost > 1 {
			log.Warnf("transaction execution costs %f seconds", cost)
		}
	}()

	txn, err = db.Begin()
	if err != nil {
		log.Errorf("exec sqls[%-.100v] begin failed %v", sqls, errors.ErrorStack(err))
		return err
	}

	for i := range sqls {
		log.Debugf("[exec][sql]%-.200v", sqls[i])
		res, err = txn.Exec(sqls[i])
		if err != nil {
			if isTiDBUnknownError(err) {
				tidbUnknownErrorCount.Inc()
			}
			log.Warnf("[exec][sql]%-.100v[error]%v", sqls, err)
			rerr := txn.Rollback()
			if rerr != nil {
				log.Errorf("[exec][sql]%-.100s[error]%v", sqls, rerr)
			}
			return err
		}
		// check update checkpoint successful or not
		if i == 2 {
			row, err1 := res.RowsAffected()
			if err1 != nil {
				log.Warnf("exec sql %s get rows affected error %s", sqls[i], err1)
				continue
			}
			if row != 1 {
				log.Warnf("update checkpoint affected rows %d", row)
			}
		}
	}

	err = txn.Commit()
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func hasUniqIndex(conn *Conn, schema string, table string, tableRouter route.TableRouter) (bool, error) {
	if schema == "" || table == "" {
		return false, errors.New("schema/table is empty")
	}

	targetSchema, targetTable := fetchMatchedLiteral(tableRouter, schema, table)

	query := fmt.Sprintf("show index from `%s`.`%s`", targetSchema, targetTable)
	rows, err := querySQL(conn.db, query)
	if err != nil {
		return false, errors.Trace(err)
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return false, errors.Trace(err)
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

	for rows.Next() {
		datas := make([]sql.RawBytes, len(rowColumns))
		values := make([]interface{}, len(rowColumns))

		for i := range values {
			values[i] = &datas[i]
		}

		err = rows.Scan(values...)
		if err != nil {
			return false, errors.Trace(err)
		}

		nonUnique := string(datas[1])
		if nonUnique == "0" {
			return true, nil
		}
	}

	if rows.Err() != nil {
		return false, errors.Trace(rows.Err())
	}

	return false, nil
}

func createConn(cfg config.DBConfig) (*Conn, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8", cfg.User, cfg.Password, cfg.Host, cfg.Port)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Conn{db: db}, nil
}

func closeConn(conn *Conn) error {
	if conn.db == nil {
		return nil
	}

	return errors.Trace(conn.db.Close())
}

func isErrDBExists(err error) bool {
	return isMySQLError(err, tmysql.ErrDBCreateExists)
}

func isErrTableExists(err error) bool {
	return isMySQLError(err, tmysql.ErrTableExists)
}

func isErrTableNotExists(err error) bool {
	return isMySQLError(err, tmysql.ErrNoSuchTable)
}

func isErrDupEntry(err error) bool {
	return isMySQLError(err, tmysql.ErrDupEntry)
}

func isRetryableError(err error) bool {
	err = causeErr(err)
	if isMySQLError(err, tmysql.ErrDupEntry) {
		return false
	}
	if isMySQLError(err, tmysql.ErrDataTooLong) {
		return false
	}
	return true
}

func isTiDBUnknownError(err error) bool {
	return isMySQLError(err, tmysql.ErrUnknown)
}

func isMySQLError(err error, code uint16) bool {
	err = causeErr(err)
	e, ok := err.(*mysql.MySQLError)
	return ok && e.Number == code
}

func createSchema(db *sql.DB, schema string) error {
	if schema == "" {
		schema = "loader"
	}
	sql := fmt.Sprintf("CREATE SCHEMA `%s`", schema)
	_, err := querySQL(db, sql)
	return errors.Trace(err)
}

func createCheckPointTable(db *sql.DB, schema, table string) error {
	tableName := fmt.Sprintf("`%s`.`%s`", schema, table)
	createTable := `CREATE TABLE %s (
		id char(6) NOT NULL,
		filename varchar(256) NOT NULL,
		cp_schema varchar(128) NOT NULL,
		cp_table varchar(128) NOT NULL,
		offset bigint NOT NULL,
		end_pos bigint NOT NULL,
		create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		UNIQUE KEY uk_id_f (id,filename)
	);
`
	sql := fmt.Sprintf(createTable, tableName)
	_, err := querySQL(db, sql)
	return errors.Trace(err)
}

func initCheckpoint(db *sql.DB, hashValue, schema, table, filename string, offset, endPos int64) error {
	idx := strings.Index(filename, ".sql")
	fname := filename[:idx]
	fields := strings.Split(fname, ".")
	if len(fields) != 2 && len(fields) != 3 {
		return errors.Errorf("invalid db table sql file - %s", filename)
	}

	// fields[0] -> db name, fields[1] -> table name
	sql := fmt.Sprintf("INSERT INTO `%s`.`%s` (`id`, `filename`, `cp_schema`, `cp_table`, `offset`, `end_pos`) VALUES(?,?,?,?,?,?)", schema, table)
	log.Debugf("sql:%s, id:%s, filename:%s, cp_schema:%s, cp_table:%s, offset:%d, end_pos:%d", sql, hashValue, filename, fields[0], fields[1], offset, endPos)
	_, err := db.Exec(sql, hashValue, filename, fields[0], fields[1], offset, endPos)
	if err != nil {
		if isErrDupEntry(err) {
			log.Infof("checkpoint id:%s filename %s already exists, skip it.", hashValue, filename)
			return nil
		}
		return errors.Annotatef(err, "initialize checkpoint")
	}

	return errors.Trace(err)
}
