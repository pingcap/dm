// Copyright 2018 PingCAP, Inc.
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

package dbutil

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/model"
)

const (
	// ImplicitColName is name of implicit column in TiDB
	ImplicitColName = "_tidb_rowid"

	// ImplicitColID is ID implicit column in TiDB
	ImplicitColID = -1
)

var (
	// ErrVersionNotFound means can't get the database's version
	ErrVersionNotFound = errors.New("can't get the database's version")
)

// DBConfig is database configuration.
type DBConfig struct {
	Host string `toml:"host" json:"host"`

	Port int `toml:"port" json:"port"`

	User string `toml:"user" json:"user"`

	Password string `toml:"password" json:"password"`

	Schema string `toml:"schema" json:"schema"`
}

// String returns native format of database configuration
func (c *DBConfig) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("DBConfig(%+v)", *c)
}

// OpenDB opens a mysql connection FD
func OpenDB(cfg DBConfig) (*sql.DB, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4", cfg.User, cfg.Password, cfg.Host, cfg.Port)
	dbConn, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = dbConn.Ping()
	return dbConn, errors.Trace(err)
}

// CloseDB closes the mysql fd
func CloseDB(db *sql.DB) error {
	if db == nil {
		return nil
	}

	return errors.Trace(db.Close())
}

// GetCreateTableSQL returns the create table statement.
func GetCreateTableSQL(ctx context.Context, db *sql.DB, schemaName string, tableName string) (string, error) {
	/*
		show create table example result:
		mysql> SHOW CREATE TABLE `test`.`itest`;
		+-------+--------------------------------------------------------------------+
		| Table | Create Table                                                                                                                              |
		+-------+--------------------------------------------------------------------+
		| itest | CREATE TABLE `itest` (
			`id` int(11) DEFAULT NULL,
		  	`name` varchar(24) DEFAULT NULL
			) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin |
		+-------+--------------------------------------------------------------------+
	*/
	query := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", schemaName, tableName)

	var tbl, createTable sql.NullString
	err := db.QueryRowContext(ctx, query).Scan(&tbl, &createTable)
	if err != nil {
		return "", errors.Trace(err)
	}
	if !tbl.Valid || !createTable.Valid {
		return "", errors.NotFoundf("table %s", tableName)
	}

	return createTable.String, nil
}

// GetRowCount returns row count of the table.
// if not specify where condition, return total row count of the table.
func GetRowCount(ctx context.Context, db *sql.DB, schemaName string, tableName string, where string) (int64, error) {
	/*
		select count example result:
		mysql> SELECT count(1) cnt from `test`.`itest` where id > 0;
		+------+
		| cnt  |
		+------+
		|  100 |
		+------+
	*/

	query := fmt.Sprintf("SELECT COUNT(1) cnt FROM `%s`.`%s`", schemaName, tableName)
	if len(where) > 0 {
		query += fmt.Sprintf(" WHERE %s", where)
	}

	var cnt sql.NullInt64
	err := db.QueryRowContext(ctx, query).Scan(&cnt)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if !cnt.Valid {
		return 0, errors.NotFoundf("table `%s`.`%s`", schemaName, tableName)
	}

	return cnt.Int64, nil
}

// GetRandomValues returns some random value of a column.
func GetRandomValues(ctx context.Context, db *sql.DB, schemaName, table, column string, num int64, min, max interface{}, limitRange string, collation string) ([]interface{}, error) {
	/*
		example:
		mysql> SELECT `id` FROM (SELECT `id` FROM `test`.`test` WHERE `id` COLLATE "latin1_bin" > 0 AND `id` COLLATE "latin1_bin" < 100 AND true ORDER BY RAND() LIMIT 3)rand_tmp ORDER BY `id`;
		+----------+
		| rand_tmp |
		+----------+
		|    15    |
		|    58    |
		|    67    |
		+----------+
	*/

	if limitRange != "" {
		limitRange = "true"
	}

	if collation != "" {
		collation = fmt.Sprintf(" COLLATE \"%s\"", collation)
	}

	randomValue := make([]interface{}, 0, num)
	query := fmt.Sprintf("SELECT `%s` FROM (SELECT `%s` FROM `%s`.`%s` WHERE `%s`%s > ? AND `%s`%s < ? AND %s ORDER BY RAND() LIMIT %d)rand_tmp ORDER BY `%s`",
		column, column, schemaName, table, column, collation, column, collation, limitRange, num, column)
	log.Debugf("get random values sql: %s, min: %v, max: %v", query, min, max)
	rows, err := db.QueryContext(ctx, query, min, max)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	for rows.Next() {
		var value interface{}
		err = rows.Scan(&value)
		if err != nil {
			return nil, errors.Trace(err)
		}
		randomValue = append(randomValue, value)
	}

	return randomValue, nil
}

// GetTables returns name of all tables in the specified schema
func GetTables(ctx context.Context, db *sql.DB, schemaName string) (tables []string, err error) {
	/*
		show tables without view: https://dev.mysql.com/doc/refman/5.7/en/show-tables.html

		example:
		mysql> show full tables in test where Table_Type != 'VIEW';
		+----------------+------------+
		| Tables_in_test | Table_type |
		+----------------+------------+
		| NTEST          | BASE TABLE |
		+----------------+------------+
	*/

	query := fmt.Sprintf("SHOW FULL TABLES IN `%s` WHERE Table_Type != 'VIEW';", schemaName)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	tables = make([]string, 0, 8)
	for rows.Next() {
		var table, tType sql.NullString
		err = rows.Scan(&table, &tType)
		if err != nil {
			return nil, errors.Trace(err)
		}

		if !table.Valid || !tType.Valid {
			continue
		}

		tables = append(tables, table.String)
	}

	return tables, errors.Trace(rows.Err())
}

// GetSchemas returns name of all schemas
func GetSchemas(ctx context.Context, db *sql.DB) ([]string, error) {
	query := "SHOW DATABASES"
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	// show an example.
	/*
		mysql> SHOW DATABASES;
		+--------------------+
		| Database           |
		+--------------------+
		| information_schema |
		| mysql              |
		| performance_schema |
		| sys                |
		| test_db            |
		+--------------------+
	*/
	schemas := make([]string, 0, 10)
	for rows.Next() {
		var schema string
		err = rows.Scan(&schema)
		if err != nil {
			return nil, errors.Trace(err)
		}
		schemas = append(schemas, schema)
	}
	return schemas, errors.Trace(rows.Err())
}

// GetCRC32Checksum returns checksum code of some data by given condition
func GetCRC32Checksum(ctx context.Context, db *sql.DB, schemaName, tableName string, tbInfo *model.TableInfo, limitRange string, args []interface{}, ignoreColumns map[string]interface{}) (int64, error) {
	/*
		calculate CRC32 checksum example:
		mysql> SELECT BIT_XOR(CAST(CRC32(CONCAT_WS(',', id, name, age, CONCAT(ISNULL(id), ISNULL(name), ISNULL(age))))AS UNSIGNED)) AS checksum FROM test.test WHERE id > 0 AND id < 10;
		+------------+
		| checksum   |
		+------------+
		| 1466098199 |
		+------------+
	*/
	columnNames := make([]string, 0, len(tbInfo.Columns))
	columnIsNull := make([]string, 0, len(tbInfo.Columns))
	for _, col := range tbInfo.Columns {
		if _, ok := ignoreColumns[col.Name.O]; ok {
			continue
		}
		columnNames = append(columnNames, col.Name.O)
		columnIsNull = append(columnIsNull, fmt.Sprintf("ISNULL(%s)", col.Name.O))
	}

	query := fmt.Sprintf("SELECT BIT_XOR(CAST(CRC32(CONCAT_WS(',', %s, CONCAT(%s)))AS UNSIGNED)) AS checksum FROM `%s`.`%s` WHERE %s;",
		strings.Join(columnNames, ", "), strings.Join(columnIsNull, ", "), schemaName, tableName, limitRange)
	log.Debugf("checksum sql: %s, args: %v", query, args)

	var checksum sql.NullInt64
	err := db.QueryRowContext(ctx, query, args...).Scan(&checksum)
	if err != nil {
		return -1, errors.Trace(err)
	}
	if !checksum.Valid {
		// if don't have any data, the checksum will be `NULL`
		log.Warnf("get empty checksum by query %s, args %v", query, args)
		return 0, nil
	}

	return checksum.Int64, nil
}

// GetTidbLatestTSO returns tidb's current TSO.
func GetTidbLatestTSO(ctx context.Context, db *sql.DB) (int64, error) {
	/*
		example in tidb:
		mysql> SHOW MASTER STATUS;
		+-------------+--------------------+--------------+------------------+-------------------+
		| File        | Position           | Binlog_Do_DB | Binlog_Ignore_DB | Executed_Gtid_Set |
		+-------------+--------------------+--------------+------------------+-------------------+
		| tidb-binlog | 400718757701615617 |              |                  |                   |
		+-------------+--------------------+--------------+------------------+-------------------+
	*/
	rows, err := db.QueryContext(ctx, "SHOW MASTER STATUS")
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer rows.Close()

	for rows.Next() {
		fields, _, err1 := ScanRow(rows)
		if err1 != nil {
			return 0, errors.Trace(err1)
		}

		ts, err1 := strconv.ParseInt(string(fields["Position"]), 10, 64)
		if err1 != nil {
			return 0, errors.Trace(err1)
		}
		return ts, nil
	}
	return 0, errors.New("get slave cluster's ts failed")
}

// SetSnapshot set the snapshot variable for tidb
func SetSnapshot(ctx context.Context, db *sql.DB, snapshot string) error {
	sql := fmt.Sprintf("SET @@tidb_snapshot='%s'", snapshot)
	log.Infof("set history snapshot: %s", sql)
	_, err := db.ExecContext(ctx, sql)
	return errors.Trace(err)
}

// GetDBVersion returns the database's version
func GetDBVersion(ctx context.Context, db *sql.DB) (string, error) {
	/*
		example in TiDB:
		mysql> select version();
		+--------------------------------------+
		| version()                            |
		+--------------------------------------+
		| 5.7.10-TiDB-v2.1.0-beta-173-g7e48ab1 |
		+--------------------------------------+

		example in MySQL:
		mysql> select version();
		+-----------+
		| version() |
		+-----------+
		| 5.7.21    |
		+-----------+
	*/
	query := "SELECT version()"
	result, err := db.QueryContext(ctx, query)
	if err != nil {
		return "", errors.Trace(err)
	}
	defer result.Close()

	var version sql.NullString
	for result.Next() {
		err := result.Scan(&version)
		if err != nil {
			return "", errors.Trace(err)
		}
		break
	}

	if version.Valid {
		return version.String, nil
	}

	return "", ErrVersionNotFound
}

// IsTiDB returns true if this database is tidb
func IsTiDB(ctx context.Context, db *sql.DB) (bool, error) {
	version, err := GetDBVersion(ctx, db)
	if err != nil {
		log.Errorf("get database's version meets error %v", err)
		return false, errors.Trace(err)
	}

	return strings.Contains(strings.ToLower(version), "tidb"), nil
}

// TableName returns `schema`.`table`
func TableName(schema, table string) string {
	return fmt.Sprintf("`%s`.`%s`", escapeName(schema), escapeName(table))
}

func escapeName(name string) string {
	return strings.Replace(name, "`", "``", -1)
}
