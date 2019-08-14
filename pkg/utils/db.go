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

package utils

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser"
	tmysql "github.com/pingcap/parser/mysql"
	gmysql "github.com/siddontang/go-mysql/mysql"
	"go.uber.org/zap"
)

var (
	// for MariaDB, UUID set as `gtid_domain_id` + domainServerIDSeparator + `server_id`
	domainServerIDSeparator = "-"
)

// GetMasterStatus gets status from master
func GetMasterStatus(db *sql.DB, flavor string) (gmysql.Position, gtid.Set, error) {
	var (
		binlogPos gmysql.Position
		gs        gtid.Set
	)

	rows, err := db.Query(`SHOW MASTER STATUS`)

	failpoint.Inject("GetMasterStatusFailed", func(val failpoint.Value) {
		err = tmysql.NewErr(uint16(val.(int)))
		log.L().Warn("GetMasterStatus failed", zap.String("failpoint", "GetMasterStatusFailed"), zap.Error(err))
	})

	if err != nil {
		return binlogPos, gs, err
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return binlogPos, gs, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}

	// Show an example.
	/*
		MySQL [test]> SHOW MASTER STATUS;
		+-----------+----------+--------------+------------------+--------------------------------------------+
		| File      | Position | Binlog_Do_DB | Binlog_Ignore_DB | Executed_Gtid_Set                          |
		+-----------+----------+--------------+------------------+--------------------------------------------+
		| ON.000001 |     4822 |              |                  | 85ab69d1-b21f-11e6-9c5e-64006a8978d2:1-46
		+-----------+----------+--------------+------------------+--------------------------------------------+
	*/
	var (
		gtidStr    string
		binlogName string
		pos        uint32
		nullPtr    interface{}
	)
	for rows.Next() {
		if len(rowColumns) == 5 {
			err = rows.Scan(&binlogName, &pos, &nullPtr, &nullPtr, &gtidStr)
		} else {
			err = rows.Scan(&binlogName, &pos, &nullPtr, &nullPtr)
		}
		if err != nil {
			return binlogPos, gs, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
		}

		binlogPos = gmysql.Position{
			Name: binlogName,
			Pos:  pos,
		}

		gs, err = gtid.ParserGTID(flavor, gtidStr)
		if err != nil {
			return binlogPos, gs, err
		}
	}
	if rows.Err() != nil {
		return binlogPos, gs, terror.DBErrorAdapt(rows.Err(), terror.ErrDBDriverError)
	}

	if flavor == gmysql.MariaDBFlavor && (gs == nil || gs.String() == "") {
		gs, err = GetMariaDBGTID(db)
		if err != nil {
			return binlogPos, gs, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
		}
	}

	return binlogPos, gs, nil
}

// GetMariaDBGTID gets MariaDB's `gtid_binlog_pos`
// it can not get by `SHOW MASTER STATUS`
func GetMariaDBGTID(db *sql.DB) (gtid.Set, error) {
	gtidStr, err := GetGlobalVariable(db, "gtid_binlog_pos")
	if err != nil {
		return nil, err
	}
	gs, err := gtid.ParserGTID(gmysql.MariaDBFlavor, gtidStr)
	if err != nil {
		return nil, err
	}
	return gs, nil
}

// GetGlobalVariable gets server's global variable
func GetGlobalVariable(db *sql.DB, variable string) (value string, err error) {
	query := fmt.Sprintf("SHOW GLOBAL VARIABLES LIKE '%s'", variable)
	rows, err := db.Query(query)

	failpoint.Inject("GetGlobalVariableFailed", func(val failpoint.Value) {
		items := strings.Split(val.(string), ",")
		if len(items) != 2 {
			log.L().Fatal("failpoint GetGlobalVariableFailed's value is invalid", zap.String("val", val.(string)))
		}
		variableName := items[0]
		errCode, err1 := strconv.ParseUint(items[1], 10, 16)
		if err1 != nil {
			log.L().Fatal("failpoint GetGlobalVariableFailed's value is invalid", zap.String("val", val.(string)))
		}

		if variable == variableName {
			err = tmysql.NewErr(uint16(errCode))
			log.L().Warn("GetGlobalVariable failed", zap.String("variable", variable), zap.String("failpoint", "GetGlobalVariableFailed"), zap.Error(err))
		}
	})

	if err != nil {
		return "", terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	defer rows.Close()

	// Show an example.
	/*
		mysql> SHOW GLOBAL VARIABLES LIKE "binlog_format";
		+---------------+-------+
		| Variable_name | Value |
		+---------------+-------+
		| binlog_format | ROW   |
		+---------------+-------+
	*/

	for rows.Next() {
		err = rows.Scan(&variable, &value)
		if err != nil {
			return "", terror.DBErrorAdapt(err, terror.ErrDBDriverError)
		}
	}

	if rows.Err() != nil {
		return "", terror.DBErrorAdapt(rows.Err(), terror.ErrDBDriverError)
	}

	return value, nil
}

// GetServerID gets server's `server_id`
func GetServerID(db *sql.DB) (int64, error) {
	serverIDStr, err := GetGlobalVariable(db, "server_id")
	if err != nil {
		return 0, err
	}

	serverID, err := strconv.ParseInt(serverIDStr, 10, 64)
	return serverID, terror.ErrInvalidServerID.Delegate(err, serverIDStr)
}

// GetMariaDBGtidDomainID gets MariaDB server's `gtid_domain_id`
func GetMariaDBGtidDomainID(db *sql.DB) (uint32, error) {
	domainIDStr, err := GetGlobalVariable(db, "gtid_domain_id")
	if err != nil {
		return 0, err
	}

	domainID, err := strconv.ParseUint(domainIDStr, 10, 32)
	return uint32(domainID), terror.ErrMariaDBDomainID.Delegate(err, domainIDStr)
}

// GetServerUUID gets server's `server_uuid`
func GetServerUUID(db *sql.DB, flavor string) (string, error) {
	if flavor == gmysql.MariaDBFlavor {
		return GetMariaDBUUID(db)
	}
	serverUUID, err := GetGlobalVariable(db, "server_uuid")
	return serverUUID, err
}

// GetMariaDBUUID gets equivalent `server_uuid` for MariaDB
// `gtid_domain_id` joined `server_id` with domainServerIDSeparator
func GetMariaDBUUID(db *sql.DB) (string, error) {
	domainID, err := GetMariaDBGtidDomainID(db)
	if err != nil {
		return "", err
	}
	serverID, err := GetServerID(db)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%d%s%d", domainID, domainServerIDSeparator, serverID), nil
}

// GetSQLMode returns sql_mode.
func GetSQLMode(db *sql.DB) (tmysql.SQLMode, error) {
	sqlMode, err := GetGlobalVariable(db, "sql_mode")
	if err != nil {
		return tmysql.ModeNone, err
	}

	mode, err := tmysql.GetSQLMode(sqlMode)
	return mode, terror.ErrGetSQLModeFromStr.Delegate(err, sqlMode)
}

// HasAnsiQuotesMode checks whether database has `ANSI_QUOTES` set
func HasAnsiQuotesMode(db *sql.DB) (bool, error) {
	mode, err := GetSQLMode(db)
	if err != nil {
		return false, err
	}
	return mode.HasANSIQuotesMode(), nil
}

// GetParser gets a parser which maybe enabled `ANSI_QUOTES` sql_mode
func GetParser(db *sql.DB, ansiQuotesMode bool) (*parser.Parser, error) {
	if !ansiQuotesMode {
		// try get from DB
		var err error
		ansiQuotesMode, err = HasAnsiQuotesMode(db)
		if err != nil {
			return nil, err
		}
	}

	parser2 := parser.New()
	if ansiQuotesMode {
		parser2.SetSQLMode(tmysql.ModeANSIQuotes)
	}
	return parser2, nil
}

// KillConn kills the DB connection (thread in mysqld)
func KillConn(db *sql.DB, connID uint32) error {
	_, err := db.Exec(fmt.Sprintf("KILL %d", connID))
	return terror.DBErrorAdapt(err, terror.ErrDBDriverError)
}

// IsMySQLError checks whether err is MySQLError error
func IsMySQLError(err error, code uint16) bool {
	err = errors.Cause(err)
	e, ok := err.(*mysql.MySQLError)
	return ok && e.Number == code
}

// IsErrBinlogPurged checks whether err is BinlogPurged error
func IsErrBinlogPurged(err error) bool {
	err = errors.Cause(err)
	e, ok := err.(*gmysql.MyError)
	return ok && e.Code == tmysql.ErrMasterFatalErrorReadingBinlog
}

// IsNoSuchThreadError checks whether err is NoSuchThreadError
func IsNoSuchThreadError(err error) bool {
	return IsMySQLError(err, tmysql.ErrNoSuchThread)
}

// BaseConn Connect to Database
type BaseConn struct {
	DB *sql.DB
}

// SQL is format sql job
type SQL struct {
	Query string
	Args  []interface{}
}

// Init base BaseConn
func (conn *BaseConn) Init(dbDSN string) error {
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return errors.Trace(err)
	}
	conn.DB = db
	return nil
}

// RetryOperation defines an abstract retry operation for db
func (conn *BaseConn) RetryOperation(operateFn func() (interface{}, error), retryFn func(error) bool) (interface{}, error) {
	var err error
	var ret interface{}
	for {
		ret, err = operateFn()
		if err != nil {
			if retryFn(err) {
				continue
			}
		}
		break
	}
	return ret, err
}

// QuerySQL query sql
func (conn *BaseConn) QuerySQL(tctx *tcontext.Context, query string) (*sql.Rows, error) {
	if conn == nil || conn.DB == nil {
		return nil, errors.NotValidf("database connection")
	}

	var (
		err  error
		rows *sql.Rows
	)

	tctx.L().Debug("Query statement", zap.String("Query", query))

	rows, err = conn.DB.QueryContext(tctx.Context(), query)

	if err != nil {
		tctx.L().Error("Query statement failed", zap.String("Query", query), log.ShortError(err))
		return nil, errors.Trace(err)
	}
	return rows, nil
}

// ExecuteSQL execute sql
func (conn *BaseConn) ExecuteSQL(tctx *tcontext.Context, sqls []SQL) (int, error) {
	if len(sqls) == 0 {
		return 0, nil
	}

	if conn == nil || conn.DB == nil {
		return 0, errors.NotValidf("database connection")
	}

	if affected, err := conn.executeSQLImp(tctx, sqls); err != nil {
		return affected, err
	}
	return 0, nil
}

func (conn *BaseConn) executeSQLImp(tctx *tcontext.Context, sqls []SQL) (int, error) {
	if conn == nil || conn.DB == nil {
		return 0, errors.NotValidf("database connection")
	}

	txn, err := conn.DB.Begin()
	if err != nil {
		return 0, errors.Trace(err)
	}

	l := len(sqls)

	for i := range sqls {
		tctx.L().Debug("execute statement", zap.String("Query", sqls[i].Query), zap.Reflect("argument", sqls[i].Args))

		_, err = txn.ExecContext(tctx.Context(), sqls[i].Query, sqls[i].Args...)
		if err != nil {
			tctx.L().Error("execute statement failed", zap.String("Query", sqls[i].Query), zap.Reflect("argument", sqls[i].Args), log.ShortError(err))
			rerr := txn.Rollback()
			if rerr != nil {
				tctx.L().Error("rollback failed", zap.String("Query", sqls[i].Query), zap.Reflect("argument", sqls[i].Args), log.ShortError(rerr))
			}
			// we should return the exec err, instead of the rollback rerr.
			return i, errors.Trace(err)
		}
	}
	err = txn.Commit()
	if err != nil {
		return l, errors.Trace(err)
	}
	return l, nil
}

// Close is close DB resource
func (conn *BaseConn) Close() error {
	if conn == nil || conn.DB == nil {
		return nil
	}

	return errors.Trace(conn.DB.Close())
}
