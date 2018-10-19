package utils

import (
	"database/sql"
	"fmt"
	"strconv"

	"github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/pingcap/tidb-enterprise-tools/pkg/gtid"
	tmysql "github.com/pingcap/tidb/mysql"
	gmysql "github.com/siddontang/go-mysql/mysql"
)

// GetMasterStatus gets status from master
func GetMasterStatus(db *sql.DB, flavor string) (gmysql.Position, gtid.Set, error) {
	var (
		binlogPos gmysql.Position
		gs        gtid.Set
	)

	rows, err := db.Query(`SHOW MASTER STATUS`)
	if err != nil {
		return binlogPos, gs, errors.Trace(err)
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return binlogPos, gs, errors.Trace(err)
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
			return binlogPos, gs, errors.Trace(err)
		}

		binlogPos = gmysql.Position{
			Name: binlogName,
			Pos:  pos,
		}

		gs, err = gtid.ParserGTID(flavor, gtidStr)
		if err != nil {
			return binlogPos, gs, errors.Trace(err)
		}
	}
	if rows.Err() != nil {
		return binlogPos, gs, errors.Trace(rows.Err())
	}

	return binlogPos, gs, nil
}

// GetGlobalVariable gets server's global variable
func GetGlobalVariable(db *sql.DB, variable string) (value string, err error) {
	query := fmt.Sprintf("SHOW GLOBAL VARIABLES LIKE '%s'", variable)
	rows, err := db.Query(query)
	if err != nil {
		return "", errors.Trace(err)
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
			return "", errors.Trace(err)
		}
	}

	if rows.Err() != nil {
		return "", errors.Trace(rows.Err())
	}

	return value, nil
}

// GetServerID gets server's `server_id`
func GetServerID(db *sql.DB) (int64, error) {
	serverIDStr, err := GetGlobalVariable(db, "server_id")
	if err != nil {
		return 0, errors.Trace(err)
	}

	serverID, err := strconv.ParseInt(serverIDStr, 10, 64)
	return serverID, errors.Trace(err)
}

// GetMariaDBGtidDomainID gets MariaDB server's `gtid_domain_id`
func GetMariaDBGtidDomainID(db *sql.DB) (uint32, error) {
	domainIDStr, err := GetGlobalVariable(db, "gtid_domain_id")
	if err != nil {
		return 0, errors.Trace(err)
	}

	domainID, err := strconv.ParseUint(domainIDStr, 10, 32)
	return uint32(domainID), errors.Trace(err)
}

// GetServerUUID gets server's `server_uuid`
func GetServerUUID(db *sql.DB) (string, error) {
	serverUUID, err := GetGlobalVariable(db, "server_uuid")
	return serverUUID, errors.Trace(err)
}

// KillConn kills the DB connection (thread in mysqld)
func KillConn(db *sql.DB, connID uint32) error {
	_, err := db.Exec(fmt.Sprintf("KILL %d", connID))
	return errors.Trace(err)
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

// IsErrTableNotExists checks whether err is TableNotExists error
func IsErrTableNotExists(err error) bool {
	return IsMySQLError(err, tmysql.ErrNoSuchTable)
}

// IsErrDupEntry checks whether err is DupEntry error
func IsErrDupEntry(err error) bool {
	return IsMySQLError(err, tmysql.ErrDupEntry)
}

// IsNoSuchThreadError checks whether err is NoSuchThreadError
func IsNoSuchThreadError(err error) bool {
	return IsMySQLError(err, tmysql.ErrNoSuchThread)
}
