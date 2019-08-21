package retry

import (
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	tmysql "github.com/pingcap/parser/mysql"
	gmysql "github.com/siddontang/go-mysql/mysql"
)

// loader define etry error

// IsLoaderRetryableError tells whether an error need retry in Loader executeSQL/querySQL
func IsLoaderRetryableError(err error) bool {
	err = errors.Cause(err)
	mysqlErr, ok := err.(*mysql.MySQLError)
	if ok {
		switch mysqlErr.Number {
		case tmysql.ErrDupEntry, tmysql.ErrDataTooLong:
			return false
		default:
			return true
		}
	}
	return true
}

// IsLoaderDDLRetryableError tells whether an error need retry in Loader executeDDL
func IsLoaderDDLRetryableError(err error) bool {
	err = errors.Cause(err) // check the original error
	mysqlErr, ok := err.(*mysql.MySQLError)
	if ok {
		switch mysqlErr.Number {
		case tmysql.ErrDBCreateExists, tmysql.ErrTableExists:
			return false
		default:
			return IsLoaderRetryableError(err)
		}
	}
	return IsLoaderRetryableError(err)
}

func isMySQLError(err error, code uint16) bool {
	err = errors.Cause(err)
	e, ok := err.(*mysql.MySQLError)
	return ok && e.Number == code
}

// syncer define retry error

// IsSyncerRetryableError tells whether an error need retry in sycner executeSQL/querySQL
func IsSyncerRetryableError(err error) bool {
	err = errors.Cause(err) // check the original error
	mysqlErr, ok := err.(*mysql.MySQLError)
	if ok {
		switch mysqlErr.Number {
		// ER_LOCK_DEADLOCK can retry to commit while meet deadlock
		case tmysql.ErrUnknown, gmysql.ER_LOCK_DEADLOCK, tmysql.ErrPDServerTimeout, tmysql.ErrTiKVServerTimeout, tmysql.ErrTiKVServerBusy, tmysql.ErrResolveLockTimeout, tmysql.ErrRegionUnavailable:
			return true
		default:
			return false
		}
	}

	return false
}
