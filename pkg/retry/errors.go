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

package retry

import (
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	tmysql "github.com/pingcap/parser/mysql"
	gmysql "github.com/siddontang/go-mysql/mysql"
)

// IsLoaderRetryableError tells whether an error need retry in Loader executeSQL/querySQL
func IsLoaderRetryableError(err error) bool {
	if err == nil {
		return false
	}
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
	if err == nil {
		return false
	}
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

// IsSyncerRetryableError tells whether an error need retry in Syncer executeSQL/querySQL
func IsSyncerRetryableError(err error) bool {
	if err == nil {
		return false
	}
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

// IsInvalidConnError tells whether it's a mysql connection error
func IsInvalidConnError(err error) bool {
	if err == nil {
		return false
	}
	err = errors.Cause(err)
	switch err {
	case mysql.ErrInvalidConn:
		return true
	}
	return false
}
