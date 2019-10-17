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
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	tmysql "github.com/pingcap/parser/mysql"
	gmysql "github.com/siddontang/go-mysql/mysql"
)

var (
	// UnsupportedDDLMsgs list the error messages of some unsupported DDL in TiDB
	UnsupportedDDLMsgs = []string{
		"can't drop column with index",
		"unsupported add column",
		"unsupported modify column",
		"unsupported modify",
		"unsupported drop integer primary key",
	}

	// UnsupportedDMLMsgs list the error messages of some un-recoverable DML, which is used in task auto recovery
	UnsupportedDMLMsgs = []string{
		"Error 1062: Duplicate entry",
		"Error 1406: Data too long for column",
	}

	// ParseRelayLogErrMsgs list the error messages of some un-recoverable relay log parsing error, which is used in task auto recovery.
	ParseRelayLogErrMsgs = []string{
		"binlog checksum mismatch, data may be corrupted",
		"get event err EOF",
	}
)

// IsRetryableError tells whether this error should retry
func IsRetryableError(err error) bool {
	err = errors.Cause(err) // check the original error
	mysqlErr, ok := err.(*mysql.MySQLError)
	if ok {
		switch mysqlErr.Number {
		// ER_LOCK_DEADLOCK can retry to commit while meet deadlock
		case tmysql.ErrUnknown, gmysql.ER_LOCK_DEADLOCK, tmysql.ErrPDServerTimeout, tmysql.ErrTiKVServerTimeout, tmysql.ErrTiKVServerBusy, tmysql.ErrResolveLockTimeout, tmysql.ErrRegionUnavailable, tmysql.ErrQueryInterrupted, tmysql.ErrWriteConflictInTiDB, tmysql.ErrTableLocked, tmysql.ErrWriteConflict:
			return true
		default:
			return false
		}
	}
	return false
}

// IsRetryableErrorFastFailFilter tells whether this error should retry,
// filtering some incompatible DDL error to achieve fast fail.
func IsRetryableErrorFastFailFilter(err error) bool {
	err2 := errors.Cause(err) // check the original error
	if mysqlErr, ok := err2.(*mysql.MySQLError); ok && mysqlErr.Number == tmysql.ErrUnknown {
		for _, msg := range UnsupportedDDLMsgs {
			if strings.Contains(mysqlErr.Message, msg) {
				return false
			}
		}
	}

	return IsRetryableError(err)
}
