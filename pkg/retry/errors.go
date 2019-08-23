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

// IsRetryableError tells whether this error should retry
func IsRetryableError(err error) bool {
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
	return errors.Cause(err) == mysql.ErrInvalidConn
}
