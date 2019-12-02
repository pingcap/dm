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
	"database/sql/driver"
	"github.com/pingcap/errors"
)

var (
	// UnsupportedDDLMsgs list the error messages of some unsupported DDL in TiDB
	UnsupportedDDLMsgs = []string{
		"can't drop column with index",
		"unsupported add column",
		"unsupported modify column",
		"unsupported modify charset",
		"unsupported modify collate",
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

// IsConnectionError tells whether this error should reconnect to Database
func IsConnectionError(err error) bool {
	err = errors.Cause(err)
	switch err {
	case driver.ErrBadConn:
		return true
	}
	return false
}
