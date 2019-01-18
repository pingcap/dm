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

package syncer

import (
	"database/sql/driver"

	"github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	tmysql "github.com/pingcap/parser/mysql"
	gmysql "github.com/siddontang/go-mysql/mysql"

	"github.com/pingcap/dm/pkg/utils"
)

func newMysqlErr(number uint16, message string) *mysql.MySQLError {
	return &mysql.MySQLError{
		Number:  number,
		Message: message,
	}
}

func (s *testSyncerSuite) TestIsRetryableError(c *C) {
	cases := []struct {
		err         error
		isRetryable bool
	}{
		{newMysqlErr(tmysql.ErrNoDB, "no db error"), false},
		{errors.New("unknown error"), false},
		{newMysqlErr(tmysql.ErrUnknown, "i/o timeout"), true},
		{newMysqlErr(tmysql.ErrDBCreateExists, "db already exists"), false},
		{driver.ErrBadConn, false},
		{newMysqlErr(gmysql.ER_LOCK_DEADLOCK, "Deadlock found when trying to get lock; try restarting transaction"), true},
		{newMysqlErr(tmysql.ErrPDServerTimeout, "pd server timeout"), true},
		{newMysqlErr(tmysql.ErrTiKVServerTimeout, "tikv server timeout"), true},
		{newMysqlErr(tmysql.ErrTiKVServerBusy, "tikv server busy"), true},
		{newMysqlErr(tmysql.ErrResolveLockTimeout, "resolve lock timeout"), true},
		{newMysqlErr(tmysql.ErrRegionUnavailable, "region unavailable"), true},
	}

	for _, t := range cases {
		c.Logf("err %v, expected %v", t.err, t.isRetryable)
		c.Assert(isRetryableError(t.err), Equals, t.isRetryable)
	}
}

func (s *testSyncerSuite) TestSpecificError(c *C) {
	err := newMysqlErr(tmysql.ErrNoSuchThread, "Unknown thread id: 111")
	c.Assert(utils.IsNoSuchThreadError(err), Equals, true)

	err = newMysqlErr(tmysql.ErrMasterFatalErrorReadingBinlog, "binlog purged error")
	c.Assert(isBinlogPurgedError(err), Equals, true)
}

func (s *testSyncerSuite) TestIsMysqlError(c *C) {
	cases := []struct {
		err          error
		code         uint16
		isMysqlError bool
	}{
		{newMysqlErr(tmysql.ErrNoSuchThread, "Unknown thread id: 1211"), 1094, true},
		{errors.New("not mysql error"), 1, false},
	}

	for _, t := range cases {
		c.Assert(isMysqlError(t.err, t.code), Equals, t.isMysqlError)
	}
}

func (s *testSyncerSuite) TestOriginError(c *C) {
	c.Assert(originError(nil), IsNil)

	err1 := errors.New("err1")
	c.Assert(originError(err1), DeepEquals, err1)

	err2 := errors.Trace(err1)
	c.Assert(originError(err2), DeepEquals, err1)

	err3 := errors.Trace(err2)
	c.Assert(originError(err3), DeepEquals, err1)
}
