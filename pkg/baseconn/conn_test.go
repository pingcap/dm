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

package baseconn

import (
	"errors"
	"testing"

	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/retry"
	"github.com/pingcap/dm/pkg/terror"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
)

func TestSuite(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testBaseConnSuite{})

type testBaseConnSuite struct {
}

func (t *testBaseConnSuite) TestBaseConn(c *C) {
	baseConn, err := NewBaseConn("error dsn", nil)
	c.Assert(terror.ErrDBDriverError.Equal(err), IsTrue)

	tctx := tcontext.Background()
	err = baseConn.ResetConn(tctx)
	c.Assert(terror.ErrDBUnExpect.Equal(err), IsTrue)

	err = baseConn.SetRetryStrategy(nil)
	c.Assert(terror.ErrDBUnExpect.Equal(err), IsTrue)

	_, err = baseConn.QuerySQL(tctx, "select 1")
	c.Assert(terror.ErrDBUnExpect.Equal(err), IsTrue)

	_, err = baseConn.ExecuteSQL(tctx, []SQL{{"", nil}})
	c.Assert(terror.ErrDBUnExpect.Equal(err), IsTrue)

	db, mock, err := sqlmock.New()
	baseConn = &BaseConn{db, "", nil}

	err = baseConn.SetRetryStrategy(&retry.FiniteRetryStrategy{})
	c.Assert(err, IsNil)

	mock.ExpectQuery("select 1").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	rows, err := baseConn.QuerySQL(tctx, "select 1")
	c.Assert(err, IsNil)
	ids := make([]int, 0, 1)
	for rows.Next() {
		var id int
		err = rows.Scan(&id)
		c.Assert(err, IsNil)
		ids = append(ids, id)
	}
	c.Assert(len(ids), Equals, 1)
	c.Assert(ids[0], Equals, 1)

	mock.ExpectQuery("select 1").WillReturnError(errors.New("invalid connection"))
	_, err = baseConn.QuerySQL(tctx, "select 1")
	c.Assert(terror.ErrDBQueryFailed.Equal(err), IsTrue)

	sqls := []SQL{}
	affected, _ := baseConn.ExecuteSQL(tctx, sqls)
	c.Assert(affected, Equals, 0)

	mock.ExpectBegin()
	mock.ExpectExec("create database test").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	sqls = []SQL{{"create database test", nil}}
	affected, err = baseConn.ExecuteSQL(tctx, sqls)
	c.Assert(err, IsNil)
	c.Assert(affected, Equals, 1)

	mock.ExpectBegin().WillReturnError(errors.New("begin error"))
	sqls = []SQL{{"create database test", nil}}
	_, err = baseConn.ExecuteSQL(tctx, sqls)
	c.Assert(terror.ErrDBExecuteFailed.Equal(err), IsTrue)

	mock.ExpectBegin()
	mock.ExpectExec("create database test").WillReturnError(errors.New("invalid connection"))
	mock.ExpectRollback()
	sqls = []SQL{{"create database test", nil}}
	_, err = baseConn.ExecuteSQL(tctx, sqls)
	c.Assert(terror.ErrDBExecuteFailed.Equal(err), IsTrue)

	if err = mock.ExpectationsWereMet(); err != nil {
		c.Fatal("thers were unexpected:", err)
	}
}
