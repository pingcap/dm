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
	sqlmock "github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/infoschema"
	gouuid "github.com/satori/go.uuid"
	"github.com/siddontang/go-mysql/mysql"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/baseconn"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/retry"
	"github.com/pingcap/dm/pkg/utils"
)

func (s *testSyncerSuite) TestGetServerUUID(c *C) {
	uuid, err := utils.GetServerUUID(s.db, "mysql")
	c.Assert(err, IsNil)
	_, err = gouuid.FromString(uuid)
	c.Assert(err, IsNil)
}

func (s *testSyncerSuite) TestGetServerID(c *C) {
	id, err := utils.GetServerID(s.db)
	c.Assert(err, IsNil)
	c.Assert(id, Greater, int64(0))
}

func (s *testSyncerSuite) TestBinaryLogs(c *C) {
	files, err := getBinaryLogs(s.db)
	c.Assert(err, IsNil)
	c.Assert(files, Not(HasLen), 0)

	fileNum := len(files)
	pos := mysql.Position{
		Name: files[fileNum-1].name,
		Pos:  0,
	}

	remainingSize, err := countBinaryLogsSize(pos, s.db)
	c.Assert(err, IsNil)
	c.Assert(remainingSize, Equals, files[fileNum-1].size)

	s.db.Exec("FLUSH BINARY LOGS")
	files, err = getBinaryLogs(s.db)
	c.Assert(err, IsNil)
	c.Assert(files, HasLen, fileNum+1)

	pos = mysql.Position{
		Name: files[fileNum].name,
		Pos:  0,
	}

	remainingSize, err = countBinaryLogsSize(pos, s.db)
	c.Assert(err, IsNil)
	c.Assert(remainingSize, Equals, files[fileNum].size)
}

func (s *testSyncerSuite) TestExecuteSQLSWithIgnore(c *C) {
	db, mock, err := sqlmock.New()
	conn := &Conn{
		baseConn: &baseconn.BaseConn{
			DB:            db,
			RetryStrategy: &retry.FiniteRetryStrategy{},
		},
		cfg: &config.SubTaskConfig{
			Name: "test",
		},
	}

	sqls := []string{"alter table t1 add column a int", "alter table t1 add column b int"}

	// will ignore the first error, and continue execute the second sql
	mock.ExpectBegin()
	mock.ExpectExec("alter table t1 add column a int").WillReturnError(newMysqlErr(uint16(infoschema.ErrColumnExists.Code()), "column a already exists"))
	mock.ExpectExec("alter table t1 add column b int").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	tctx := tcontext.Background().WithLogger(log.With(zap.String("test", "TestExecuteSQLSWithIgnore")))
	n, err := conn.executeSQLWithIgnore(tctx, ignoreDDLError, sqls)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 2)

	// will return error when execute the first sql
	mock.ExpectBegin()
	mock.ExpectExec("alter table t1 add column a int").WillReturnError(newMysqlErr(uint16(infoschema.ErrColumnExists.Code()), "column a already exists"))

	n, err = conn.executeSQL(tctx, sqls)
	c.Assert(err, ErrorMatches, ".*column a already exists")
	c.Assert(n, Equals, 0)
}
