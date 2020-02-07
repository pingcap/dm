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
	"context"
	"database/sql"
	"fmt"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/pingcap/tidb/infoschema"
	gouuid "github.com/satori/go.uuid"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/conn"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/retry"
	"github.com/pingcap/dm/pkg/utils"
)

var _ = Suite(&testDBSuite{})

type testDBSuite struct {
	db       *sql.DB
	syncer   *replication.BinlogSyncer
	streamer *replication.BinlogStreamer
	cfg      *config.SubTaskConfig
}

func (s *testDBSuite) SetUpSuite(c *C) {
	s.cfg = &config.SubTaskConfig{
		From:       getDBConfigFromEnv(),
		To:         getDBConfigFromEnv(),
		ServerID:   102,
		MetaSchema: "db_test",
		Name:       "db_ut",
		Mode:       config.ModeIncrement,
		Flavor:     "mysql",
	}
	s.cfg.From.Adjust()
	s.cfg.To.Adjust()

	dir := c.MkDir()
	s.cfg.RelayDir = dir

	var err error
	dbAddr := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8", s.cfg.From.User, s.cfg.From.Password, s.cfg.From.Host, s.cfg.From.Port)
	s.db, err = sql.Open("mysql", dbAddr)
	c.Assert(err, IsNil)

	s.resetBinlogSyncer(c)
	_, err = s.db.Exec("SET GLOBAL binlog_format = 'ROW';")
	c.Assert(err, IsNil)
}

func (s *testDBSuite) resetBinlogSyncer(c *C) {
	cfg := replication.BinlogSyncerConfig{
		ServerID:       uint32(s.cfg.ServerID),
		Flavor:         "mysql",
		Host:           s.cfg.From.Host,
		Port:           uint16(s.cfg.From.Port),
		User:           s.cfg.From.User,
		Password:       s.cfg.From.Password,
		UseDecimal:     true,
		VerifyChecksum: true,
	}
	if s.cfg.Timezone != "" {
		timezone, err2 := time.LoadLocation(s.cfg.Timezone)
		c.Assert(err2, IsNil)
		cfg.TimestampStringLocation = timezone
	}

	if s.syncer != nil {
		s.syncer.Close()
	}

	pos, _, err := utils.GetMasterStatus(s.db, "mysql")
	c.Assert(err, IsNil)

	s.syncer = replication.NewBinlogSyncer(cfg)
	s.streamer, err = s.syncer.StartSync(pos)
	c.Assert(err, IsNil)
}

func (s *testDBSuite) TestGetServerUUID(c *C) {
	uuid, err := utils.GetServerUUID(s.db, "mysql")
	c.Assert(err, IsNil)
	_, err = gouuid.FromString(uuid)
	c.Assert(err, IsNil)
}

func (s *testDBSuite) TestGetServerID(c *C) {
	id, err := utils.GetServerID(s.db)
	c.Assert(err, IsNil)
	c.Assert(id, Greater, uint32(0))
}

func (s *testDBSuite) TestBinaryLogs(c *C) {
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
	c.Assert(err, IsNil)
	dbConn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)
	conn := &DBConn{
		baseConn: &conn.BaseConn{
			DBConn:        dbConn,
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
	mock.ExpectRollback()

	n, err = conn.executeSQL(tctx, sqls)
	c.Assert(err, ErrorMatches, ".*column a already exists")
	c.Assert(n, Equals, 0)

	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *testDBSuite) TestTimezone(c *C) {
	s.cfg.BWList = &filter.Rules{
		DoDBs:     []string{"~^tztest_.*"},
		IgnoreDBs: []string{"stest", "~^foo.*"},
	}

	createSQLs := []string{
		"create database if not exists tztest_1",
		"create table if not exists tztest_1.t_1(id int, a timestamp)",
	}

	testCases := []struct {
		sqls     []string
		timezone string
	}{
		{
			[]string{
				"insert into tztest_1.t_1(id, a) values (1, '1990-04-15 01:30:12')",
				"insert into tztest_1.t_1(id, a) values (2, '1990-04-15 02:30:12')",
				"insert into tztest_1.t_1(id, a) values (3, '1990-04-15 03:30:12')",
			},
			"Asia/Shanghai",
		},
		{
			[]string{
				"insert into tztest_1.t_1(id, a) values (4, '1990-04-15 01:30:12')",
				"insert into tztest_1.t_1(id, a) values (5, '1990-04-15 02:30:12')",
				"insert into tztest_1.t_1(id, a) values (6, '1990-04-15 03:30:12')",
			},
			"America/Phoenix",
		},
	}
	queryTs := "select unix_timestamp(a) from `tztest_1`.`t_1` where id = ?"

	dropSQLs := []string{
		"drop table tztest_1.t_1",
		"drop database tztest_1",
	}

	defer func() {
		for _, sql := range dropSQLs {
			_, err := s.db.Exec(sql)
			c.Assert(err, IsNil)
		}
	}()

	for _, sql := range createSQLs {
		_, err := s.db.Exec(sql)
		c.Assert(err, IsNil)
	}

	for _, testCase := range testCases {
		s.cfg.Timezone = testCase.timezone
		syncer := NewSyncer(s.cfg, nil)
		syncer.genRouter()
		s.resetBinlogSyncer(c)

		// we should not use `sql.DB.Exec` to do query which depends on session variables
		// because `sql.DB.Exec` will choose a underlying DBConn for every query from the connection pool
		// and different Conn using different session
		// ref: `sql.DB.Conn`
		// and `set @@global` is also not reasonable, because it can not affect sessions already exist
		// if we must ensure multi queries use the same session, we should use a transaction
		txn, err := s.db.Begin()
		c.Assert(err, IsNil)
		txn.Exec("set @@session.time_zone = ?", testCase.timezone)
		txn.Exec("set @@session.sql_mode = ''")
		for _, sql := range testCase.sqls {
			_, err = txn.Exec(sql)
			c.Assert(err, IsNil)
		}
		err = txn.Commit()
		c.Assert(err, IsNil)

		location, err := time.LoadLocation(testCase.timezone)
		c.Assert(err, IsNil)

		idx := 0
		for {
			if idx >= len(testCase.sqls) {
				break
			}
			e, err := s.streamer.GetEvent(context.Background())
			c.Assert(err, IsNil)
			switch ev := e.Event.(type) {
			case *replication.RowsEvent:
				skip, err := syncer.skipDMLEvent(string(ev.Table.Schema), string(ev.Table.Table), e.Header.EventType)
				c.Assert(err, IsNil)
				if skip {
					continue
				}

				rowid := ev.Rows[0][0].(int32)
				var ts sql.NullInt64
				err2 := s.db.QueryRow(queryTs, rowid).Scan(&ts)
				c.Assert(err2, IsNil)
				c.Assert(ts.Valid, IsTrue)

				raw := ev.Rows[0][1].(string)
				data, err := time.ParseInLocation("2006-01-02 15:04:05", raw, location)
				c.Assert(err, IsNil)
				c.Assert(data.Unix(), DeepEquals, ts.Int64)
				idx++
			default:
				continue
			}
		}
	}
}
