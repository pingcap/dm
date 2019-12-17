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
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/conn"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/retry"
	"github.com/pingcap/dm/pkg/schema"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/siddontang/go-mysql/mysql"
	"go.uber.org/zap/zapcore"
)

var (
	cpid               = "test_for_db"
	schemaCreateSQL    = ""
	tableCreateSQL     = ""
	clearCheckPointSQL = ""
	loadCheckPointSQL  = ""
	flushCheckPointSQL = ""
)

var _ = Suite(&testCheckpointSuite{})

type testCheckpointSuite struct {
	cfg     *config.SubTaskConfig
	mock    sqlmock.Sqlmock
	tracker *schema.Tracker
}

func (s *testCheckpointSuite) SetUpSuite(c *C) {
	s.cfg = &config.SubTaskConfig{
		ServerID:   101,
		MetaSchema: "test",
		Name:       "syncer_checkpoint_ut",
	}

	log.SetLevel(zapcore.ErrorLevel)
	var err error
	s.tracker, err = schema.NewTracker()
	c.Assert(err, IsNil)
}

func (s *testCheckpointSuite) TestUpTest(c *C) {
	err := s.tracker.Reset()
	c.Assert(err, IsNil)
}

func (s *testCheckpointSuite) prepareCheckPointSQL() {
	schemaCreateSQL = fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS `%s`", s.cfg.MetaSchema)
	tableCreateSQL = fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s_syncer_checkpoint` .*", s.cfg.MetaSchema, s.cfg.Name)
	flushCheckPointSQL = fmt.Sprintf("INSERT INTO `%s`.`%s_syncer_checkpoint` .* VALUES.* ON DUPLICATE KEY UPDATE .*", s.cfg.MetaSchema, s.cfg.Name)
	clearCheckPointSQL = fmt.Sprintf("DELETE FROM `%s`.`%s_syncer_checkpoint` WHERE id = \\?", s.cfg.MetaSchema, s.cfg.Name)
	loadCheckPointSQL = fmt.Sprintf("SELECT .* FROM `%s`.`%s_syncer_checkpoint` WHERE id = \\?", s.cfg.MetaSchema, s.cfg.Name)
}

// this test case uses sqlmock to simulate all SQL operations in tests
func (s *testCheckpointSuite) TestCheckPoint(c *C) {
	tctx := tcontext.Background()

	cp := NewRemoteCheckPoint(tctx, s.cfg, cpid)
	defer func() {
		s.mock.ExpectClose()
		cp.Close()
	}()

	var err error
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	s.mock = mock

	s.prepareCheckPointSQL()

	mock.ExpectBegin()
	mock.ExpectExec(schemaCreateSQL).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectExec(tableCreateSQL).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectExec(clearCheckPointSQL).WithArgs(cpid).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	dbConn, err := db.Conn(tcontext.Background().Context())
	c.Assert(err, IsNil)
	conn := &DBConn{cfg: s.cfg, baseConn: conn.NewBaseConn(dbConn, &retry.FiniteRetryStrategy{})}

	cp.(*RemoteCheckPoint).dbConn = conn
	err = cp.(*RemoteCheckPoint).prepare(tctx)
	c.Assert(err, IsNil)
	c.Assert(cp.Clear(tctx), IsNil)

	// test operation for global checkpoint
	s.testGlobalCheckPoint(c, cp)

	// test operation for table checkpoint
	s.testTableCheckPoint(c, cp)
}

func (s *testCheckpointSuite) testGlobalCheckPoint(c *C, cp CheckPoint) {
	tctx := tcontext.Background()

	// global checkpoint init to min
	c.Assert(cp.GlobalPoint(), Equals, minCheckpoint)
	c.Assert(cp.FlushedGlobalPoint(), Equals, minCheckpoint)

	// try load, but should load nothing
	s.mock.ExpectQuery(loadCheckPointSQL).WillReturnRows(sqlmock.NewRows(nil))
	err := cp.Load(tctx, s.tracker)
	c.Assert(err, IsNil)
	c.Assert(cp.GlobalPoint(), Equals, minCheckpoint)
	c.Assert(cp.FlushedGlobalPoint(), Equals, minCheckpoint)

	oldMode := s.cfg.Mode
	oldDir := s.cfg.Dir
	defer func() {
		s.cfg.Mode = oldMode
		s.cfg.Dir = oldDir
	}()

	// try load from mydumper's output
	pos1 := mysql.Position{
		Name: "mysql-bin.000003",
		Pos:  1943,
	}
	dir, err := ioutil.TempDir("", "test_global_checkpoint")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	filename := filepath.Join(dir, "metadata")
	err = ioutil.WriteFile(filename, []byte(
		fmt.Sprintf("SHOW MASTER STATUS:\n\tLog: %s\n\tPos: %d\n\tGTID:\n\nSHOW SLAVE STATUS:\n\tHost: %s\n\tLog: %s\n\tPos: %d\n\tGTID:\n\n", pos1.Name, pos1.Pos, "slave_host", pos1.Name, pos1.Pos+1000)),
		0644)
	c.Assert(err, IsNil)
	s.cfg.Mode = config.ModeAll
	s.cfg.Dir = dir

	s.mock.ExpectQuery(loadCheckPointSQL).WithArgs(cpid).WillReturnRows(sqlmock.NewRows(nil))
	err = cp.Load(tctx, s.tracker)
	c.Assert(err, IsNil)
	cp.SaveGlobalPoint(pos1)

	s.mock.ExpectBegin()
	s.mock.ExpectExec("(162)?"+flushCheckPointSQL).WithArgs(cpid, "", "", pos1.Name, pos1.Pos, []byte("null"), true).WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()
	err = cp.FlushPointsExcept(tctx, nil, nil, nil)
	c.Assert(err, IsNil)
	c.Assert(cp.GlobalPoint(), Equals, pos1)
	c.Assert(cp.FlushedGlobalPoint(), Equals, pos1)

	// try load from config
	pos1.Pos = 2044
	s.cfg.Mode = config.ModeIncrement
	s.cfg.Meta = &config.Meta{BinLogName: pos1.Name, BinLogPos: pos1.Pos}
	err = cp.LoadMeta()
	c.Assert(err, IsNil)
	c.Assert(cp.GlobalPoint(), Equals, pos1)
	c.Assert(cp.FlushedGlobalPoint(), Equals, pos1)

	s.cfg.Mode = oldMode
	s.cfg.Meta = nil

	// test save global point
	pos2 := mysql.Position{
		Name: "mysql-bin.000005",
		Pos:  2052,
	}
	cp.SaveGlobalPoint(pos2)
	c.Assert(cp.GlobalPoint(), Equals, pos2)
	c.Assert(cp.FlushedGlobalPoint(), Equals, pos1)

	// test rollback
	cp.Rollback(s.tracker)
	c.Assert(cp.GlobalPoint(), Equals, pos1)
	c.Assert(cp.FlushedGlobalPoint(), Equals, pos1)

	// save again
	cp.SaveGlobalPoint(pos2)
	c.Assert(cp.GlobalPoint(), Equals, pos2)
	c.Assert(cp.FlushedGlobalPoint(), Equals, pos1)

	// flush + rollback
	s.mock.ExpectBegin()
	s.mock.ExpectExec("(202)?"+flushCheckPointSQL).WithArgs(cpid, "", "", pos2.Name, pos2.Pos, []byte("null"), true).WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()
	err = cp.FlushPointsExcept(tctx, nil, nil, nil)
	c.Assert(err, IsNil)
	cp.Rollback(s.tracker)
	c.Assert(cp.GlobalPoint(), Equals, pos2)
	c.Assert(cp.FlushedGlobalPoint(), Equals, pos2)

	// try load from DB
	pos3 := pos2
	pos3.Pos = pos2.Pos + 1000 // > pos2 to enable save
	cp.SaveGlobalPoint(pos3)
	columns := []string{"cp_schema", "cp_table", "binlog_name", "binlog_pos", "table_info", "is_global"}
	s.mock.ExpectQuery(loadCheckPointSQL).WithArgs(cpid).WillReturnRows(sqlmock.NewRows(columns).AddRow("", "", pos2.Name, pos2.Pos, []byte("null"), true))
	err = cp.Load(tctx, s.tracker)
	c.Assert(err, IsNil)
	c.Assert(cp.GlobalPoint(), Equals, pos2)
	c.Assert(cp.FlushedGlobalPoint(), Equals, pos2)

	// test save older point
	/*var buf bytes.Buffer
	log.SetOutput(&buf)
	cp.SaveGlobalPoint(pos1)
	c.Assert(cp.GlobalPoint(), Equals, pos2)
	c.Assert(cp.FlushedGlobalPoint(), Equals, pos2)
	matchStr := fmt.Sprintf(".*try to save %s is older than current pos %s", pos1, pos2)
	matchStr = strings.Replace(strings.Replace(matchStr, ")", "\\)", -1), "(", "\\(", -1)
	c.Assert(strings.TrimSpace(buf.String()), Matches, matchStr)
	log.SetOutput(os.Stdout)*/

	// test clear
	s.mock.ExpectBegin()
	s.mock.ExpectExec(clearCheckPointSQL).WithArgs(cpid).WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()
	err = cp.Clear(tctx)
	c.Assert(err, IsNil)
	c.Assert(cp.GlobalPoint(), Equals, minCheckpoint)
	c.Assert(cp.FlushedGlobalPoint(), Equals, minCheckpoint)

	s.mock.ExpectQuery(loadCheckPointSQL).WillReturnRows(sqlmock.NewRows(nil))
	err = cp.Load(tctx, s.tracker)
	c.Assert(err, IsNil)
	c.Assert(cp.GlobalPoint(), Equals, minCheckpoint)
	c.Assert(cp.FlushedGlobalPoint(), Equals, minCheckpoint)
}

func (s *testCheckpointSuite) testTableCheckPoint(c *C, cp CheckPoint) {
	var (
		tctx   = tcontext.Background()
		schema = "test_db"
		table  = "test_table"
		pos1   = mysql.Position{
			Name: "mysql-bin.000008",
			Pos:  123,
		}
		pos2 = mysql.Position{
			Name: "mysql-bin.000008",
			Pos:  456,
		}
		err error
	)

	// not exist
	newer := cp.IsNewerTablePoint(schema, table, pos1)
	c.Assert(newer, IsTrue)

	// save
	cp.SaveTablePoint(schema, table, pos2, nil)
	newer = cp.IsNewerTablePoint(schema, table, pos1)
	c.Assert(newer, IsFalse)

	// rollback, to min
	cp.Rollback(s.tracker)
	newer = cp.IsNewerTablePoint(schema, table, pos1)
	c.Assert(newer, IsTrue)

	// save again
	cp.SaveTablePoint(schema, table, pos2, nil)
	newer = cp.IsNewerTablePoint(schema, table, pos1)
	c.Assert(newer, IsFalse)

	// flush + rollback
	s.mock.ExpectBegin()
	s.mock.ExpectExec("(284)?"+flushCheckPointSQL).WithArgs(cpid, schema, table, pos2.Name, pos2.Pos, sqlmock.AnyArg(), false).WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()
	err = cp.FlushPointsExcept(tctx, nil, nil, nil)
	c.Assert(err, IsNil)
	cp.Rollback(s.tracker)
	newer = cp.IsNewerTablePoint(schema, table, pos1)
	c.Assert(newer, IsFalse)

	// clear, to min
	s.mock.ExpectBegin()
	s.mock.ExpectExec(clearCheckPointSQL).WithArgs(cpid).WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()
	err = cp.Clear(tctx)
	c.Assert(err, IsNil)
	newer = cp.IsNewerTablePoint(schema, table, pos1)
	c.Assert(newer, IsTrue)

	// save
	cp.SaveTablePoint(schema, table, pos2, nil)
	newer = cp.IsNewerTablePoint(schema, table, pos1)
	c.Assert(newer, IsFalse)

	// test save table point less than global point
	func() {
		defer func() {
			r := recover()
			matchStr := fmt.Sprintf("table checkpoint %+v less than global checkpoint %+v.*", pos1, cp.GlobalPoint())
			matchStr = strings.ReplaceAll(strings.ReplaceAll(matchStr, "(", "\\("), ")", "\\)")
			c.Assert(r, Matches, matchStr)
		}()
		cp.SaveGlobalPoint(pos2)
		cp.SaveTablePoint(schema, table, pos1, nil)
	}()

	// flush but except + rollback
	s.mock.ExpectBegin()
	s.mock.ExpectExec("(320)?"+flushCheckPointSQL).WithArgs(cpid, "", "", pos2.Name, pos2.Pos, []byte("null"), true).WillReturnResult(sqlmock.NewResult(0, 1))
	s.mock.ExpectCommit()
	err = cp.FlushPointsExcept(tctx, [][]string{{schema, table}}, nil, nil)
	c.Assert(err, IsNil)
	cp.Rollback(s.tracker)
	newer = cp.IsNewerTablePoint(schema, table, pos1)
	c.Assert(newer, IsTrue)
}
