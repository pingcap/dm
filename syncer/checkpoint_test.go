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
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	. "github.com/pingcap/check"
	"github.com/siddontang/go-mysql/mysql"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/log"
)

// NOTE: there are binlog events conflict with other test cases
// and we can not simply disable `sql_log_bin` for this test case
// because go-sql-driver/mysql using connection pool for *sql.DB
// and different SQLs in different txn may in different sessions
// ref: https://github.com/go-sql-driver/mysql/issues/208
// so we use different DB from other tests.
func (s *testSyncerSuite) TestCheckPoint(c *C) {
	id := "test_for_db"
	cp := NewRemoteCheckPoint(s.cfg, id)
	defer cp.Close()

	err := cp.Init()
	c.Assert(err, IsNil)
	cp.Clear()

	// test operation for global checkpoint
	s.testGlobalCheckPoint(c, cp)

	// test operation for table checkpoint
	s.testTableCheckPoint(c, cp)
}

func (s *testSyncerSuite) testGlobalCheckPoint(c *C, cp CheckPoint) {
	// global checkpoint init to min
	c.Assert(cp.GlobalPoint(), Equals, minCheckpoint)
	c.Assert(cp.FlushedGlobalPoint(), Equals, minCheckpoint)

	// try load, but should load nothing
	err := cp.Load()
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
	err = cp.Load()
	c.Assert(err, IsNil)
	cp.SaveGlobalPoint(pos1)
	cp.FlushPointsExcept(nil)
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
	cp.Rollback()
	c.Assert(cp.GlobalPoint(), Equals, pos1)
	c.Assert(cp.FlushedGlobalPoint(), Equals, pos1)

	// save again
	cp.SaveGlobalPoint(pos2)
	c.Assert(cp.GlobalPoint(), Equals, pos2)
	c.Assert(cp.FlushedGlobalPoint(), Equals, pos1)

	// flush + rollback
	cp.FlushPointsExcept(nil)
	cp.Rollback()
	c.Assert(cp.GlobalPoint(), Equals, pos2)
	c.Assert(cp.FlushedGlobalPoint(), Equals, pos2)

	// try load from DB
	pos3 := pos2
	pos3.Pos = pos2.Pos + 1000 // > pos2 to enable save
	cp.SaveGlobalPoint(pos3)
	err = cp.Load()
	c.Assert(err, IsNil)
	c.Assert(cp.GlobalPoint(), Equals, pos2)
	c.Assert(cp.FlushedGlobalPoint(), Equals, pos2)

	// test save older point
	var buf bytes.Buffer
	log.SetOutput(&buf)
	cp.SaveGlobalPoint(pos1)
	c.Assert(err, IsNil)
	c.Assert(cp.GlobalPoint(), Equals, pos2)
	c.Assert(cp.FlushedGlobalPoint(), Equals, pos2)
	matchStr := fmt.Sprintf(".*try to save %s is older than current pos %s", pos1, pos2)
	matchStr = strings.Replace(strings.Replace(matchStr, ")", "\\)", -1), "(", "\\(", -1)
	c.Assert(strings.TrimSpace(buf.String()), Matches, matchStr)
	log.SetOutput(os.Stdout)

	// test clear
	err = cp.Clear()
	c.Assert(err, IsNil)
	c.Assert(cp.GlobalPoint(), Equals, minCheckpoint)
	c.Assert(cp.FlushedGlobalPoint(), Equals, minCheckpoint)

	err = cp.Load()
	c.Assert(err, IsNil)
	c.Assert(cp.GlobalPoint(), Equals, minCheckpoint)
	c.Assert(cp.FlushedGlobalPoint(), Equals, minCheckpoint)
}

func (s *testSyncerSuite) testTableCheckPoint(c *C, cp CheckPoint) {
	var (
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
	)

	// not exist
	newer := cp.IsNewerTablePoint(schema, table, pos1)
	c.Assert(newer, IsTrue)

	// save
	cp.SaveTablePoint(schema, table, pos2)
	newer = cp.IsNewerTablePoint(schema, table, pos1)
	c.Assert(newer, IsFalse)

	// rollback, to min
	cp.Rollback()
	newer = cp.IsNewerTablePoint(schema, table, pos1)
	c.Assert(newer, IsTrue)

	// save again
	cp.SaveTablePoint(schema, table, pos2)
	newer = cp.IsNewerTablePoint(schema, table, pos1)
	c.Assert(newer, IsFalse)

	// flush + rollback
	cp.FlushPointsExcept(nil)
	cp.Rollback()
	newer = cp.IsNewerTablePoint(schema, table, pos1)
	c.Assert(newer, IsFalse)

	// clear, to min
	err := cp.Clear()
	c.Assert(err, IsNil)
	newer = cp.IsNewerTablePoint(schema, table, pos1)
	c.Assert(newer, IsTrue)

	// save
	cp.SaveTablePoint(schema, table, pos2)
	newer = cp.IsNewerTablePoint(schema, table, pos1)
	c.Assert(newer, IsFalse)

	// flush but except + rollback
	cp.FlushPointsExcept([][]string{{schema, table}})
	cp.Rollback()
	newer = cp.IsNewerTablePoint(schema, table, pos1)
	c.Assert(newer, IsTrue)
}
