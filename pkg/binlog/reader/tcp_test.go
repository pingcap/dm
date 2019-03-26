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

package reader

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	gmysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"

	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/utils"
)

var (
	_           = Suite(&testTCPReaderSuite{})
	dbName      = "test_tcp_reader_db"
	tableName   = "test_tcp_reader_table"
	columnValue = 123
	flavor      = gmysql.MySQLFlavor
	serverIDs   = []uint32{3251, 3252}
)

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testTCPReaderSuite struct {
	host     string
	port     int
	user     string
	password string
	db       *sql.DB
}

func (t *testTCPReaderSuite) SetUpSuite(c *C) {
	t.setUpConn(c)
	t.setUpData(c)
}

func (t *testTCPReaderSuite) setUpConn(c *C) {
	t.host = os.Getenv("MYSQL_HOST")
	if t.host == "" {
		t.host = "127.0.0.1"
	}
	t.port, _ = strconv.Atoi(os.Getenv("MYSQL_PORT"))
	if t.port == 0 {
		t.port = 3306
	}
	t.user = os.Getenv("MYSQL_USER")
	if t.user == "" {
		t.user = "root"
	}
	t.password = os.Getenv("MYSQL_PSWD")

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4", t.user, t.password, t.host, t.port)
	db, err := sql.Open("mysql", dsn)
	c.Assert(err, IsNil)
	t.db = db
}

func (t *testTCPReaderSuite) setUpData(c *C) {
	// drop database first.
	query := fmt.Sprintf("DROP DATABASE `%s`", dbName)
	_, err := t.db.Exec(query)

	// delete previous binlog files/events.
	query = "RESET MASTER"
	_, err = t.db.Exec(query)
	c.Assert(err, IsNil)

	// execute some SQL statements to generate binlog events.
	query = fmt.Sprintf("CREATE DATABASE `%s`", dbName)
	_, err = t.db.Exec(query)
	c.Assert(err, IsNil)

	query = fmt.Sprintf("CREATE TABLE `%s`.`%s` (c1 INT)", dbName, tableName)
	_, err = t.db.Exec(query)
	c.Assert(err, IsNil)

	// we assume `binlog_format` already set to `ROW`.
	query = fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES (%d)", dbName, tableName, columnValue)
	_, err = t.db.Exec(query)
	c.Assert(err, IsNil)
}

func (t *testTCPReaderSuite) TestSyncPos(c *C) {
	var (
		cfg = replication.BinlogSyncerConfig{
			ServerID:       serverIDs[0],
			Flavor:         flavor,
			Host:           t.host,
			Port:           uint16(t.port),
			User:           t.user,
			Password:       t.password,
			UseDecimal:     true,
			VerifyChecksum: true,
		}
		pos gmysql.Position // empty position
	)

	// the first reader
	r := NewTCPReader(cfg)
	c.Assert(r, NotNil)

	// not prepared
	e, err := r.GetEvent(context.Background(), true)
	c.Assert(err, NotNil)
	c.Assert(e, IsNil)

	// prepare
	err = r.StartSyncByPos(pos)
	c.Assert(err, IsNil)

	// verify
	t.verifyInitialEvents(c, r)

	// check status, stagePrepared
	status := r.Status()
	trStatus, ok := status.(*TCPReaderStatus)
	c.Assert(ok, IsTrue)
	c.Assert(trStatus.Stage, Equals, stagePrepared.String())
	c.Assert(trStatus.Connection, Greater, uint32(0))
	trStatusStr := trStatus.String()
	c.Assert(strings.Contains(trStatusStr, stagePrepared.String()), IsTrue)

	// re-prepare is invalid
	err = r.StartSyncByPos(pos)
	c.Assert(err, NotNil)

	// close the reader
	err = r.Close()
	c.Assert(err, IsNil)

	// already closed
	err = r.Close()
	c.Assert(err, NotNil)

	// invalid startup position
	pos = gmysql.Position{
		Name: "mysql-bin.888888",
		Pos:  666666,
	}

	// create a new one.
	r = NewTCPReader(cfg)
	err = r.StartSyncByPos(pos)
	c.Assert(err, IsNil)

	_, err = r.GetEvent(context.Background(), true)
	// ERROR 1236 (HY000): Could not find first log file name in binary log index file
	// close connection automatically.
	c.Assert(err, NotNil)

	// get current position for master
	pos, _, err = utils.GetMasterStatus(t.db, flavor)

	// execute another DML again
	query := fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES (%d)", dbName, tableName, columnValue+1)
	_, err = t.db.Exec(query)
	c.Assert(err, IsNil)

	// create a new one again
	r = NewTCPReader(cfg)
	err = r.StartSyncByPos(pos)
	c.Assert(err, IsNil)

	t.verifyOneDML(c, r)

	err = r.Close()
	c.Assert(err, IsNil)
}

func (t *testTCPReaderSuite) TestSyncGTID(c *C) {
	var (
		cfg = replication.BinlogSyncerConfig{
			ServerID:       serverIDs[1],
			Flavor:         flavor,
			Host:           t.host,
			Port:           uint16(t.port),
			User:           t.user,
			Password:       t.password,
			UseDecimal:     true,
			VerifyChecksum: true,
		}
		gSet gtid.Set // nit GTID set
	)

	// the first reader
	r := NewTCPReader(cfg)
	c.Assert(r, NotNil)

	// check status, stageNew
	status := r.Status()
	trStatus, ok := status.(*TCPReaderStatus)
	c.Assert(ok, IsTrue)
	c.Assert(trStatus.Stage, Equals, stageNew.String())
	c.Assert(trStatus.Connection, Equals, uint32(0))

	// not prepared
	e, err := r.GetEvent(context.Background(), true)
	c.Assert(err, NotNil)
	c.Assert(e, IsNil)

	// nil GTID set
	err = r.StartSyncByGTID(gSet)
	c.Assert(err, NotNil)

	// empty GTID set
	gSet, err = gtid.ParserGTID(flavor, "")
	c.Assert(err, IsNil)

	// prepare
	err = r.StartSyncByGTID(gSet)
	c.Assert(err, IsNil)

	// verify
	t.verifyInitialEvents(c, r)

	// re-prepare is invalid
	err = r.StartSyncByGTID(gSet)
	c.Assert(err, NotNil)

	// close the reader
	err = r.Close()
	c.Assert(err, IsNil)

	// check status, stageClosed
	status = r.Status()
	trStatus, ok = status.(*TCPReaderStatus)
	c.Assert(ok, IsTrue)
	c.Assert(trStatus.Stage, Equals, stageClosed.String())
	c.Assert(trStatus.Connection, Greater, uint32(0))

	// already closed
	err = r.Close()
	c.Assert(err, NotNil)

	// get current GTID set for master
	_, gSet, err = utils.GetMasterStatus(t.db, flavor)

	// execute another DML again
	query := fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES (%d)", dbName, tableName, columnValue+2)
	_, err = t.db.Exec(query)
	c.Assert(err, IsNil)

	// create a new one
	r = NewTCPReader(cfg)
	err = r.StartSyncByGTID(gSet)
	c.Assert(err, IsNil)

	t.verifyOneDML(c, r)

	err = r.Close()
	c.Assert(err, IsNil)
}

func (t *testTCPReaderSuite) verifyInitialEvents(c *C, reader Reader) {
	// if timeout, we think the test case failed.
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	parser2, err := utils.GetParser(t.db, false)
	c.Assert(err, IsNil)

forLoop:
	for {
		e, err := reader.GetEvent(timeoutCtx, true)
		c.Assert(err, IsNil)
		switch ev := e.Event.(type) {
		case *replication.QueryEvent:
			stmt, err := parser2.ParseOneStmt(string(ev.Query), "", "")
			c.Assert(err, IsNil)
			switch ddl := stmt.(type) {
			case *ast.CreateDatabaseStmt:
				c.Assert(ddl.Name, Equals, dbName)
			case *ast.CreateTableStmt:
				c.Assert(ddl.Table.Name.O, Equals, tableName)
			}
		case *replication.RowsEvent:
			c.Assert(string(ev.Table.Schema), Equals, dbName)
			c.Assert(string(ev.Table.Table), Equals, tableName)
			c.Assert(ev.Rows, HasLen, 1)                // `INSERT INTO`
			c.Assert(ev.ColumnCount, Equals, uint64(1)) // `c1`
			break forLoop                               // if we got the DML, then we think our test case passed.
		}
	}
}

func (t *testTCPReaderSuite) verifyOneDML(c *C, reader Reader) {
	// if timeout, we think the test case failed.
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

forLoop:
	for {
		e, err := reader.GetEvent(timeoutCtx, false)
		c.Assert(err, IsNil)
		switch ev := e.Event.(type) {
		case *replication.RowsEvent:
			c.Assert(string(ev.Table.Schema), Equals, dbName)
			c.Assert(string(ev.Table.Table), Equals, tableName)
			c.Assert(ev.Rows, HasLen, 1)                // `INSERT INTO`
			c.Assert(ev.ColumnCount, Equals, uint64(1)) // `c1`
			break forLoop
		}
	}
}
