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
	"database/sql/driver"
	"fmt"
	"github.com/pingcap/dm/pkg/baseconn"
	"github.com/pingcap/dm/pkg/retry"
	"github.com/pingcap/parser"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	_ "github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/parser/ast"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	cm "github.com/pingcap/tidb-tools/pkg/column-mapping"
	"github.com/pingcap/tidb-tools/pkg/filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/siddontang/go-mysql/mysql"

	"github.com/siddontang/go-mysql/replication"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/binlog/event"
	"github.com/pingcap/dm/pkg/log"
	parserpkg "github.com/pingcap/dm/pkg/parser"
	"github.com/pingcap/dm/pkg/utils"
)

var _ = Suite(&testSyncerSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type mockBinlogEvents []mockBinlogEvent
type mockBinlogEvent struct {
	typ  int
	args []interface{}
}

const (
	DBCreate = iota
	DBDrop
	TableCreate
	TableDrop

	Write
	Update
	Delete
)

type testSyncerSuite struct {
	db              *sql.DB
	dbAddr          string
	syncer          *replication.BinlogSyncer
	streamer        *replication.BinlogStreamer
	cfg             *config.SubTaskConfig
	eventsGenerator *event.Generator
}

func (s *testSyncerSuite) SetUpSuite(c *C) {
	s.cfg = &config.SubTaskConfig{
		From:       getDBConfigFromEnv(),
		To:         getDBConfigFromEnv(),
		ServerID:   101,
		MetaSchema: "test",
		Name:       "syncer_ut",
		Mode:       config.ModeIncrement,
		Flavor:     "mysql",
	}
	s.cfg.From.Adjust()
	s.cfg.To.Adjust()

	dir := c.MkDir()
	s.cfg.RelayDir = dir

	var err error
	s.dbAddr = fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8", s.cfg.From.User, s.cfg.From.Password, s.cfg.From.Host, s.cfg.From.Port)
	s.db, err = sql.Open("mysql", s.dbAddr)
	if err != nil {
		log.L().Fatal("", zap.Error(err))
	}

	s.resetMaster()
	s.resetBinlogSyncer()
	s.resetEventsGenerator(c)

	_, err = s.db.Exec("SET GLOBAL binlog_format = 'ROW';")
	c.Assert(err, IsNil)
}

func (s *testSyncerSuite) generateEvents(binlogEvents mockBinlogEvents, c *C) []*replication.BinlogEvent {
	events := make([]*replication.BinlogEvent, 0, 1024)
	for _, e := range binlogEvents {
		switch e.typ {
		case DBCreate:
			evs, _, err := s.eventsGenerator.GenCreateDatabaseEvents(e.args[0].(string))
			c.Assert(err, IsNil)
			events = append(events, evs...)
		case DBDrop:
			evs, _, err := s.eventsGenerator.GenDropDatabaseEvents(e.args[0].(string))
			c.Assert(err, IsNil)
			events = append(events, evs...)
		case TableCreate:
			evs, _, err := s.eventsGenerator.GenCreateTableEvents(e.args[0].(string), e.args[1].(string))
			c.Assert(err, IsNil)
			events = append(events, evs...)
		case TableDrop:
			evs, _, err := s.eventsGenerator.GenDropTableEvents(e.args[0].(string), e.args[1].(string))
			c.Assert(err, IsNil)
			events = append(events, evs...)

		case Write, Update, Delete:
			dmlData := []*event.DMLData{
				{
					TableID:    e.args[0].(uint64),
					Schema:     e.args[1].(string),
					Table:      e.args[2].(string),
					ColumnType: e.args[3].([]byte),
					Rows:       e.args[4].([][]interface{}),
				},
			}
			var eventType replication.EventType
			switch e.typ {
			case Write:
				eventType = replication.WRITE_ROWS_EVENTv2
			case Update:
				eventType = replication.UPDATE_ROWS_EVENTv2
			case Delete:
				eventType = replication.DELETE_ROWS_EVENTv2
			default:
				c.Fatal(fmt.Sprintf("mock event generator don't support event type: %d", e.typ))
			}
			evs, _, err := s.eventsGenerator.GenDMLEvents(eventType, dmlData)
			c.Assert(err, IsNil)
			events = append(events, evs...)
		}

	}
	return events
}

func (s *testSyncerSuite) resetEventsGenerator(c *C) {
	previousGTIDSetStr := "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14,406a3f61-690d-11e7-87c5-6c92bf46f384:1-94321383"
	previousGTIDSet, err := gtid.ParserGTID(s.cfg.Flavor, previousGTIDSetStr)
	if err != nil {
		c.Fatal(err)
	}
	latestGTIDStr := "3ccc475b-2343-11e7-be21-6c0b84d59f30:14"
	latestGTID, err := gtid.ParserGTID(s.cfg.Flavor, latestGTIDStr)
	s.eventsGenerator, err = event.NewGenerator(s.cfg.Flavor, uint32(s.cfg.ServerID), 0, latestGTID, previousGTIDSet, 0)
	if err != nil {
		c.Fatal(err)
	}
}

func (s *testSyncerSuite) resetBinlogSyncer() {
	var err error
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
		if err != nil {
			log.L().Fatal("", zap.Error(err2))
		}
		cfg.TimestampStringLocation = timezone
	}

	if s.syncer != nil {
		s.syncer.Close()
	}

	pos, _, err := utils.GetMasterStatus(s.db, "mysql")
	if err != nil {
		log.L().Fatal("", zap.Error(err))
	}

	s.syncer = replication.NewBinlogSyncer(cfg)
	s.streamer, err = s.syncer.StartSync(pos)
	if err != nil {
		log.L().Fatal("", zap.Error(err))
	}
}

func (s *testSyncerSuite) TearDownSuite(c *C) {
	s.db.Close()
}

func (s *testSyncerSuite) mockParser(db *sql.DB, mock sqlmock.Sqlmock) (*parser.Parser, error) {
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("sql_mode", "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"))
	return utils.GetParser(db, false)
}

func (s *testSyncerSuite) resetMaster() {
	s.db.Exec("reset master")
}

func (s *testSyncerSuite) catchUpBinlog() {
	ch := make(chan interface{})
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		for {
			ev, _ := s.streamer.GetEvent(ctx)
			if ev == nil {
				return
			}
			ch <- struct{}{}
		}
	}()

	for {
		select {
		case <-ch:
			// do nothing
		case <-time.After(10 * time.Millisecond):
			cancel()
			return
		}
	}
}

func (s *testSyncerSuite) TestSelectDB(c *C) {
	s.cfg.BWList = &filter.Rules{
		DoDBs: []string{"~^b.*", "s1", "stest"},
	}

	schemas := [][]byte{[]byte("s1"), []byte("s2"), []byte("btest"), []byte("b1"), []byte("stest"), []byte("st")}
	skips := []bool{false, true, false, false, false, true}
	type Case struct {
		schema []byte
		query  []byte
		skip   bool
	}
	cases := make([]Case, 0, 2*len(schemas))
	for i, schema := range schemas {
		cases = append(cases, Case{
			schema: schema,
			query:  append([]byte("create database "), schema...),
			skip:   skips[i]})
		cases = append(cases, Case{
			schema: schema,
			query:  append([]byte("drop database "), schema...),
			skip:   skips[i]})
	}

	db, mock, err := sqlmock.New()
	p, err := s.mockParser(db, mock)
	c.Assert(err, IsNil)

	syncer := NewSyncer(s.cfg)
	err = syncer.genRouter()
	c.Assert(err, IsNil)

	header := &replication.EventHeader{
		Timestamp: uint32(time.Now().Unix()),
		ServerID:  101,
		Flags:     0x01,
	}

	for _, cs := range cases {
		e, err := event.GenQueryEvent(header, 123, 0, 0, 0, nil, cs.schema, cs.query)
		c.Assert(err, IsNil)
		c.Assert(e, NotNil)
		ev, ok := e.Event.(*replication.QueryEvent)
		c.Assert(ok, IsTrue)

		query := string(ev.Query)
		stmt, err := p.ParseOneStmt(query, "", "")
		c.Assert(err, IsNil)

		tableNames, err := parserpkg.FetchDDLTableNames(string(ev.Schema), stmt)
		c.Assert(err, IsNil)

		r, err := syncer.skipQuery(tableNames, stmt, query)
		c.Assert(err, IsNil)
		c.Assert(r, Equals, cs.skip)
	}
}

func (s *testSyncerSuite) TestSelectTable(c *C) {
	s.cfg.BWList = &filter.Rules{
		DoDBs: []string{"t2", "stest", "~^ptest*"},
		DoTables: []*filter.Table{
			{Schema: "stest", Name: "log"},
			{Schema: "stest", Name: "~^t.*"},
			{Schema: "~^ptest*", Name: "~^t.*"},
		},
	}
	s.resetEventsGenerator(c)
	events := mockBinlogEvents{
		mockBinlogEvent{typ: DBCreate, args: []interface{}{"s1"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"s1", "create table s1.log(id int)"}},
		mockBinlogEvent{typ: DBDrop, args: []interface{}{"s1"}},

		mockBinlogEvent{typ: TableCreate, args: []interface{}{"mysql", "create table mysql.test(id int)"}},
		mockBinlogEvent{typ: TableDrop, args: []interface{}{"mysql", "test"}},
		mockBinlogEvent{typ: DBCreate, args: []interface{}{"stest"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"stest", "create table stest.log(id int)"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"stest", "create table stest.t(id int)"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"stest", "create table stest.log2(id int)"}},
		mockBinlogEvent{typ: Write, args: []interface{}{uint64(8), "stest", "t", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(10)}}}},
		mockBinlogEvent{typ: Write, args: []interface{}{uint64(9), "stest", "log", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(10)}}}},
		mockBinlogEvent{typ: Write, args: []interface{}{uint64(10), "stest", "log2", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(10)}}}},
		mockBinlogEvent{typ: TableDrop, args: []interface{}{"stest", "log"}},
		mockBinlogEvent{typ: TableDrop, args: []interface{}{"stest", "t"}},
		mockBinlogEvent{typ: TableDrop, args: []interface{}{"stest", "log2"}},
		mockBinlogEvent{typ: DBDrop, args: []interface{}{"stest"}},

		mockBinlogEvent{typ: DBCreate, args: []interface{}{"t2"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"t2", "create table t2.log(id int)"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"t2", "create table t2.log1(id int)"}},
		mockBinlogEvent{typ: TableDrop, args: []interface{}{"t2", "log"}},
		mockBinlogEvent{typ: DBDrop, args: []interface{}{"t2"}},

		mockBinlogEvent{typ: DBCreate, args: []interface{}{"ptest1"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"ptest1", "create table ptest1.t1(id int)"}},
		mockBinlogEvent{typ: DBDrop, args: []interface{}{"ptest1"}},
	}

	allEvents := s.generateEvents(events, c)

	res := [][]bool{
		{true},
		{true},
		{true},

		{true},
		{true},
		{false},
		{false},
		{false},
		{true},
		{false},
		{false},
		{true},
		{false},
		{false},
		{true},
		{false},

		{false},
		{true},
		{true},
		{true},
		{false},

		{false},
		{false},
		{false},
	}

	db, mock, err := sqlmock.New()
	p, err := s.mockParser(db, mock)
	c.Assert(err, IsNil)

	syncer := NewSyncer(s.cfg)
	syncer.genRouter()
	i := 0
	for _, e := range allEvents {
		switch ev := e.Event.(type) {
		case *replication.QueryEvent:
			query := string(ev.Query)

			result, err := syncer.parseDDLSQL(query, p, string(ev.Schema))
			c.Assert(err, IsNil)
			if !result.isDDL {
				continue // BEGIN event
			}
			querys, _, err := syncer.resolveDDLSQL(p, result.stmt, string(ev.Schema))
			c.Assert(err, IsNil)
			if len(querys) == 0 {
				continue
			}

			for j, sql := range querys {
				stmt, err := p.ParseOneStmt(sql, "", "")
				c.Assert(err, IsNil)

				tableNames, err := parserpkg.FetchDDLTableNames(string(ev.Schema), stmt)
				c.Assert(err, IsNil)
				r, err := syncer.skipQuery(tableNames, stmt, sql)
				c.Assert(err, IsNil)
				c.Assert(r, Equals, res[i][j])
			}
		case *replication.RowsEvent:
			r, err := syncer.skipDMLEvent(string(ev.Table.Schema), string(ev.Table.Table), e.Header.EventType)
			c.Assert(err, IsNil)
			c.Assert(r, Equals, res[i][0])
		default:
			continue
		}
		i++
	}
}

func (s *testSyncerSuite) TestIgnoreDB(c *C) {
	s.cfg.BWList = &filter.Rules{
		IgnoreDBs: []string{"~^b.*", "s1", "stest"},
	}

	s.resetEventsGenerator(c)
	events := mockBinlogEvents{
		mockBinlogEvent{typ: DBCreate, args: []interface{}{"s1"}},
		mockBinlogEvent{typ: DBDrop, args: []interface{}{"s1"}},
		mockBinlogEvent{typ: DBCreate, args: []interface{}{"s2"}},
		mockBinlogEvent{typ: DBDrop, args: []interface{}{"s2"}},
		mockBinlogEvent{typ: DBCreate, args: []interface{}{"btest"}},
		mockBinlogEvent{typ: DBDrop, args: []interface{}{"btest"}},
		mockBinlogEvent{typ: DBCreate, args: []interface{}{"b1"}},
		mockBinlogEvent{typ: DBDrop, args: []interface{}{"b1"}},
		mockBinlogEvent{typ: DBCreate, args: []interface{}{"stest"}},
		mockBinlogEvent{typ: DBDrop, args: []interface{}{"stest"}},
		mockBinlogEvent{typ: DBCreate, args: []interface{}{"st"}},
		mockBinlogEvent{typ: DBDrop, args: []interface{}{"st"}},
	}

	allEvents := s.generateEvents(events, c)

	res := []bool{true, true, false, false, true, true, true, true, true, true, false, false}

	db, mock, err := sqlmock.New()
	p, err := s.mockParser(db, mock)
	c.Assert(err, IsNil)

	syncer := NewSyncer(s.cfg)
	syncer.genRouter()
	i := 0
	for _, e := range allEvents {
		ev, ok := e.Event.(*replication.QueryEvent)
		if !ok {
			continue
		}
		sql := string(ev.Query)
		stmt, err := p.ParseOneStmt(sql, "", "")
		c.Assert(err, IsNil)

		tableNames, err := parserpkg.FetchDDLTableNames(sql, stmt)
		c.Assert(err, IsNil)
		r, err := syncer.skipQuery(tableNames, stmt, sql)
		c.Assert(err, IsNil)
		c.Assert(r, Equals, res[i])
		i++
	}
}

func (s *testSyncerSuite) TestIgnoreTable(c *C) {
	s.cfg.BWList = &filter.Rules{
		IgnoreDBs: []string{"t2"},
		IgnoreTables: []*filter.Table{
			{Schema: "stest", Name: "log"},
			{Schema: "stest", Name: "~^t.*"},
		},
	}

	s.resetEventsGenerator(c)
	events := mockBinlogEvents{
		mockBinlogEvent{typ: DBCreate, args: []interface{}{"s1"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"s1", "create table s1.log(id int)"}},
		mockBinlogEvent{typ: DBDrop, args: []interface{}{"s1"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"mysql", "create table mysql.test(id int)"}},
		mockBinlogEvent{typ: TableDrop, args: []interface{}{"mysql", "test"}},

		mockBinlogEvent{typ: DBCreate, args: []interface{}{"stest"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"stest", "create table stest.log(id int)"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"stest", "create table stest.t(id int)"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"stest", "create table stest.log2(id int)"}},

		mockBinlogEvent{typ: Write, args: []interface{}{uint64(8), "stest", "t", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(10)}}}},
		mockBinlogEvent{typ: Write, args: []interface{}{uint64(9), "stest", "log", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(10)}}}},
		mockBinlogEvent{typ: Write, args: []interface{}{uint64(10), "stest", "log2", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(10)}}}},
		// TODO event generator support generate an event with multiple tables DDL
		mockBinlogEvent{typ: TableDrop, args: []interface{}{"stest", "log"}},
		mockBinlogEvent{typ: TableDrop, args: []interface{}{"stest", "t"}},
		mockBinlogEvent{typ: TableDrop, args: []interface{}{"stest", "log2"}},
		mockBinlogEvent{typ: DBDrop, args: []interface{}{"stest"}},

		mockBinlogEvent{typ: DBCreate, args: []interface{}{"t2"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"t2", "create table t2.log(id int)"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"t2", "create table t2.log1(id int)"}},
		mockBinlogEvent{typ: TableDrop, args: []interface{}{"t2", "log"}},
		mockBinlogEvent{typ: DBDrop, args: []interface{}{"t2"}},
	}
	allEvents := s.generateEvents(events, c)

	res := [][]bool{
		{false},
		{false},
		{false},

		{true},
		{true},
		{false},
		{true},
		{true},
		{false},
		{true},
		{true},
		{false},
		{true},
		{true},
		{false},
		{false},

		{true},
		{true},
		{true},
		{true},
		{true},
	}

	db, mock, err := sqlmock.New()
	p, err := s.mockParser(db, mock)
	c.Assert(err, IsNil)

	syncer := NewSyncer(s.cfg)
	syncer.genRouter()

	i := 0
	for _, e := range allEvents {
		switch ev := e.Event.(type) {
		case *replication.QueryEvent:
			query := string(ev.Query)
			result, err := syncer.parseDDLSQL(query, p, string(ev.Schema))
			c.Assert(err, IsNil)
			if !result.isDDL {
				continue // BEGIN event
			}

			querys, _, err := syncer.resolveDDLSQL(p, result.stmt, string(ev.Schema))
			c.Assert(err, IsNil)
			if len(querys) == 0 {
				continue
			}

			for j, sql := range querys {
				stmt, err := p.ParseOneStmt(sql, "", "")
				c.Assert(err, IsNil)

				tableNames, err := parserpkg.FetchDDLTableNames(string(ev.Schema), stmt)
				c.Assert(err, IsNil)
				r, err := syncer.skipQuery(tableNames, stmt, sql)
				c.Assert(err, IsNil)
				c.Assert(r, Equals, res[i][j])
			}
		case *replication.RowsEvent:
			r, err := syncer.skipDMLEvent(string(ev.Table.Schema), string(ev.Table.Table), e.Header.EventType)
			c.Assert(err, IsNil)
			c.Assert(r, Equals, res[i][0])

		default:
			continue
		}
		i++
	}
}

func (s *testSyncerSuite) TestSkipDML(c *C) {
	s.cfg.FilterRules = []*bf.BinlogEventRule{
		{
			SchemaPattern: "*",
			TablePattern:  "",
			Events:        []bf.EventType{bf.UpdateEvent},
			Action:        bf.Ignore,
		}, {
			SchemaPattern: "foo",
			TablePattern:  "",
			Events:        []bf.EventType{bf.DeleteEvent},
			Action:        bf.Ignore,
		}, {
			SchemaPattern: "foo1",
			TablePattern:  "bar1",
			Events:        []bf.EventType{bf.DeleteEvent},
			Action:        bf.Ignore,
		}, {
			SchemaPattern: "foo1",
			TablePattern:  "bar2",
			Events:        []bf.EventType{bf.EventType(strings.ToUpper(string(bf.DeleteEvent)))},
			Action:        bf.Ignore,
		},
	}
	s.cfg.BWList = nil

	s.resetEventsGenerator(c)

	type SQLChecker struct {
		events  []*replication.BinlogEvent
		isDML   bool
		skipped bool
	}

	sqls := make([]SQLChecker, 0, 16)

	evs := s.generateEvents([]mockBinlogEvent{{DBCreate, []interface{}{"foo"}}}, c)
	sqls = append(sqls, SQLChecker{events: evs, isDML: false, skipped: false})

	evs = s.generateEvents([]mockBinlogEvent{{TableCreate, []interface{}{"foo", "create table foo.bar(id int)"}}}, c)
	sqls = append(sqls, SQLChecker{events: evs, isDML: false, skipped: false})

	evs = s.generateEvents([]mockBinlogEvent{{Write, []interface{}{uint64(8), "foo", "bar", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(1)}}}}}, c)
	sqls = append(sqls, SQLChecker{events: evs, isDML: true, skipped: false})

	evs = s.generateEvents([]mockBinlogEvent{{Update, []interface{}{uint64(8), "foo", "bar", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(2)}, {int32(1)}}}}}, c)
	sqls = append(sqls, SQLChecker{events: evs, isDML: true, skipped: true})

	evs = s.generateEvents([]mockBinlogEvent{{Delete, []interface{}{uint64(8), "foo", "bar", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(2)}}}}}, c)
	sqls = append(sqls, SQLChecker{events: evs, isDML: true, skipped: true})

	evs = s.generateEvents([]mockBinlogEvent{{DBDrop, []interface{}{"foo1"}}}, c)
	sqls = append(sqls, SQLChecker{events: evs, isDML: false, skipped: false})

	evs = s.generateEvents([]mockBinlogEvent{{DBCreate, []interface{}{"foo1"}}}, c)
	sqls = append(sqls, SQLChecker{events: evs, isDML: false, skipped: false})

	evs = s.generateEvents([]mockBinlogEvent{{TableCreate, []interface{}{"foo1", "create table foo1.bar1(id int)"}}}, c)
	sqls = append(sqls, SQLChecker{events: evs, isDML: false, skipped: false})

	evs = s.generateEvents([]mockBinlogEvent{{Write, []interface{}{uint64(9), "foo1", "bar1", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(1)}}}}}, c)
	sqls = append(sqls, SQLChecker{events: evs, isDML: true, skipped: false})

	evs = s.generateEvents([]mockBinlogEvent{{Update, []interface{}{uint64(9), "foo1", "bar1", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(2)}, {int32(1)}}}}}, c)
	sqls = append(sqls, SQLChecker{events: evs, isDML: true, skipped: true})

	evs = s.generateEvents([]mockBinlogEvent{{Delete, []interface{}{uint64(9), "foo1", "bar1", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(2)}}}}}, c)
	sqls = append(sqls, SQLChecker{events: evs, isDML: true, skipped: true})

	evs = s.generateEvents([]mockBinlogEvent{{TableCreate, []interface{}{"foo1", "create table foo1.bar2(id int)"}}}, c)
	sqls = append(sqls, SQLChecker{events: evs, isDML: false, skipped: false})

	evs = s.generateEvents([]mockBinlogEvent{{Write, []interface{}{uint64(10), "foo1", "bar2", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(1)}}}}}, c)
	sqls = append(sqls, SQLChecker{events: evs, isDML: true, skipped: false})

	evs = s.generateEvents([]mockBinlogEvent{{Update, []interface{}{uint64(10), "foo1", "bar2", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(2)}, {int32(1)}}}}}, c)
	sqls = append(sqls, SQLChecker{events: evs, isDML: true, skipped: true})

	evs = s.generateEvents([]mockBinlogEvent{{Delete, []interface{}{uint64(10), "foo1", "bar2", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(2)}}}}}, c)
	sqls = append(sqls, SQLChecker{events: evs, isDML: true, skipped: true})

	db, mock, err := sqlmock.New()
	p, err := s.mockParser(db, mock)
	c.Assert(err, IsNil)

	syncer := NewSyncer(s.cfg)
	syncer.genRouter()

	syncer.binlogFilter, err = bf.NewBinlogEvent(false, s.cfg.FilterRules)
	c.Assert(err, IsNil)

	for _, sql := range sqls {
		events := sql.events
		for _, e := range events {
			switch ev := e.Event.(type) {
			case *replication.QueryEvent:
				stmt, err := p.ParseOneStmt(string(ev.Query), "", "")
				c.Assert(err, IsNil)
				_, isDDL := stmt.(ast.DDLNode)
				if !isDDL {
					continue
				}

			case *replication.RowsEvent:
				r, err := syncer.skipDMLEvent(string(ev.Table.Schema), string(ev.Table.Table), e.Header.EventType)
				c.Assert(err, IsNil)
				c.Assert(r, Equals, sql.skipped)
			default:
				continue
			}
		}
	}
}

func (s *testSyncerSuite) TestColumnMapping(c *C) {
	rules := []*cm.Rule{
		{
			PatternSchema: "stest*",
			PatternTable:  "log*",
			TargetColumn:  "id",
			Expression:    cm.AddPrefix,
			Arguments:     []string{"test:"},
		},
		{
			PatternSchema: "stest*",
			PatternTable:  "t*",
			TargetColumn:  "id",
			Expression:    cm.PartitionID,
			Arguments:     []string{"1", "stest_", "t_"},
		},
	}

	s.resetEventsGenerator(c)

	//create baseConn and tables
	events := mockBinlogEvents{
		mockBinlogEvent{typ: DBCreate, args: []interface{}{"stest_3"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"stest_3", "create table stest_3.log(id varchar(45))"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"stest_3", "create table stest_3.t_2(name varchar(45), id bigint)"}},
		mockBinlogEvent{typ: TableCreate, args: []interface{}{"stest_3", "create table stest_3.a(id int)"}},
	}

	createEvents := s.generateEvents(events, c)

	// dmls
	type dml struct {
		events []*replication.BinlogEvent
		column []string
		data   []interface{}
	}

	dmls := make([]dml, 0, 3)

	evs := s.generateEvents([]mockBinlogEvent{{typ: Write, args: []interface{}{uint64(8), "stest_3", "t_2", []byte{mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_LONG}, [][]interface{}{{"ian", int32(10)}}}}}, c)
	dmls = append(dmls, dml{events: evs, column: []string{"name", "id"}, data: []interface{}{"ian", int64(1<<59 | 3<<52 | 2<<44 | 10)}})

	evs = s.generateEvents([]mockBinlogEvent{{typ: Write, args: []interface{}{uint64(9), "stest_3", "log", []byte{mysql.MYSQL_TYPE_STRING}, [][]interface{}{{"10"}}}}}, c)
	dmls = append(dmls, dml{events: evs, column: []string{"id"}, data: []interface{}{"test:10"}})

	evs = s.generateEvents([]mockBinlogEvent{{typ: Write, args: []interface{}{uint64(10), "stest_3", "a", []byte{mysql.MYSQL_TYPE_LONG}, [][]interface{}{{int32(10)}}}}}, c)
	dmls = append(dmls, dml{events: evs, column: []string{"id"}, data: []interface{}{int32(10)}})

	dmlEvents := make([]*replication.BinlogEvent, 0, 15)
	for _, dml := range dmls {
		dmlEvents = append(dmlEvents, dml.events...)
	}

	// drop tables and baseConn
	events = mockBinlogEvents{
		// TODO event generator support generate an event with multiple tables DDL
		mockBinlogEvent{typ: TableDrop, args: []interface{}{"stest_3", "log"}},
		mockBinlogEvent{typ: TableDrop, args: []interface{}{"stest_3", "t_2"}},
		mockBinlogEvent{typ: TableDrop, args: []interface{}{"stest_3", "a"}},
		mockBinlogEvent{typ: DBDrop, args: []interface{}{"stest_3"}},
	}
	dropEvents := s.generateEvents(events, c)

	db, mock, err := sqlmock.New()
	p, err := s.mockParser(db, mock)
	c.Assert(err, IsNil)

	mapping, err := cm.NewMapping(false, rules)
	c.Assert(err, IsNil)

	allEvents := append(createEvents, dmlEvents...)
	allEvents = append(allEvents, dropEvents...)
	dmlIndex := 0
	for _, e := range allEvents {
		switch ev := e.Event.(type) {
		case *replication.QueryEvent:
			stmt, err := p.ParseOneStmt(string(ev.Query), "", "")
			c.Assert(err, IsNil)
			_, isDDL := stmt.(ast.DDLNode)
			if !isDDL {
				continue
			}
		case *replication.RowsEvent:
			r, _, err := mapping.HandleRowValue(string(ev.Table.Schema), string(ev.Table.Table), dmls[dmlIndex].column, ev.Rows[0])
			c.Assert(err, IsNil)
			c.Assert(r, DeepEquals, dmls[dmlIndex].data)
			dmlIndex++
		default:
			continue
		}
	}
}

func (s *testSyncerSuite) TestTimezone(c *C) {
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

	for _, sql := range createSQLs {
		s.db.Exec(sql)
	}

	for _, testCase := range testCases {
		s.cfg.Timezone = testCase.timezone
		syncer := NewSyncer(s.cfg)
		syncer.genRouter()
		s.resetBinlogSyncer()

		// we should not use `sql.DB.Exec` to do query which depends on session variables
		// because `sql.DB.Exec` will choose a underlying BaseConn for every query from the connection pool
		// and different BaseConn using different session
		// ref: `sql.DB.BaseConn`
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

	for _, sql := range dropSQLs {
		s.db.Exec(sql)
	}
	s.catchUpBinlog()
}

func (s *testSyncerSuite) TestGeneratedColumn(c *C) {
	defer s.db.Exec("drop database if exists gctest_1")

	s.cfg.BWList = &filter.Rules{
		DoDBs: []string{"~^gctest_.*"},
	}

	createSQLs := []string{
		"create database if not exists gctest_1 DEFAULT CHARSET=utf8mb4",
		"create table if not exists gctest_1.t_1(id int, age int, cfg varchar(40), cfg_json json as (cfg) virtual)",
		"create table if not exists gctest_1.t_2(id int primary key, age int, cfg varchar(40), cfg_json json as (cfg) virtual)",
		"create table if not exists gctest_1.t_3(id int, cfg varchar(40), gen_id int as (cfg->\"$.id\"), unique key gen_id_unique(`gen_id`))",
	}

	// if table has json typed generated column but doesn't have primary key or unique key,
	// update/delete operation will not be replicated successfully because json field can't
	// compared with raw value in where condition. In unit test we only check generated SQL
	// and don't check the data replication to downstream.
	testCases := []struct {
		sqls     []string
		expected []string
		args     [][]interface{}
	}{
		{
			[]string{
				"insert into gctest_1.t_1(id, age, cfg) values (1, 18, '{}')",
				"insert into gctest_1.t_1(id, age, cfg) values (2, 19, '{\"key\": \"value\"}')",
				"insert into gctest_1.t_1(id, age, cfg) values (3, 17, NULL)",
				"insert into gctest_1.t_2(id, age, cfg) values (1, 18, '{}')",
				"insert into gctest_1.t_2(id, age, cfg) values (2, 19, '{\"key\": \"value\", \"int\": 123}')",
				"insert into gctest_1.t_2(id, age, cfg) values (3, 17, NULL)",
				"insert into gctest_1.t_3(id, cfg) values (1, '{\"id\": 1}')",
				"insert into gctest_1.t_3(id, cfg) values (2, '{\"id\": 2}')",
				"insert into gctest_1.t_3(id, cfg) values (3, '{\"id\": 3}')",
			},
			[]string{
				"INSERT INTO `gctest_1`.`t_1` (`id`,`age`,`cfg`) VALUES (?,?,?);",
				"INSERT INTO `gctest_1`.`t_1` (`id`,`age`,`cfg`) VALUES (?,?,?);",
				"INSERT INTO `gctest_1`.`t_1` (`id`,`age`,`cfg`) VALUES (?,?,?);",
				"INSERT INTO `gctest_1`.`t_2` (`id`,`age`,`cfg`) VALUES (?,?,?);",
				"INSERT INTO `gctest_1`.`t_2` (`id`,`age`,`cfg`) VALUES (?,?,?);",
				"INSERT INTO `gctest_1`.`t_2` (`id`,`age`,`cfg`) VALUES (?,?,?);",
				"INSERT INTO `gctest_1`.`t_3` (`id`,`cfg`) VALUES (?,?);",
				"INSERT INTO `gctest_1`.`t_3` (`id`,`cfg`) VALUES (?,?);",
				"INSERT INTO `gctest_1`.`t_3` (`id`,`cfg`) VALUES (?,?);",
			},
			[][]interface{}{
				{int32(1), int32(18), "{}"},
				{int32(2), int32(19), "{\"key\": \"value\"}"},
				{int32(3), int32(17), nil},
				{int32(1), int32(18), "{}"},
				{int32(2), int32(19), "{\"key\": \"value\", \"int\": 123}"},
				{int32(3), int32(17), nil},
				{int32(1), "{\"id\": 1}"},
				{int32(2), "{\"id\": 2}"},
				{int32(3), "{\"id\": 3}"},
			},
		},
		{
			[]string{
				"update gctest_1.t_1 set cfg = '{\"a\": 12}', age = 21 where id = 1",
				"update gctest_1.t_1 set cfg = '{}' where id = 2 and age = 19",
				"update gctest_1.t_1 set age = 20 where cfg is NULL",
				"update gctest_1.t_2 set cfg = '{\"a\": 12}', age = 21 where id = 1",
				"update gctest_1.t_2 set cfg = '{}' where id = 2 and age = 19",
				"update gctest_1.t_2 set age = 20 where cfg is NULL",
				"update gctest_1.t_3 set cfg = '{\"id\": 11}' where id = 1",
				"update gctest_1.t_3 set cfg = '{\"id\": 12, \"old_id\": 2}' where gen_id = 2",
			},
			[]string{
				"UPDATE `gctest_1`.`t_1` SET `id` = ?, `age` = ?, `cfg` = ? WHERE `id` = ? AND `age` = ? AND `cfg` = ? AND `cfg_json` = ? LIMIT 1;",
				"UPDATE `gctest_1`.`t_1` SET `id` = ?, `age` = ?, `cfg` = ? WHERE `id` = ? AND `age` = ? AND `cfg` = ? AND `cfg_json` = ? LIMIT 1;",
				"UPDATE `gctest_1`.`t_1` SET `id` = ?, `age` = ?, `cfg` = ? WHERE `id` = ? AND `age` = ? AND `cfg` IS ? AND `cfg_json` IS ? LIMIT 1;",
				"UPDATE `gctest_1`.`t_2` SET `id` = ?, `age` = ?, `cfg` = ? WHERE `id` = ? LIMIT 1;",
				"UPDATE `gctest_1`.`t_2` SET `id` = ?, `age` = ?, `cfg` = ? WHERE `id` = ? LIMIT 1;",
				"UPDATE `gctest_1`.`t_2` SET `id` = ?, `age` = ?, `cfg` = ? WHERE `id` = ? LIMIT 1;",
				"UPDATE `gctest_1`.`t_3` SET `id` = ?, `cfg` = ? WHERE `gen_id` = ? LIMIT 1;",
				"UPDATE `gctest_1`.`t_3` SET `id` = ?, `cfg` = ? WHERE `gen_id` = ? LIMIT 1;",
			},
			[][]interface{}{
				{int32(1), int32(21), "{\"a\": 12}", int32(1), int32(18), "{}", []uint8("{}")},
				{int32(2), int32(19), "{}", int32(2), int32(19), "{\"key\": \"value\"}", []uint8("{\"key\":\"value\"}")},
				{int32(3), int32(20), nil, int32(3), int32(17), nil, nil},
				{int32(1), int32(21), "{\"a\": 12}", int32(1)},
				{int32(2), int32(19), "{}", int32(2)},
				{int32(3), int32(20), nil, int32(3)},
				{int32(1), "{\"id\": 11}", int32(1)},
				{int32(2), "{\"id\": 12, \"old_id\": 2}", int32(2)},
			},
		},
		{
			[]string{
				"delete from gctest_1.t_1 where id = 1",
				"delete from gctest_1.t_1 where id = 2 and age = 19",
				"delete from gctest_1.t_1 where cfg is NULL",
				"delete from gctest_1.t_2 where id = 1",
				"delete from gctest_1.t_2 where id = 2 and age = 19",
				"delete from gctest_1.t_2 where cfg is NULL",
				"delete from gctest_1.t_3 where id = 1",
				"delete from gctest_1.t_3 where gen_id = 12",
			},
			[]string{
				"DELETE FROM `gctest_1`.`t_1` WHERE `id` = ? AND `age` = ? AND `cfg` = ? AND `cfg_json` = ? LIMIT 1;",
				"DELETE FROM `gctest_1`.`t_1` WHERE `id` = ? AND `age` = ? AND `cfg` = ? AND `cfg_json` = ? LIMIT 1;",
				"DELETE FROM `gctest_1`.`t_1` WHERE `id` = ? AND `age` = ? AND `cfg` IS ? AND `cfg_json` IS ? LIMIT 1;",
				"DELETE FROM `gctest_1`.`t_2` WHERE `id` = ? LIMIT 1;",
				"DELETE FROM `gctest_1`.`t_2` WHERE `id` = ? LIMIT 1;",
				"DELETE FROM `gctest_1`.`t_2` WHERE `id` = ? LIMIT 1;",
				"DELETE FROM `gctest_1`.`t_3` WHERE `gen_id` = ? LIMIT 1;",
				"DELETE FROM `gctest_1`.`t_3` WHERE `gen_id` = ? LIMIT 1;",
			},
			[][]interface{}{
				{int32(1), int32(21), "{\"a\": 12}", []uint8("{\"a\":12}")},
				{int32(2), int32(19), "{}", []uint8("{}")},
				{int32(3), int32(20), nil, nil},
				{int32(1)},
				{int32(2)},
				{int32(3)},
				{int32(11)},
				{int32(12)},
			},
		},
	}

	dropSQLs := []string{
		"drop table gctest_1.t_1",
		"drop table gctest_1.t_2",
		"drop table gctest_1.t_3",
		"drop database gctest_1",
	}

	for _, sql := range createSQLs {
		_, err := s.db.Exec(sql)
		c.Assert(err, IsNil)
	}

	syncer := NewSyncer(s.cfg)
	// use upstream baseConn as mock downstream
	syncer.toDBs = []*Conn{{baseConn: &baseconn.BaseConn{s.db, s.dbAddr, &retry.FiniteRetryStrategy{}}}}

	for _, testCase := range testCases {
		for _, sql := range testCase.sqls {
			_, err := s.db.Exec(sql)
			c.Assert(err, IsNil, Commentf("sql: %s", sql))
		}
		idx := 0
		for {
			if idx >= len(testCase.sqls) {
				break
			}
			e, err := s.streamer.GetEvent(context.Background())
			c.Assert(err, IsNil)
			switch ev := e.Event.(type) {
			case *replication.RowsEvent:
				table, _, err := syncer.getTable(string(ev.Table.Schema), string(ev.Table.Table))
				c.Assert(err, IsNil)
				var (
					sqls []string
					args [][]interface{}
				)

				prunedColumns, prunedRows, err := pruneGeneratedColumnDML(table.columns, ev.Rows, table.schema, table.name, syncer.genColsCache)
				c.Assert(err, IsNil)
				param := &genDMLParam{
					schema:               table.schema,
					table:                table.name,
					data:                 prunedRows,
					originalData:         ev.Rows,
					columns:              prunedColumns,
					originalColumns:      table.columns,
					originalIndexColumns: table.indexColumns,
				}
				switch e.Header.EventType {
				case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
					sqls, _, args, err = genInsertSQLs(param)
					c.Assert(err, IsNil)
					c.Assert(sqls[0], Equals, testCase.expected[idx])
					c.Assert(args[0], DeepEquals, testCase.args[idx])
				case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
					// test with sql_mode = false only
					sqls, _, args, err = genUpdateSQLs(param)
					c.Assert(err, IsNil)
					c.Assert(sqls[0], Equals, testCase.expected[idx])
					c.Assert(args[0], DeepEquals, testCase.args[idx])
				case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
					sqls, _, args, err = genDeleteSQLs(param)
					c.Assert(err, IsNil)
					c.Assert(sqls[0], Equals, testCase.expected[idx])
					c.Assert(args[0], DeepEquals, testCase.args[idx])
				}
				idx++
			default:
				continue
			}
		}
	}

	for _, sql := range dropSQLs {
		s.db.Exec(sql)
	}
	s.catchUpBinlog()
}

func (s *testSyncerSuite) TestcheckpointID(c *C) {
	syncer := NewSyncer(s.cfg)
	checkpointID := syncer.checkpointID()
	c.Assert(checkpointID, Equals, "101")
}

func (s *testSyncerSuite) TestExecErrors(c *C) {
	syncer := NewSyncer(s.cfg)
	syncer.appendExecErrors(new(ExecErrorContext))
	c.Assert(syncer.execErrors.errors, HasLen, 1)

	syncer.resetExecErrors()
	c.Assert(syncer.execErrors.errors, HasLen, 0)
}

func (s *testSyncerSuite) TestCasuality(c *C) {
	var wg sync.WaitGroup
	s.cfg.WorkerCount = 1
	syncer := NewSyncer(s.cfg)
	syncer.jobs = []chan *job{make(chan *job, 1)}

	wg.Add(1)
	go func() {
		defer wg.Done()
		job := <-syncer.jobs[0]
		c.Assert(job.tp, Equals, flush)
		syncer.jobWg.Done()
	}()

	key, err := syncer.resolveCasuality([]string{"a"})
	c.Assert(err, IsNil)
	c.Assert(key, Equals, "a")

	key, err = syncer.resolveCasuality([]string{"b"})
	c.Assert(err, IsNil)
	c.Assert(key, Equals, "b")

	// will detect casuality and add a flush job
	key, err = syncer.resolveCasuality([]string{"a", "b"})
	c.Assert(err, IsNil)
	c.Assert(key, Equals, "a")

	wg.Wait()
}

func (s *testSyncerSuite) TestSharding(c *C) {

	createSQLs := []string{
		"CREATE DATABASE IF NOT EXISTS `stest_1` CHARACTER SET = utf8mb4",
		"CREATE TABLE IF NOT EXISTS `stest_1`.`st_1` (id INT, age INT)",
		"CREATE TABLE IF NOT EXISTS `stest_1`.`st_2` (id INT, age INT)",
	}

	testCases := []struct {
		testSQLs   []string
		expectSQLS []struct {
			sql  string
			args []driver.Value
		}
	}{
		// case 1:
		// upstream binlog events:
		// insert t1 -> insert t2 -> alter t1 -> insert t2 -> alter t2 -> insert t1(new schema) -> insert t2(new schema)
		// downstream expected events:
		// insert t(from t1) -> insert t(from t2) -> insert t(from t2) -> alter t(from t2 same as t1) -> insert t1(new schema) -> insert t2(new schema)
		{
			[]string{
				"INSERT INTO `stest_1`.`st_1`(id, age) VALUES (1, 1)",
				"INSERT INTO `stest_1`.`st_2`(id, age) VALUES (2, 2)",
				"ALTER TABLE `stest_1`.`st_1` ADD COLUMN NAME VARCHAR(30)",
				"INSERT INTO `stest_1`.`st_2`(id, age) VALUES (4, 4)",
				"ALTER TABLE `stest_1`.`st_2` ADD COLUMN NAME VARCHAR(30)",
				"INSERT INTO `stest_1`.`st_1`(id, age, name) VALUES (3, 3, 'test')",
				"INSERT INTO `stest_1`.`st_2`(id, age, name) VALUES (6, 6, 'test')",
			},
			[]struct {
				sql  string
				args []driver.Value
			}{
				{
					"REPLACE INTO",
					[]driver.Value{1, 1},
				},
				{
					"REPLACE INTO",
					[]driver.Value{2, 2},
				},
				{
					"REPLACE INTO",
					[]driver.Value{4, 4},
				},
				{
					"ALTER TABLE",
					[]driver.Value{},
				},
				{
					"REPLACE INTO",
					[]driver.Value{3, 3, "test"},
				},
				{
					"REPLACE INTO",
					[]driver.Value{6, 6, "test"},
				},
			},
		},
		// case 2:
		// upstream binlog events:
		// insert t1 -> insert t2 -> alter t1 -> insert t1(new schema) -> insert t2 -> alter t2 -> insert t1(new schema) -> insert t2(new schema)
		// downstream expected events:
		// insert t(from t1) -> insert t(from t2) -> insert t(from t2) -> alter t(from t2 same as t1) -> insert t1(new schema) -> insert t1(new schema) -> insert t2(new schema)
		{
			[]string{
				"INSERT INTO `stest_1`.`st_1`(id, age) VALUES (1, 1)",
				"INSERT INTO `stest_1`.`st_2`(id, age) VALUES (2, 2)",
				"ALTER TABLE `stest_1`.`st_1` ADD COLUMN NAME VARCHAR(30)",
				"INSERT INTO `stest_1`.`st_1`(id, age, name) VALUES (3, 3, 'test')",
				"INSERT INTO `stest_1`.`st_2`(id, age) VALUES (4, 4)",
				"ALTER TABLE `stest_1`.`st_2` ADD COLUMN NAME VARCHAR(30)",
				"INSERT INTO `stest_1`.`st_1`(id, age, name) VALUES (5, 5, 'test')",
				"INSERT INTO `stest_1`.`st_2`(id, age, name) VALUES (6, 6, 'test')",
			},
			[]struct {
				sql  string
				args []driver.Value
			}{
				{
					"REPLACE INTO",
					[]driver.Value{1, 1},
				},
				{
					"REPLACE INTO",
					[]driver.Value{2, 2},
				},
				{
					"REPLACE INTO",
					[]driver.Value{4, 4},
				},
				{
					"ALTER TABLE",
					[]driver.Value{},
				},
				{
					"REPLACE INTO",
					[]driver.Value{3, 3, "test"},
				},
				{
					"REPLACE INTO",
					[]driver.Value{5, 5, "test"},
				},
				{
					"REPLACE INTO",
					[]driver.Value{6, 6, "test"},
				},
			},
		},
	}

	dropSQLs := []string{
		"DROP DATABASE IF EXISTS stest_1",
		// drop checkpoint info for next test
		fmt.Sprintf("DROP DATABASE IF EXISTS %s", s.cfg.MetaSchema),
	}

	runSQL := func(sqls []string) {
		for _, sql := range sqls {
			_, err := s.db.Exec(sql)
			c.Assert(err, IsNil)
		}
	}

	s.cfg.Flavor = "mysql"
	s.cfg.BWList = &filter.Rules{
		DoDBs: []string{"stest_1"},
	}
	s.cfg.IsSharding = true
	s.cfg.RouteRules = []*router.TableRule{
		{
			SchemaPattern: "stest_1",
			TablePattern:  "st_*",
			TargetSchema:  "stest",
			TargetTable:   "st",
		},
	}
	// set batch to 1 is easy to mock
	s.cfg.Batch = 1
	s.cfg.WorkerCount = 1

	for i, _case := range testCases {
		// drop first if last time test failed
		runSQL(dropSQLs)
		s.resetMaster()
		// must wait for reset Master finish
		time.Sleep(2 * time.Second)

		db, mock, err := sqlmock.New()
		if err != nil {
			c.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
		}
		syncer := NewSyncer(s.cfg)
		syncer.Init()

		c.Assert(syncer.checkpoint.GlobalPoint(), Equals, minCheckpoint)
		c.Assert(syncer.checkpoint.FlushedGlobalPoint(), Equals, minCheckpoint)

		// make syncer write to mock baseConn
		syncer.toDBs = []*Conn{{cfg: s.cfg, baseConn: &baseconn.BaseConn{db, s.dbAddr, &retry.FiniteRetryStrategy{}}}}
		syncer.ddlDB = &Conn{cfg: s.cfg, baseConn: &baseconn.BaseConn{db, s.dbAddr, &retry.FiniteRetryStrategy{}}}

		// run sql on upstream baseConn to generate binlog event
		runSQL(createSQLs)

		// mock downstream baseConn result
		mock.ExpectBegin()
		mock.ExpectExec("CREATE DATABASE").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()

		mock.ExpectBegin()
		mock.ExpectExec("CREATE TABLE").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()

		mock.ExpectBegin()
		e := newMysqlErr(1050, "Table exist")
		mock.ExpectExec("CREATE TABLE").WillReturnError(e)
		mock.ExpectRollback()

		// mock get table in first handle RowEvent
		mock.ExpectQuery("SHOW COLUMNS").WillReturnRows(
			sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}).AddRow("id", "int", "NO", "PRI", null, "").AddRow("age", "int", "NO", "", null, ""))
		mock.ExpectQuery("SHOW INDEX").WillReturnRows(
			sqlmock.NewRows([]string{"Table", "Non_unique", "Key_name", "Seq_in_index", "Column_name",
				"Collation", "Cardinality", "Sub_part", "Packed", "Null", "Index_type", "Comment", "Index_comment"},
			).AddRow("st", 0, "PRIMARY", 1, "id", "A", 0, null, null, null, "BTREE", "", ""))

		// run sql on upstream baseConn
		runSQL(_case.testSQLs)
		// mock expect sql
		for i, expectSQL := range _case.expectSQLS {
			mock.ExpectBegin()
			if strings.HasPrefix(expectSQL.sql, "ALTER") {
				mock.ExpectExec(expectSQL.sql).WillReturnResult(sqlmock.NewResult(1, int64(i)+1))
				mock.ExpectCommit()
				// mock get table after ddl sql exec
				mock.ExpectQuery("SHOW COLUMNS").WillReturnRows(
					sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}).AddRow("id", "int", "NO", "PRI", null, "").AddRow("age", "int", "NO", "", null, "").AddRow("name", "varchar", "NO", "", null, ""))
				mock.ExpectQuery("SHOW INDEX").WillReturnRows(
					sqlmock.NewRows([]string{"Table", "Non_unique", "Key_name", "Seq_in_index", "Column_name",
						"Collation", "Cardinality", "Sub_part", "Packed", "Null", "Index_type", "Comment", "Index_comment"},
					).AddRow("st", 0, "PRIMARY", 1, "id", "A", 0, null, null, null, "BTREE", "", ""))
			} else {
				// change insert to replace because of safe mode
				mock.ExpectExec(expectSQL.sql).WithArgs(expectSQL.args...).WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectCommit()
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		resultCh := make(chan pb.ProcessResult)

		go syncer.Process(ctx, resultCh)

		go func() {
			// sleep to ensure ddlExecInfo.Send() happen after ddlExecInfo.Renew() in Process
			// because Renew() will generate new channel, Send() may send to old channel due to goroutine schedule
			time.Sleep(1 * time.Second)
			// mock permit exec ddl request from dm-master
			req := &DDLExecItem{&pb.ExecDDLRequest{Exec: true}, make(chan error, 1)}
			syncer.ddlExecInfo.Send(ctx, req)
		}()

		select {
		case r := <-resultCh:
			for _, err := range r.Errors {
				c.Errorf("Case %d: Process Err:%s", i, err)
			}
			c.Assert(len(r.Errors), Equals, 0)
		case <-time.After(2 * time.Second):
		}
		// wait for flush finish in Process.Run()
		// cancel function only closed done channel asynchronously
		// other goroutine(s.streamer.GetEvent()) received done msg then trigger flush
		// so the flush action may not execute before we check due to goroutine schedule
		cancel()

		syncer.Close()
		c.Assert(syncer.isClosed(), IsTrue)

		flushedGP := syncer.checkpoint.FlushedGlobalPoint().Pos
		GP := syncer.checkpoint.GlobalPoint().Pos
		c.Assert(GP, Equals, flushedGP)

		// check expectations for mock baseConn
		if err := mock.ExpectationsWereMet(); err != nil {
			c.Errorf("there were unfulfilled expectations: %s", err)
		}
		runSQL(dropSQLs)
	}
}

func (s *testSyncerSuite) TestRun(c *C) {
	// 1. run syncer with column mapping
	// 2. execute some sqls which will trigger casuality
	// 3. check the generated jobs
	// 4. update config, add route rules, and update syncer
	// 5. execute somes sqls and then check jobs generated

	defer s.db.Exec("drop database if exists test_1")

	s.resetMaster()
	s.resetBinlogSyncer()
	testJobs.jobs = testJobs.jobs[:0]

	s.cfg.BWList = &filter.Rules{
		DoDBs: []string{"test_1"},
		DoTables: []*filter.Table{
			{Schema: "test_1", Name: "t_1"},
			{Schema: "test_1", Name: "t_2"},
		},
	}

	s.cfg.ColumnMappingRules = []*cm.Rule{
		{
			PatternSchema: "test_*",
			PatternTable:  "t_*",
			SourceColumn:  "id",
			TargetColumn:  "id",
			Expression:    cm.PartitionID,
			Arguments:     []string{"1", "test_", "t_"},
		},
	}

	s.cfg.Batch = 1000
	s.cfg.WorkerCount = 2
	s.cfg.DisableCausality = false

	syncer := NewSyncer(s.cfg)
	err := syncer.Init()
	c.Assert(err, IsNil)
	c.Assert(syncer.Type(), Equals, pb.UnitType_Sync)

	syncer.addJobFunc = syncer.addJobToMemory

	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan pb.ProcessResult)

	go syncer.Process(ctx, resultCh)

	sqls1 := []string{
		"create database if not exists test_1",
		"create table if not exists test_1.t_1(id int primary key, name varchar(24))",
		"create table if not exists test_1.t_2(id int primary key, name varchar(24))",
		"insert into test_1.t_1 values(1, 'a')",
		"alter table test_1.t_1 add index index1(name)",
		"insert into test_1.t_1 values(2, 'b')",
		"delete from test_1.t_1 where id = 1",
		"update test_1.t_1 set id = 1 where id = 2", // will find casuality and then generate flush job
	}
	expectJobs1 := []*expectJob{
		{
			ddl,
			"CREATE DATABASE IF NOT EXISTS `test_1`",
			nil,
		}, {
			ddl,
			"CREATE TABLE IF NOT EXISTS `test_1`.`t_1` (`id` INT PRIMARY KEY,`name` VARCHAR(24))",
			nil,
		}, {
			ddl,
			"CREATE TABLE IF NOT EXISTS `test_1`.`t_2` (`id` INT PRIMARY KEY,`name` VARCHAR(24))",
			nil,
		}, {
			insert,
			"REPLACE INTO `test_1`.`t_1` (`id`,`name`) VALUES (?,?);",
			[]interface{}{int64(580981944116838401), "a"},
		}, {
			ddl,
			"ALTER TABLE `test_1`.`t_1` ADD INDEX `index1`(`name`)",
			nil,
		}, {
			insert,
			"REPLACE INTO `test_1`.`t_1` (`id`,`name`) VALUES (?,?);",
			[]interface{}{int64(580981944116838402), "b"},
		}, {
			del,
			"DELETE FROM `test_1`.`t_1` WHERE `id` = ? LIMIT 1;",
			[]interface{}{int64(580981944116838401)},
		}, {
			flush,
			"",
			nil,
		}, {
			// in first 5 minutes, safe mode is true, will split update to delete + replace
			update,
			"DELETE FROM `test_1`.`t_1` WHERE `id` = ? LIMIT 1;",
			[]interface{}{int64(580981944116838402)},
		}, {
			// in first 5 minutes, , safe mode is true, will split update to delete + replace
			update,
			"REPLACE INTO `test_1`.`t_1` (`id`,`name`) VALUES (?,?);",
			[]interface{}{int64(580981944116838401), "b"},
		},
	}

	executeSQLAndWait(c, s.db, sqls1, len(expectJobs1))

	testJobs.Lock()
	checkJobs(c, testJobs.jobs, expectJobs1)
	testJobs.jobs = testJobs.jobs[:0]
	testJobs.Unlock()

	s.cfg.ColumnMappingRules = nil
	s.cfg.RouteRules = []*router.TableRule{
		{
			SchemaPattern: "test_1",
			TablePattern:  "t_1",
			TargetSchema:  "test_1",
			TargetTable:   "t_2",
		},
	}

	cancel()
	// when syncer exit Run(), will flush job
	syncer.Pause()
	syncer.Update(s.cfg)

	s.resetMaster()
	s.resetBinlogSyncer()

	ctx, cancel = context.WithCancel(context.Background())
	resultCh = make(chan pb.ProcessResult)
	go syncer.Resume(ctx, resultCh)

	sql2 := []string{
		"insert into test_1.t_1 values(3, 'c')",
		"delete from test_1.t_1 where id = 3",
	}

	expectJobs2 := []*expectJob{
		{
			flush,
			"",
			nil,
		}, {
			insert,
			"REPLACE INTO `test_1`.`t_2` (`id`,`name`) VALUES (?,?);",
			[]interface{}{int32(3), "c"},
		}, {
			del,
			"DELETE FROM `test_1`.`t_2` WHERE `id` = ? LIMIT 1;",
			[]interface{}{int32(3)},
		},
	}

	executeSQLAndWait(c, s.db, sql2, len(expectJobs2))

	testJobs.RLock()
	checkJobs(c, testJobs.jobs, expectJobs2)
	testJobs.RUnlock()

	status := syncer.Status().(*pb.SyncStatus)
	c.Assert(status.TotalEvents, Equals, int64(len(expectJobs1)+len(expectJobs2)))

	cancel()
	syncer.Close()
	c.Assert(syncer.isClosed(), IsTrue)
}

func executeSQLAndWait(c *C, db *sql.DB, sqls []string, expectJobNum int) {
	for _, sql := range sqls {
		c.Log("exec sql: ", sql)
		_, err := db.Exec(sql)
		c.Assert(err, IsNil)
	}

	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)

		testJobs.RLock()
		jobNum := len(testJobs.jobs)
		testJobs.RUnlock()

		if jobNum >= expectJobNum {
			break
		}
	}
}

type expectJob struct {
	tp       opType
	sqlInJob string
	args     []interface{}
}

func checkJobs(c *C, jobs []*job, expectJobs []*expectJob) {
	c.Assert(jobs, HasLen, len(expectJobs))
	for i, job := range jobs {
		c.Log(i, job.tp, job.ddls, job.sql, job.args)

		c.Assert(job.tp, Equals, expectJobs[i].tp)
		if job.tp == ddl {
			c.Assert(job.ddls[0], Equals, expectJobs[i].sqlInJob)
		} else {
			c.Assert(job.sql, Equals, expectJobs[i].sqlInJob)
			c.Assert(job.args, DeepEquals, expectJobs[i].args)
		}
	}
}

var testJobs struct {
	sync.RWMutex
	jobs []*job
}

func (s *Syncer) addJobToMemory(job *job) error {
	log.L().Info("add job to memory", zap.Stringer("job", job))

	switch job.tp {
	case ddl, insert, update, del, flush:
		s.addCount(false, "test", job.tp, 1)
		testJobs.Lock()
		testJobs.jobs = append(testJobs.jobs, job)
		testJobs.Unlock()
	}

	return nil
}
