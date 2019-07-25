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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	_ "github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	cm "github.com/pingcap/tidb-tools/pkg/column-mapping"
	"github.com/pingcap/tidb-tools/pkg/filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"

	"github.com/siddontang/go-mysql/replication"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/binlog/event"
	"github.com/pingcap/dm/pkg/log"
	parserpkg "github.com/pingcap/dm/pkg/parser"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

var _ = Suite(&testSyncerSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testSyncerSuite struct {
	db       *sql.DB
	syncer   *replication.BinlogSyncer
	streamer *replication.BinlogStreamer
	cfg      *config.SubTaskConfig
}

func (s *testSyncerSuite) SetUpSuite(c *C) {
	s.cfg = &config.SubTaskConfig{
		From:       getDBConfigFromEnv(),
		To:         getDBConfigFromEnv(),
		ServerID:   101,
		MetaSchema: "test",
		Name:       "syncer_ut",
		Mode:       config.ModeIncrement,
	}
	s.cfg.From.Adjust(terror.ScopeUpstream)
	s.cfg.To.Adjust(terror.ScopeDownstream)

	dir := c.MkDir()
	s.cfg.RelayDir = dir

	var err error
	dbAddr := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8", s.cfg.From.User, s.cfg.From.Password, s.cfg.From.Host, s.cfg.From.Port)
	s.db, err = sql.Open("mysql", dbAddr)
	if err != nil {
		log.L().Fatal("", zap.Error(err))
	}

	s.resetMaster()
	s.resetBinlogSyncer()

	_, err = s.db.Exec("SET GLOBAL binlog_format = 'ROW';")
	c.Assert(err, IsNil)
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

	p, err := utils.GetParser(s.db, false)
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
	s.resetBinlogSyncer()

	s.cfg.BWList = &filter.Rules{
		DoDBs: []string{"t2", "stest", "~^ptest*"},
		DoTables: []*filter.Table{
			{Schema: "stest", Name: "log"},
			{Schema: "stest", Name: "~^t.*"},
			{Schema: "~^ptest*", Name: "~^t.*"},
		},
	}

	sqls := []string{
		"create database s1",
		"create table s1.log(id int)",
		"drop database s1",

		"create table mysql.test(id int)",
		"drop table mysql.test",
		"create database stest",
		"create table stest.log(id int)",
		"create table stest.t(id int)",
		"create table stest.log2(id int)",
		"insert into stest.t(id) values (10)",
		"insert into stest.log(id) values (10)",
		"insert into stest.log2(id) values (10)",
		"drop table stest.log,stest.t,stest.log2",
		"drop database stest",

		"create database t2",
		"create table t2.log(id int)",
		"create table t2.log1(id int)",
		"drop table t2.log",
		"drop database t2",
		"create database ptest1",
		"create table ptest1.t1(id int)",
		"drop database ptest1",
	}
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
		{false, false, true},
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

	for _, sql := range sqls {
		s.db.Exec(sql)
	}

	p, err := utils.GetParser(s.db, false)
	c.Assert(err, IsNil)

	syncer := NewSyncer(s.cfg)
	syncer.genRouter()
	var i int
	for {
		if i == len(sqls) {
			break
		}
		e, err := s.streamer.GetEvent(context.Background())
		c.Assert(err, IsNil)
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
	s.catchUpBinlog()
}

func (s *testSyncerSuite) TestIgnoreDB(c *C) {
	s.resetMaster()
	s.resetBinlogSyncer()

	s.cfg.BWList = &filter.Rules{
		IgnoreDBs: []string{"~^b.*", "s1", "stest"},
	}

	sqls := []string{
		"create database s1",
		"drop database s1",
		"create database s2",
		"drop database s2",
		"create database btest",
		"drop database btest",
		"create database b1",
		"drop database b1",
		"create database stest",
		"drop database stest",
		"create database st",
		"drop database st",
	}
	res := []bool{true, true, false, false, true, true, true, true, true, true, false, false}

	for _, sql := range sqls {
		s.db.Exec(sql)
	}

	p, err := utils.GetParser(s.db, false)
	c.Assert(err, IsNil)

	syncer := NewSyncer(s.cfg)
	syncer.genRouter()
	i := 0
	for {
		if i == len(sqls) {
			break
		}

		e, err := s.streamer.GetEvent(context.Background())
		c.Assert(err, IsNil)
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
	s.catchUpBinlog()
}

func (s *testSyncerSuite) TestIgnoreTable(c *C) {
	s.resetBinlogSyncer()

	s.cfg.BWList = &filter.Rules{
		IgnoreDBs: []string{"t2"},
		IgnoreTables: []*filter.Table{
			{Schema: "stest", Name: "log"},
			{Schema: "stest", Name: "~^t.*"},
		},
	}

	sqls := []string{
		"create database s1",
		"create table s1.log(id int)",
		"drop database s1",

		"create table mysql.test(id int)",
		"drop table mysql.test",
		"create database stest",
		"create table stest.log(id int)",
		"create table stest.t(id int)",
		"create table stest.log2(id int)",
		"insert into stest.t(id) values (10)",
		"insert into stest.log(id) values (10)",
		"insert into stest.log2(id) values (10)",
		"drop table stest.log,stest.t,stest.log2",
		"drop database stest",

		"create database t2",
		"create table t2.log(id int)",
		"create table t2.log1(id int)",
		"drop table t2.log",
		"drop database t2",
	}
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
		{true, true, false},
		{false},

		{true},
		{true},
		{true},
		{true},
		{true},
	}

	for _, sql := range sqls {
		s.db.Exec(sql)
	}

	p, err := utils.GetParser(s.db, false)
	c.Assert(err, IsNil)

	syncer := NewSyncer(s.cfg)

	syncer.genRouter()
	i := 0
	for {
		if i == len(sqls) {
			break
		}
		e, err := s.streamer.GetEvent(context.Background())
		c.Assert(err, IsNil)
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
	s.catchUpBinlog()
}

func (s *testSyncerSuite) TestSkipDML(c *C) {
	s.resetBinlogSyncer()

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

	sqls := []struct {
		sql     string
		isDML   bool
		skipped bool
	}{
		{"drop database if exists foo", false, false},
		{"create database foo", false, false},
		{"create table foo.bar(id int)", false, false},
		{"insert into foo.bar values(1)", true, false},
		{"update foo.bar set id=2", true, true},
		{"delete from foo.bar where id=2", true, true},
		{"drop database if exists foo1", false, false},
		{"create database foo1", false, false},
		{"create table foo1.bar1(id int)", false, false},
		{"insert into foo1.bar1 values(1)", true, false},
		{"update foo1.bar1 set id=2", true, true},
		{"delete from foo1.bar1 where id=2", true, true},
		{"create table foo1.bar2(id int)", false, false},
		{"insert into foo1.bar2 values(1)", true, false},
		{"update foo1.bar2 set id=2", true, true},
		{"delete from foo1.bar2 where id=2", true, true},
	}

	for i := range sqls {
		s.db.Exec(sqls[i].sql)
	}

	p, err := utils.GetParser(s.db, false)
	c.Assert(err, IsNil)

	syncer := NewSyncer(s.cfg)
	syncer.genRouter()

	syncer.binlogFilter, err = bf.NewBinlogEvent(false, s.cfg.FilterRules)
	c.Assert(err, IsNil)

	i := 0
	for {
		if i >= len(sqls) {
			break
		}
		e, err := s.streamer.GetEvent(context.Background())
		c.Assert(err, IsNil)
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
			c.Assert(r, Equals, sqls[i].skipped)
		default:
			continue
		}
		i++
	}
	s.catchUpBinlog()
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

	createTableSQLs := []string{
		"create database if not exists stest_3",
		"create table if not exists stest_3.log(id varchar(45))",
		"create table if not exists stest_3.t_2(name varchar(45), id bigint)",
		"create table if not exists stest_3.a(id int)",
	}

	dmls := []struct {
		sql    string
		column []string
		data   []interface{}
	}{
		{"insert into stest_3.t_2(name, id) values (\"ian\", 10)", []string{"name", "id"}, []interface{}{"ian", int64(1<<59 | 3<<52 | 2<<44 | 10)}},
		{"insert into stest_3.log(id) values (\"10\")", []string{"id"}, []interface{}{"test:10"}},
		{"insert into stest_3.a(id) values (10)", []string{"id"}, []interface{}{int32(10)}},
	}

	dropTableSQLs := []string{
		"drop table stest_3.log,stest_3.t_2,stest_3.a",
		"drop database stest_3",
	}

	for _, sql := range createTableSQLs {
		s.db.Exec(sql)
	}

	for i := range dmls {
		s.db.Exec(dmls[i].sql)
	}

	for _, sql := range dropTableSQLs {
		s.db.Exec(sql)
	}

	p, err := utils.GetParser(s.db, false)
	c.Assert(err, IsNil)

	mapping, err := cm.NewMapping(false, rules)
	c.Assert(err, IsNil)

	totalEvent := len(dmls) + len(createTableSQLs) + len(dropTableSQLs)
	i := 0
	dmlIndex := 0
	for {
		if i == totalEvent {
			break
		}
		e, err := s.streamer.GetEvent(context.Background())
		c.Assert(err, IsNil)
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
		i++
	}
	s.catchUpBinlog()
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
		// because `sql.DB.Exec` will choose a underlying Conn for every query from the connection pool
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
	syncer.cfg.MaxRetry = 1
	// use upstream db as mock downstream
	syncer.toDBs = []*Conn{{db: s.db}}

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
	s.cfg.MaxRetry = 1

	for i, _case := range testCases {
		// drop first if last time test failed
		runSQL(dropSQLs)
		s.resetMaster()
		// must wait for reset Master finish
		time.Sleep(time.Second)

		db, mock, err := sqlmock.New()
		if err != nil {
			c.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
		}
		syncer := NewSyncer(s.cfg)
		syncer.Init()

		c.Assert(syncer.checkpoint.GlobalPoint(), Equals, minCheckpoint)
		c.Assert(syncer.checkpoint.FlushedGlobalPoint(), Equals, minCheckpoint)

		// make syncer write to mock db
		syncer.toDBs = []*Conn{{cfg: s.cfg, db: db}}
		syncer.ddlDB = &Conn{cfg: s.cfg, db: db}

		// run sql on upstream db to generate binlog event
		runSQL(createSQLs)

		// mock downstream db result
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

		// run sql on upstream db
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

		// check expectations for mock db
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
