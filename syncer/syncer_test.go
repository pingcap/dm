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
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	cm "github.com/pingcap/tidb-tools/pkg/column-mapping"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/siddontang/go-mysql/replication"

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
	s.cfg.From.Adjust()
	s.cfg.To.Adjust()

	dir := c.MkDir()
	s.cfg.RelayDir = dir

	var err error
	dbAddr := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8", s.cfg.From.User, s.cfg.From.Password, s.cfg.From.Host, s.cfg.From.Port)
	s.db, err = sql.Open("mysql", dbAddr)
	if err != nil {
		log.Fatal(err)
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
			log.Fatal(err2)
		}
		cfg.TimestampStringLocation = timezone
	}

	if s.syncer != nil {
		s.syncer.Close()
	}

	pos, _, err := utils.GetMasterStatus(s.db, "mysql")
	if err != nil {
		log.Fatal(err)
	}

	s.syncer = replication.NewBinlogSyncer(cfg)
	s.streamer, err = s.syncer.StartSync(pos)
	if err != nil {
		log.Fatal(err)
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
				"REPLACE INTO `gctest_1`.`t_1` (`id`,`age`,`cfg`) VALUES (?,?,?);",
				"REPLACE INTO `gctest_1`.`t_1` (`id`,`age`,`cfg`) VALUES (?,?,?);",
				"REPLACE INTO `gctest_1`.`t_1` (`id`,`age`,`cfg`) VALUES (?,?,?);",
				"REPLACE INTO `gctest_1`.`t_2` (`id`,`age`,`cfg`) VALUES (?,?,?);",
				"REPLACE INTO `gctest_1`.`t_2` (`id`,`age`,`cfg`) VALUES (?,?,?);",
				"REPLACE INTO `gctest_1`.`t_2` (`id`,`age`,`cfg`) VALUES (?,?,?);",
				"REPLACE INTO `gctest_1`.`t_3` (`id`,`cfg`) VALUES (?,?);",
				"REPLACE INTO `gctest_1`.`t_3` (`id`,`cfg`) VALUES (?,?);",
				"REPLACE INTO `gctest_1`.`t_3` (`id`,`cfg`) VALUES (?,?);",
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
			c.Assert(err, IsNil)
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

func (s *testSyncerSuite) TestRun(c *C) {
	// 1. run syncer with column mapping
	// 2. update config, add route rules, and update syncer

	defer s.db.Exec("drop database if exists test_1")

	s.resetMaster()
	s.resetBinlogSyncer()

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

	s.cfg.Batch = 10
	s.cfg.WorkerCount = 2

	syncer := NewSyncer(s.cfg)
	err := syncer.Init()
	c.Assert(err, IsNil)
	c.Assert(syncer.Type(), Equals, pb.UnitType_Sync)

	syncer.addJobFunc = syncer.addJobToMemory

	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan pb.ProcessResult)

	go syncer.Process(ctx, resultCh)

	testCases1 := []struct {
		sql      string
		tp       opType
		sqlInJob string
		arg      interface{}
	}{
		{
			"create database if not exists test_1",
			ddl,
			"CREATE DATABASE IF NOT EXISTS `test_1`",
			nil,
		}, {
			"create table if not exists test_1.t_1(id int)",
			ddl,
			"CREATE TABLE IF NOT EXISTS `test_1`.`t_1` (`id` INT)",
			nil,
		}, {
			"create table if not exists test_1.t_2(id int)",
			ddl,
			"CREATE TABLE IF NOT EXISTS `test_1`.`t_2` (`id` INT)",
			nil,
		}, {
			"insert into test_1.t_1 values(1)",
			insert,
			"REPLACE INTO `test_1`.`t_1` (`id`) VALUES (?);",
			int64(580981944116838401),
		}, {
			"alter table test_1.t_1 add index index1(id)",
			ddl,
			"ALTER TABLE `test_1`.`t_1` ADD INDEX `index1`(`id`)",
			nil,
		}, {
			"insert into test_1.t_1 values(2)",
			insert,
			"REPLACE INTO `test_1`.`t_1` (`id`) VALUES (?);",
			int64(580981944116838402),
		}, {
			"delete from test_1.t_1 where id = 1",
			del,
			"DELETE FROM `test_1`.`t_1` WHERE `id` = ? LIMIT 1;",
			int64(580981944116838401),
		},
	}

	for _, testCase := range testCases1 {
		c.Log("exec sql: ", testCase.sql)
		_, err := s.db.Exec(testCase.sql)
		c.Assert(err, IsNil)
	}

	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)

		testJobs.RLock()
		jobNum := len(testJobs.jobs)
		testJobs.RUnlock()

		if jobNum >= len(testCases1) {
			break
		}
	}

	testJobs.Lock()
	c.Assert(testJobs.jobs, HasLen, len(testCases1))
	for i, testCase := range testCases1 {
		c.Assert(testJobs.jobs[i].tp, Equals, testCase.tp)
		if testJobs.jobs[i].tp == ddl {
			c.Assert(testJobs.jobs[i].ddls[0], Equals, testCase.sqlInJob)
		} else {
			c.Assert(testJobs.jobs[i].sql, Equals, testCase.sqlInJob)
			c.Assert(testJobs.jobs[i].args[0], Equals, testCase.arg)
		}
	}

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
	syncer.Pause()
	syncer.Update(s.cfg)

	s.resetMaster()
	s.resetBinlogSyncer()

	ctx, cancel = context.WithCancel(context.Background())
	resultCh = make(chan pb.ProcessResult)
	go syncer.Resume(ctx, resultCh)

	testCases2 := []struct {
		sql      string
		tp       opType
		sqlInJob string
		arg      interface{}
	}{
		{
			"insert into test_1.t_1 values(3)",
			insert,
			"REPLACE INTO `test_1`.`t_2` (`id`) VALUES (?);",
			int32(3),
		}, {
			"delete from test_1.t_1 where id = 3",
			del,
			"DELETE FROM `test_1`.`t_2` WHERE `id` = ? LIMIT 1;",
			int32(3),
		},
	}

	for _, testCase := range testCases2 {
		c.Log("exec sql: ", testCase.sql)
		_, err := s.db.Exec(testCase.sql)
		c.Assert(err, IsNil)
	}

	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)

		testJobs.RLock()
		jobNum := len(testJobs.jobs)
		testJobs.RUnlock()

		if jobNum >= len(testCases2) {
			break
		}
	}

	testJobs.RLock()
	c.Assert(testJobs.jobs, HasLen, len(testCases2))
	for i, testCase := range testCases2 {
		c.Assert(testJobs.jobs[i].tp, Equals, testCase.tp)
		if testJobs.jobs[i].tp == ddl {
			c.Assert(testJobs.jobs[i].ddls[0], Equals, testCase.sqlInJob)
		} else {
			c.Assert(testJobs.jobs[i].sql, Equals, testCase.sqlInJob)
			c.Assert(testJobs.jobs[i].args[0], Equals, testCase.arg)
		}
	}
	testJobs.RUnlock()

	status := syncer.Status().(*pb.SyncStatus)
	c.Assert(status.TotalEvents, Equals, int64(len(testCases1)+len(testCases2)))

	cancel()
	syncer.Close()
	c.Assert(syncer.isClosed(), IsTrue)
}

var testJobs struct {
	sync.RWMutex
	jobs []*job
}

func (s *Syncer) addJobToMemory(job *job) error {
	log.Infof("addJobToMemory: %v", job)

	switch job.tp {
	case ddl, insert, update, del:
		s.addCount(false, "test", job.tp, 1)
		testJobs.Lock()
		testJobs.jobs = append(testJobs.jobs, job)
		testJobs.Unlock()
	}

	return nil
}
