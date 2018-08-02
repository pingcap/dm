// Copyright 2016 PingCAP, Inc.
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
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-enterprise-tools/dm/config"
	"github.com/pingcap/tidb-enterprise-tools/pkg/filter"
	gmysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"golang.org/x/net/context"
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
	host := os.Getenv("MYSQL_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port, _ := strconv.Atoi(os.Getenv("MYSQL_PORT"))
	if port == 0 {
		port = 3306
	}
	user := os.Getenv("MYSQL_USER")
	if user == "" {
		user = "root"
	}
	pswd := os.Getenv("MYSQL_PSWD")

	s.cfg = &config.SubTaskConfig{
		From: config.DBConfig{
			Host:     host,
			User:     user,
			Password: pswd,
			Port:     port,
		},
		ServerID: 101,
	}

	var err error
	dbAddr := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8", s.cfg.From.User, s.cfg.From.Password, s.cfg.From.Host, s.cfg.From.Port)
	s.db, err = sql.Open("mysql", dbAddr)
	if err != nil {
		log.Fatal(err)
	}

	s.syncer = replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID:   uint32(s.cfg.ServerID),
		Flavor:     "mysql",
		Host:       s.cfg.From.Host,
		Port:       uint16(s.cfg.From.Port),
		User:       s.cfg.From.User,
		Password:   s.cfg.From.Password,
		UseDecimal: true,
	})
	s.resetMaster()
	s.streamer, err = s.syncer.StartSync(gmysql.Position{Name: "", Pos: 4})
	if err != nil {
		log.Fatal(err)
	}

	s.db.Exec("SET GLOBAL binlog_format = 'ROW';")
}

func (s *testSyncerSuite) TearDownSuite(c *C) {
	s.db.Close()
}

func (s *testSyncerSuite) resetMaster() {
	s.db.Exec("reset master")
}

func (s *testSyncerSuite) clearRules() {
	s.cfg.DoDBs = nil
	s.cfg.DoTables = nil
	s.cfg.IgnoreDBs = nil
	s.cfg.IgnoreTables = nil
	s.cfg.SkipEvents = nil
	s.cfg.SkipDMLs = nil
}

func (s *testSyncerSuite) TestSelectDB(c *C) {
	s.cfg.DoDBs = []string{"~^b.*", "s1", "stest"}
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
	res := []bool{false, false, true, true, false, false, false, false, false, false, true, true}

	for _, sql := range sqls {
		s.db.Exec(sql)
	}

	syncer := NewSyncer(s.cfg)
	syncer.genRouter()
	syncer.genSkipDMLRules()
	i := 0
	for {
		if i >= len(sqls) {
			break
		}

		e, err := s.streamer.GetEvent(context.Background())
		c.Assert(err, IsNil)
		ev, ok := e.Event.(*replication.QueryEvent)
		if !ok {
			continue
		}
		sql := string(ev.Query)
		if syncer.skipQueryEvent(sql) {
			continue
		}

		tableNames, err := syncer.fetchDDLTableNames(sql, string(ev.Schema))
		c.Assert(err, IsNil)
		r := syncer.skipQueryDDL(sql, tableNames[1])
		c.Assert(r, Equals, res[i])
		i++
	}
	s.clearRules()
}

func (s *testSyncerSuite) TestSelectTable(c *C) {
	s.cfg.DoDBs = []string{"t2"}
	s.cfg.DoTables = []*filter.Table{
		{Schema: "stest", Name: "log"},
		{Schema: "stest", Name: "~^t.*"},
		{Schema: "~^ptest*", Name: "~^t.*"},
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
		{false},
		{false},
		{false},
		{false},
		{false},
		{false},
	}

	for _, sql := range sqls {
		s.db.Exec(sql)
	}

	syncer := NewSyncer(s.cfg)
	syncer.genRouter()
	syncer.genSkipDMLRules()
	i := 0
	for {
		if i >= len(sqls) {
			break
		}

		e, err := s.streamer.GetEvent(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		switch ev := e.Event.(type) {
		case *replication.QueryEvent:
			query := string(ev.Query)
			if syncer.skipQueryEvent(query) {
				continue
			}

			querys, err := resolveDDLSQL(query)
			if err != nil {
				log.Fatalf("ResolveDDlSQL failed %v", err)
			}
			if len(querys) == 0 {
				continue
			}
			log.Debugf("querys:%+v", querys)
			for j, q := range querys {
				tableNames, err := syncer.fetchDDLTableNames(q, string(ev.Schema))
				c.Assert(err, IsNil)
				r := syncer.skipQueryDDL(q, tableNames[1])
				c.Assert(r, Equals, res[i][j])
			}
		case *replication.RowsEvent:
			r := syncer.skipRowEvent(string(ev.Table.Schema), string(ev.Table.Table), e.Header.EventType)
			c.Assert(r, Equals, res[i][0])

		default:
			continue
		}

		i++

	}
	s.clearRules()
}

func (s *testSyncerSuite) TestIgnoreDB(c *C) {
	s.cfg.IgnoreDBs = []string{"~^b.*", "s1", "stest"}
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

	syncer := NewSyncer(s.cfg)
	syncer.genRouter()
	syncer.genSkipDMLRules()
	i := 0
	for {
		if i >= len(sqls) {
			break
		}

		e, err := s.streamer.GetEvent(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		ev, ok := e.Event.(*replication.QueryEvent)
		if !ok {
			continue
		}
		sql := string(ev.Query)
		if syncer.skipQueryEvent(sql) {
			continue
		}
		tableNames, err := syncer.fetchDDLTableNames(sql, string(ev.Schema))
		c.Assert(err, IsNil)
		r := syncer.skipQueryDDL(sql, tableNames[1])
		c.Assert(r, Equals, res[i])
		i++
	}
	s.clearRules()
}

func (s *testSyncerSuite) TestIgnoreTable(c *C) {
	s.cfg.IgnoreDBs = []string{"t2"}
	s.cfg.IgnoreTables = []*filter.Table{
		{Schema: "stest", Name: "log"},
		{Schema: "stest", Name: "~^t.*"},
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
		{true},
		{true},
		{true},
		{false},
		{true},
		{true},
		{false},
		{true, true, false},
		{true},

		{true},
		{true},
		{true},
		{true},
		{true},
	}

	for _, sql := range sqls {
		s.db.Exec(sql)
	}

	syncer := NewSyncer(s.cfg)
	syncer.genRouter()
	syncer.genSkipDMLRules()
	i := 0
	for {
		if i >= len(sqls) {
			break
		}
		e, err := s.streamer.GetEvent(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		switch ev := e.Event.(type) {
		case *replication.QueryEvent:
			query := string(ev.Query)
			if syncer.skipQueryEvent(query) {
				continue
			}

			querys, err := resolveDDLSQL(query)
			if err != nil {
				log.Fatalf("ResolveDDlSQL failed %v", err)
			}
			if len(querys) == 0 {
				continue
			}

			for j, q := range querys {
				tableNames, err := syncer.fetchDDLTableNames(q, string(ev.Schema))
				c.Assert(err, IsNil)
				r := syncer.skipQueryDDL(q, tableNames[1])
				c.Assert(r, Equals, res[i][j])
			}
		case *replication.RowsEvent:
			r := syncer.skipRowEvent(string(ev.Table.Schema), string(ev.Table.Table), e.Header.EventType)
			c.Assert(r, Equals, res[i][0])

		default:
			continue
		}

		i++

	}
	s.clearRules()
}

func (s *testSyncerSuite) TestSkipDML(c *C) {
	s.cfg.SkipDMLs = []*config.SkipDML{
		{
			Schema: "",
			Table:  "",
			Type:   "update",
		}, {
			Schema: "foo",
			Table:  "",
			Type:   "delete",
		}, {
			Schema: "foo1",
			Table:  "bar1",
			Type:   "delete",
		},
	}

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

	res := make([]bool, 0, len(sqls))
	for _, t := range sqls {
		if t.isDML {
			res = append(res, t.skipped)
		}
		s.db.Exec(t.sql)
	}

	syncer := NewSyncer(s.cfg)
	syncer.genRouter()
	syncer.genSkipDMLRules()

	c.Logf("skip rules %+v", syncer.skipDMLRules)

	i := 0
	for {
		if i >= len(res) {
			break
		}
		e, err := s.streamer.GetEvent(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		switch ev := e.Event.(type) {
		case *replication.QueryEvent:
		case *replication.RowsEvent:
			r := syncer.skipRowEvent(string(ev.Table.Schema), string(ev.Table.Table), e.Header.EventType)
			c.Logf("%s.%s, ev %v", string(ev.Table.Schema), string(ev.Table.Table), e.Header.EventType)
			c.Assert(r, Equals, res[i])
			i++
		default:
		}
	}
	s.clearRules()
}
