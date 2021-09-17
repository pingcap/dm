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

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-mysql-org/go-mysql/replication"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tidb-tools/pkg/filter"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/conn"
)

type testFilterSuite struct {
	baseConn *conn.BaseConn
	db       *sql.DB
}

var _ = Suite(&testFilterSuite{})

func (s *testFilterSuite) SetUpSuite(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	s.db = db
	mock.ExpectClose()
	con, err := db.Conn(context.Background())
	c.Assert(err, IsNil)
	s.baseConn = conn.NewBaseConn(con, nil)
}

func (s *testFilterSuite) TearDownSuite(c *C) {
	c.Assert(s.baseConn.DBConn.Close(), IsNil)
	c.Assert(s.db.Close(), IsNil)
}

func (s *testFilterSuite) TestFilterQueryEvent(c *C) {
	syncer := &Syncer{}
	// test binlog filter
	filterRules := []*bf.BinlogEventRule{
		{
			SchemaPattern: "foo*",
			TablePattern:  "",
			Events:        []bf.EventType{bf.CreateTable},
			SQLPattern:    []string{"^create\\s+table"},
			Action:        bf.Ignore,
		},
	}
	var err error
	syncer.binlogFilter, err = bf.NewBinlogEvent(false, filterRules)
	c.Assert(err, IsNil)

	cases := []struct {
		sql           string
		tables        []*filter.Table
		expectSkipped bool
	}{
		{
			// system table
			"create table mysql.test (id int)",
			[]*filter.Table{{Schema: "mysql", Name: "test"}},
			true,
		}, {
			// test filter one event
			"drop table foo.test",
			[]*filter.Table{{Schema: "foo", Name: "test"}},
			false,
		}, {
			"create table foo.test (id int)",
			[]*filter.Table{{Schema: "foo", Name: "test"}},
			true,
		},
	}
	p := parser.New()
	for _, ca := range cases {
		stmt, err := p.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil)
		skipped, err2 := syncer.filterQueryEvent(ca.tables, stmt, ca.sql)
		c.Assert(err2, IsNil)
		c.Assert(skipped, Equals, ca.expectSkipped)
	}
}

func (s *testFilterSuite) TestFilterRowsEvent(c *C) {
	syncer := &Syncer{}
	filterRules := []*bf.BinlogEventRule{
		{
			SchemaPattern: "foo*",
			TablePattern:  "",
			Events:        []bf.EventType{bf.InsertEvent},
			SQLPattern:    []string{""},
			Action:        bf.Ignore,
		},
	}
	var err error
	syncer.binlogFilter, err = bf.NewBinlogEvent(false, filterRules)
	c.Assert(err, IsNil)

	cases := []struct {
		table         *filter.Table
		eventType     replication.EventType
		expectSkipped bool
	}{
		{
			// test filter one event
			&filter.Table{Schema: "foo", Name: "test"},
			replication.WRITE_ROWS_EVENTv0,
			true,
		}, {
			&filter.Table{Schema: "foo", Name: "test"},
			replication.UPDATE_ROWS_EVENTv0,
			false,
		}, {
			&filter.Table{Schema: "foo", Name: "test"},
			replication.DELETE_ROWS_EVENTv0,
			false,
		},
	}
	for _, ca := range cases {
		skipped, err2 := syncer.filterRowsEvent(ca.table, ca.eventType)
		c.Assert(err2, IsNil)
		c.Assert(skipped, Equals, ca.expectSkipped)
	}
}

func (s *testFilterSuite) TestFilterOneEvent(c *C) {
	cfg := &config.SubTaskConfig{
		BAList: &filter.Rules{
			IgnoreDBs: []string{"s1"},
		},
	}
	syncer := NewSyncer(cfg, nil)
	// test binlog filter
	filterRules := []*bf.BinlogEventRule{
		{
			SchemaPattern: "*",
			TablePattern:  "",
			Events:        []bf.EventType{bf.DropTable},
			SQLPattern:    []string{"^drop\\s+table"},
			Action:        bf.Ignore,
		},
	}
	var err error
	syncer.binlogFilter, err = bf.NewBinlogEvent(false, filterRules)
	c.Assert(err, IsNil)

	cases := []struct {
		sql           string
		table         *filter.Table
		eventType     bf.EventType
		expectSkipped bool
	}{
		{
			// system table
			"create table mysql.test (id int)",
			&filter.Table{Schema: "mysql", Name: "test"},
			"",
			true,
		}, {

			// test binlog filter
			"drop table tx.test",
			&filter.Table{Schema: "tx", Name: "test"},
			bf.DropTable,
			true,
		}, {
			"create table tx.test (id int)",
			&filter.Table{Schema: "tx", Name: "test"},
			bf.CreateTable,
			false,
		}, {
			// test balist
			"create table s1.test (id int)",
			&filter.Table{Schema: "s1", Name: "test"},
			bf.CreateTable,
			false, // there should be true, so what does BAList configure?
		},
	}
	for _, ca := range cases {
		skipped, err2 := syncer.filterOneEvent(ca.table, ca.eventType, ca.sql)
		c.Assert(err2, IsNil)
		c.Assert(skipped, Equals, ca.expectSkipped)
	}
}
