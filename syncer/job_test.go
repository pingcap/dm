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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/filter"

	"github.com/pingcap/dm/pkg/binlog"
)

var _ = Suite(&testJobSuite{})

type testJobSuite struct{}

func (t *testJobSuite) TestJobTypeString(c *C) {
	testCases := []struct {
		tp  opType
		str string
	}{
		{
			null,
			"",
		}, {
			insert,
			"insert",
		}, {
			update,
			"update",
		}, {
			del,
			"delete",
		}, {
			ddl,
			"ddl",
		}, {
			xid,
			"xid",
		}, {
			flush,
			"flush",
		}, {
			skip,
			"skip",
		}, {
			rotate,
			"rotate",
		},
	}

	for _, testCase := range testCases {
		tpStr := testCase.tp.String()
		c.Assert(tpStr, Equals, testCase.str)
	}
}

func (t *testJobSuite) TestJob(c *C) {
	ddlInfo := &ddlInfo{
		sourceTables: []*filter.Table{{Schema: "test1", Name: "t1"}},
		targetTables: []*filter.Table{{Schema: "test2", Name: "t2"}},
	}
	table := &filter.Table{Schema: "test", Name: "t1"}
	location := binlog.NewLocation("")
	ec := &eventContext{startLocation: &location, currentLocation: &location, lastLocation: &location}
	qec := &queryEventContext{
		eventContext:    ec,
		originSQL:       "create database test",
		needHandleDDLs:  []string{"create database test"},
		shardingDDLInfo: ddlInfo,
	}
	testCases := []struct {
		job    *job
		jobStr string
	}{
		{
			newDMLJob(insert, table, table, nil, ec),
			"tp: insert, ddls: [], last_location: position: (, 4), gtid-set: , start_location: position: (, 4), gtid-set: , current_location: position: (, 4), gtid-set: ",
		}, {
			newDDLJob(qec),
			"tp: ddl, ddls: [create database test], last_location: position: (, 4), gtid-set: , start_location: position: (, 4), gtid-set: , current_location: position: (, 4), gtid-set: ",
		}, {
			newXIDJob(binlog.NewLocation(""), binlog.NewLocation(""), binlog.NewLocation("")),
			"tp: xid, ddls: [], last_location: position: (, 4), gtid-set: , start_location: position: (, 4), gtid-set: , current_location: position: (, 4), gtid-set: ",
		}, {
			newFlushJob(),
			"tp: flush, ddls: [], last_location: position: (, 0), gtid-set: , start_location: position: (, 0), gtid-set: , current_location: position: (, 0), gtid-set: ",
		}, {
			newSkipJob(ec),
			"tp: skip, ddls: [], last_location: position: (, 4), gtid-set: , start_location: position: (, 0), gtid-set: , current_location: position: (, 0), gtid-set: ",
		},
	}

	for _, testCase := range testCases {
		c.Assert(testCase.job.String(), Equals, testCase.jobStr)
	}
}

func (t *testJobSuite) TestQueueBucketName(c *C) {
	name := queueBucketName(0)
	c.Assert(name, Equals, "q_0")

	name = queueBucketName(8)
	c.Assert(name, Equals, "q_0")

	name = queueBucketName(9)
	c.Assert(name, Equals, "q_1")
}
