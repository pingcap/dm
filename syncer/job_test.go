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
	"github.com/siddontang/go-mysql/mysql"
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
	ddlInfo := &shardingDDLInfo{
		tableNames: [][]*filter.Table{
			{
				{
					Schema: "test1",
					Name:   "t1",
				},
			}, {
				{
					Schema: "test2",
					Name:   "t2",
				},
			},
		},
	}

	testCases := []struct {
		job    *job
		jobStr string
	}{
		{
			newJob(insert, "test", "t1", "test", "t1", "insert into test.t1 values(?)", []interface{}{1}, "1", mysql.Position{}, mysql.Position{}, nil, ""),
			"tp: insert, sql: insert into test.t1 values(?), args: [1], key: 1, ddls: [], last_pos: (, 0), current_pos: (, 0), gtid:<nil>",
		}, {
			newDDLJob(ddlInfo, []string{"create database test"}, mysql.Position{}, mysql.Position{}, nil, ""),
			"tp: ddl, sql: , args: [], key: , ddls: [create database test], last_pos: (, 0), current_pos: (, 0), gtid:<nil>",
		}, {
			newXIDJob(mysql.Position{}, mysql.Position{}, nil, ""),
			"tp: xid, sql: , args: [], key: , ddls: [], last_pos: (, 0), current_pos: (, 0), gtid:<nil>",
		}, {
			newFlushJob(),
			"tp: flush, sql: , args: [], key: , ddls: [], last_pos: (, 0), current_pos: (, 0), gtid:<nil>",
		}, {
			newSkipJob(mysql.Position{}, nil),
			"tp: skip, sql: , args: [], key: , ddls: [], last_pos: (, 0), current_pos: (, 0), gtid:<nil>",
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
