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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/tidb/util/mock"

	"github.com/pingcap/dm/pkg/binlog"
	"github.com/pingcap/dm/pkg/utils"
)

func (s *testSyncerSuite) TestDetectConflict(c *C) {
	ca := &Causality{
		relations: make(map[string]string),
	}
	caseData := []string{"test_1", "test_2", "test_3"}
	excepted := map[string]string{
		"test_1": "test_1",
		"test_2": "test_1",
		"test_3": "test_1",
	}
	c.Assert(ca.detectConflict(caseData), IsFalse)
	ca.add(caseData)
	c.Assert(ca.relations, DeepEquals, excepted)
	c.Assert(ca.detectConflict([]string{"test_4"}), IsFalse)
	ca.add([]string{"test_4"})
	excepted["test_4"] = "test_4"
	c.Assert(ca.relations, DeepEquals, excepted)
	conflictData := []string{"test_4", "test_3"}
	c.Assert(ca.detectConflict(conflictData), IsTrue)
	ca.reset()
	c.Assert(ca.relations, HasLen, 0)
}

func (s *testSyncerSuite) TestCasuality(c *C) {
	p := parser.New()
	se := mock.NewContext()
	schema := "create table tb(a int primary key, b int unique);"
	ti, err := createTableInfo(p, se, int64(0), schema)
	c.Assert(err, IsNil)

	location := binlog.NewLocation("")
	jobCh := make(chan *job, 10)
	causality := RunCausality(1024, "task", "source", jobCh)
	testCases := []struct {
		op   opType
		vals [][]interface{}
	}{
		{
			op:   insert,
			vals: [][]interface{}{{1, 2}},
		},
		{
			op:   insert,
			vals: [][]interface{}{{2, 3}},
		},
		{
			op:   update,
			vals: [][]interface{}{{2, 3}, {4, 3}},
		},
		{
			op:   del,
			vals: [][]interface{}{{1, 2}},
		},
		{
			op:   insert,
			vals: [][]interface{}{{1, 3}},
		},
	}
	results := []opType{insert, insert, update, del, conflict, insert}

	for _, tc := range testCases {
		var keys []string
		for _, val := range tc.vals {
			keys = append(keys, genMultipleKeys(ti, val, "tb")...)
		}
		job := newDMLJob(tc.op, "", "", "", "", "", nil, keys, location, location, location, nil)
		jobCh <- job
	}

	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return len(causality.CausalityCh) == len(results)
	}), IsTrue)

	for _, op := range results {
		job := <-causality.CausalityCh
		c.Assert(job.tp, Equals, op)
	}
}
