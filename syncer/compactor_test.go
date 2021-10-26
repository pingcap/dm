// Copyright 2021 PingCAP, Inc.
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
	"math/rand"

	. "github.com/pingcap/check"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/util/mock"

	"github.com/pingcap/dm/pkg/binlog"
	"github.com/pingcap/dm/pkg/log"
)

// mockExecute mock a kv store.
func mockExecute(kv map[interface{}][]interface{}, dmls []*DML) map[interface{}][]interface{} {
	for _, dml := range dmls {
		switch dml.op {
		case insert:
			kv[dml.values[0]] = dml.values
		case update:
			delete(kv, dml.oldValues[0])
			kv[dml.values[0]] = dml.values
		case del:
			delete(kv, dml.values[0])
		}
	}

	return kv
}

func randString(n int) string {
	letter := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = letter[rand.Intn(len(letter))]
	}
	return string(b)
}

func (s *testSyncerSuite) TestCompactJob(c *C) {
	compactor := &compactor{
		bufferSize: 10000,
		logger:     log.L(),
		keyMap:     make(map[string]map[string]int),
		buffer:     make([]*compactItem, 0, 10000),
	}

	location := binlog.NewLocation("")
	ec := &eventContext{startLocation: &location, currentLocation: &location, lastLocation: &location}
	p := parser.New()
	se := mock.NewContext()
	targetTableID := "`test`.`tb`"
	sourceTable := &filter.Table{Schema: "test", Name: "tb1"}
	targetTable := &filter.Table{Schema: "test", Name: "tb"}
	schema := "create table test.tb(id int primary key, col1 int, name varchar(24))"
	ti, err := createTableInfo(p, se, 0, schema)
	c.Assert(err, IsNil)

	var dml *DML
	var dmls []*DML
	dmlNum := 1000000
	maxID := 1000
	batch := 1000
	updateIdentifyProbability := 0.1

	// generate DMLs
	kv := make(map[interface{}][]interface{})
	for i := 0; i < dmlNum; i++ {
		newID := rand.Intn(maxID)
		newCol1 := rand.Intn(maxID * 10)
		newName := randString(rand.Intn(20))
		values := []interface{}{newID, newCol1, newName}
		oldValues, ok := kv[newID]
		if !ok {
			// insert
			dml = newDML(insert, false, targetTableID, sourceTable, nil, values, nil, values, ti.Columns, ti)
		} else {
			if rand.Int()%2 > 0 {
				// update
				// check whether to update ID
				if rand.Float64() < updateIdentifyProbability {
					for try := 0; try < 10; try++ {
						newID := rand.Intn(maxID)
						if _, ok := kv[newID]; !ok {
							values[0] = newID
							break
						}
					}
				}
				dml = newDML(update, false, targetTableID, sourceTable, oldValues, values, oldValues, values, ti.Columns, ti)
			} else {
				// delete
				dml = newDML(del, false, targetTableID, sourceTable, nil, oldValues, nil, oldValues, ti.Columns, ti)
			}
		}

		kv = mockExecute(kv, []*DML{dml})
		dmls = append(dmls, dml)
	}

	kv = make(map[interface{}][]interface{})
	compactKV := make(map[interface{}][]interface{})

	// mock compactJob
	for i := 0; i < len(dmls); i += batch {
		end := i + batch
		if end > len(dmls) {
			end = len(dmls)
		}
		kv = mockExecute(kv, dmls[i:end])

		for _, dml := range dmls[i:end] {
			j := newDMLJob(dml.op, sourceTable, targetTable, dml, ec)
			if j.dml.op == update && j.dml.updateIdentify() {
				delDML, insertDML := updateToDelAndInsert(j.dml)
				delJob := j.clone()
				delJob.tp = del
				delJob.dml = delDML

				insertJob := j.clone()
				insertJob.tp = insert
				insertJob.dml = insertDML

				compactor.compactJob(delJob)
				compactor.compactJob(insertJob)
			} else {
				compactor.compactJob(j)
			}
		}

		noCompactNumber := end - i
		compactNumber := 0
		for _, dml := range dmls[i:end] {
			c.Logf("before compact, dml: %s", dml.String())
		}
		for _, compactItem := range compactor.buffer {
			if !compactItem.compacted {
				compactKV = mockExecute(compactKV, []*DML{compactItem.j.dml})
				compactNumber++
				c.Logf("after compact, dml: %s", compactItem.j.dml.String())
			}
		}
		c.Logf("before compcat: %d, after compact: %d", noCompactNumber, compactNumber)
		c.Assert(compactKV, DeepEquals, kv)
		compactor.keyMap = make(map[string]map[string]int)
		compactor.buffer = compactor.buffer[0:0]
	}
}
