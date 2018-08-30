// Copyright 2017 PingCAP, Inc.
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
	"github.com/pingcap/tidb-enterprise-tools/pkg/gtid"
	"github.com/siddontang/go-mysql/mysql"
)

type opType byte

const (
	insert opType = iota + 1
	update
	del
	ddl
	xid
	flush
	skip // used by Syncer.recordSkipSQLsPos to record global pos, but not execute SQL
)

func (t opType) String() string {
	switch t {
	case insert:
		return "insert"
	case update:
		return "update"
	case del:
		return "delete"
	case ddl:
		return "ddl"
	case xid:
		return "xid"
	case flush:
		return "flush"
	case skip:
		return "skip"
	}

	return ""
}

type job struct {
	tp           opType
	sourceSchema string
	sourceTable  string
	targetSchema string
	targetTable  string
	sql          string
	args         []interface{}
	key          string
	retry        bool
	pos          mysql.Position
	gtidSet      gtid.Set
}

// TODO zxc: add RENAME TABLE support
func newJob(tp opType, sourceSchema, sourceTable, targetSchema, targetTable, sql string, args []interface{}, key string, retry bool, pos mysql.Position, gtidSet gtid.Set) *job {
	var gs gtid.Set
	if gtidSet != nil {
		gs = gtidSet.Clone()
	}
	return &job{tp: tp, sourceSchema: sourceSchema, sourceTable: sourceTable, targetSchema: targetSchema, targetTable: targetTable, sql: sql, args: args, key: key, retry: retry, pos: pos, gtidSet: gs}
}

func newXIDJob(pos mysql.Position, gtidSet gtid.Set) *job {
	var gs gtid.Set
	if gtidSet != nil {
		gs = gtidSet.Clone()
	}
	return &job{tp: xid, pos: pos, gtidSet: gs}
}

func newFlushJob() *job {
	return &job{tp: flush}
}

func newSkipJob(pos mysql.Position, gtidSet gtid.Set) *job {
	return &job{tp: skip, pos: pos, gtidSet: gtidSet}
}
