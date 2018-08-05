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
	}

	return ""
}

type job struct {
	tp      opType
	sql     string
	args    []interface{}
	key     string
	retry   bool
	pos     mysql.Position
	gtidSet gtid.GTIDSet
}

func newJob(tp opType, sql string, args []interface{}, key string, retry bool, pos mysql.Position, gtidSet gtid.GTIDSet) *job {
	var gs gtid.GTIDSet
	if gtidSet != nil {
		gs = gtidSet.Clone()
	}
	return &job{tp: tp, sql: sql, args: args, key: key, retry: retry, pos: pos, gtidSet: gs}
}

func newXIDJob(pos mysql.Position, gtidSet gtid.GTIDSet) *job {
	var gs gtid.GTIDSet
	if gtidSet != nil {
		gs = gtidSet.Clone()
	}
	return &job{tp: xid, pos: pos, gtidSet: gs}
}

func newFlushJob() *job {
	return &job{tp: flush}
}
