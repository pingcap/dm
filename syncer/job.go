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
	"fmt"

	"github.com/siddontang/go-mysql/mysql"

	"github.com/pingcap/dm/pkg/gtid"
)

type opType byte

const (
	null opType = iota
	insert
	update
	del
	ddl
	xid
	flush
	skip // used by Syncer.recordSkipSQLsPos to record global pos, but not execute SQL
	rotate
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
	case rotate:
		return "rotate"
	}

	return ""
}

type job struct {
	tp opType
	// ddl in ShardOptimistic and ShardPessimistic will only affect one table at one time but for normal node
	// we don't have this limit. So we should update multi tables in normal mode.
	// sql example: drop table `s1`.`t1`, `s2`.`t2`.
	sourceTbl    map[string][]string
	targetSchema string
	targetTable  string
	sql          string
	args         []interface{}
	key          string
	retry        bool
	pos          mysql.Position
	currentPos   mysql.Position // exactly binlog position of current SQL
	gtidSet      gtid.Set
	ddlExecItem  *DDLExecItem
	ddls         []string
	traceID      string
	traceGID     string
}

func (j *job) String() string {
	// only output some important information, maybe useful in execution.
	return fmt.Sprintf("tp: %s, sql: %s, args: %v, key: %s, ddls: %s, last_pos: %s, current_pos: %s, gtid:%v", j.tp, j.sql, j.args, j.key, j.ddls, j.pos, j.currentPos, j.gtidSet)
}

func newJob(tp opType, sourceSchema, sourceTable, targetSchema, targetTable, sql string, args []interface{}, key string, pos, cmdPos mysql.Position, currentGtidSet gtid.Set, traceID string) *job {
	var gs gtid.Set
	if currentGtidSet != nil {
		gs = currentGtidSet.Clone()
	}
	return &job{
		tp:           tp,
		sourceTbl:    map[string][]string{sourceSchema: {sourceTable}},
		targetSchema: targetSchema,
		targetTable:  targetTable,
		sql:          sql,
		args:         args,
		key:          key,
		pos:          pos,
		currentPos:   cmdPos,
		gtidSet:      gs,
		retry:        true,
		traceID:      traceID,
	}
}

// newDDL job is used to create a new ddl job
// when cfg.ShardMode == "", ddlInfo == nil，sourceTbls != nil, we use sourceTbls to record ddl affected tables.
// when cfg.ShardMode == ShardOptimistic || ShardPessimistic, ddlInfo != nil, sourceTbls == nil.
func newDDLJob(ddlInfo *shardingDDLInfo, ddls []string, pos, cmdPos mysql.Position, currentGtidSet gtid.Set, ddlExecItem *DDLExecItem, traceID string, sourceTbls map[string]map[string]struct{}) *job {
	var gs gtid.Set
	if currentGtidSet != nil {
		gs = currentGtidSet.Clone()
	}
	j := &job{
		tp:          ddl,
		ddls:        ddls,
		pos:         pos,
		currentPos:  cmdPos,
		gtidSet:     gs,
		ddlExecItem: ddlExecItem,
		traceID:     traceID,
	}

	if ddlInfo != nil {
		j.sourceTbl = map[string][]string{ddlInfo.tableNames[0][0].Schema: {ddlInfo.tableNames[0][0].Name}}
		j.targetSchema = ddlInfo.tableNames[1][0].Schema
		j.targetTable = ddlInfo.tableNames[1][0].Name
	} else if sourceTbls != nil {
		sourceTbl := make(map[string][]string, len(sourceTbls))
		for schema, tbMap := range sourceTbls {
			if len(tbMap) > 0 {
				sourceTbl[schema] = make([]string, 0, len(tbMap))
			}
			for name := range tbMap {
				sourceTbl[schema] = append(sourceTbl[schema], name)
			}
		}
		j.sourceTbl = sourceTbl
	}

	if ddlExecItem != nil && ddlExecItem.req != nil {
		j.traceGID = ddlExecItem.req.TraceGID
	}

	return j
}

func newXIDJob(pos, cmdPos mysql.Position, currentGtidSet gtid.Set, traceID string) *job {
	var gs gtid.Set
	if currentGtidSet != nil {
		gs = currentGtidSet.Clone()
	}
	return &job{
		tp:         xid,
		pos:        pos,
		currentPos: cmdPos,
		gtidSet:    gs,
		traceID:    traceID,
	}
}

func newFlushJob() *job {
	return &job{
		tp: flush,
	}
}

func newSkipJob(pos mysql.Position, currentGtidSet gtid.Set) *job {
	return &job{
		tp:      skip,
		pos:     pos,
		gtidSet: currentGtidSet,
	}
}

// put queues into bucket to monitor them
func queueBucketName(queueID int) string {
	return fmt.Sprintf("q_%d", queueID%defaultBucketCount)
}
