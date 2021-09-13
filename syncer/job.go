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
	"time"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/pingcap/dm/pkg/binlog"
	"github.com/pingcap/tidb-tools/pkg/filter"
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
	skip // used by Syncer.recordSkipSQLsLocation to record global location, but not execute SQL
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
	sourceTbls      map[string][]string
	targetTable     *filter.Table
	sql             string
	args            []interface{}
	key             string
	retry           bool
	location        binlog.Location // location of last received (ROTATE / QUERY / XID) event, for global/table checkpoint
	startLocation   binlog.Location // start location of the sql in binlog, for handle_error
	currentLocation binlog.Location // end location of the sql in binlog, for user to skip sql manually by changing checkpoint
	ddls            []string
	originSQL       string // show origin sql when error, only DDL now

	eventHeader *replication.EventHeader
	jobAddTime  time.Time // job commit time
}

func (j *job) String() string {
	// only output some important information, maybe useful in execution.
	return fmt.Sprintf("tp: %s, sql: %s, args: %v, key: %s, ddls: %s, last_location: %s, start_location: %s, current_location: %s", j.tp, j.sql, j.args, j.key, j.ddls, j.location, j.startLocation, j.currentLocation)
}

func newDMLJob(tp opType, sql string, sourceTable, targetTable *filter.Table, args []interface{},
	key string, location, startLocation, cmdLocation binlog.Location, eventHeader *replication.EventHeader) *job {
	return &job{
		tp:          tp,
		sourceTbls:  map[string][]string{sourceTable.Schema: {sourceTable.Name}},
		targetTable: targetTable,
		sql:         sql,
		args:        args,
		key:         key,
		retry:       true,

		location:        location,
		startLocation:   startLocation,
		currentLocation: cmdLocation,
		eventHeader:     eventHeader,
		jobAddTime:      time.Now(),
	}
}

// newDDL job is used to create a new ddl job
// when cfg.ShardMode == "", ddlInfo == nil，sourceTbls != nil, we use sourceTbls to record ddl affected tables.
// when cfg.ShardMode == ShardOptimistic || ShardPessimistic, ddlInfo != nil, sourceTbls == nil.
func newDDLJob(ddlInfo *shardingDDLInfo, ddls []string, location, startLocation, cmdLocation binlog.Location,
	sourceTbls map[string]map[string]struct{}, originSQL string, eventHeader *replication.EventHeader) *job {
	j := &job{
		tp:        ddl,
		ddls:      ddls,
		originSQL: originSQL,

		location:        location,
		startLocation:   startLocation,
		currentLocation: cmdLocation,
		eventHeader:     eventHeader,
		jobAddTime:      time.Now(),
	}

	if ddlInfo != nil {
		j.sourceTbls = map[string][]string{ddlInfo.tables[0][0].Schema: {ddlInfo.tables[0][0].Name}}
		j.targetTable = ddlInfo.tables[1][0]
	} else if sourceTbls != nil {
		j.sourceTbls = make(map[string][]string, len(sourceTbls))
		for schema, tbMap := range sourceTbls {
			if len(tbMap) > 0 {
				j.sourceTbls[schema] = make([]string, 0, len(tbMap))
			}
			for name := range tbMap {
				j.sourceTbls[schema] = append(j.sourceTbls[schema], name)
			}
		}
	}

	return j
}

func newSkipJob(ec *eventContext) *job {
	return &job{
		tp:          skip,
		location:    *ec.lastLocation,
		eventHeader: ec.header,
		jobAddTime:  time.Now(),
	}
}

func newXIDJob(location, startLocation, currentLocation binlog.Location) *job {
	return &job{
		tp:              xid,
		location:        location,
		startLocation:   startLocation,
		currentLocation: currentLocation,
		jobAddTime:      time.Now(),
	}
}

func newFlushJob() *job {
	return &job{
		tp:         flush,
		jobAddTime: time.Now(),
	}
}

// put queues into bucket to monitor them.
func queueBucketName(queueID int) string {
	return fmt.Sprintf("q_%d", queueID%defaultBucketCount)
}

func dmlWorkerJobIdx(queueID int) int {
	return queueID + workerJobTSArrayInitSize
}

func dmlWorkerJobIdxToQueueID(idx int) int {
	return idx - workerJobTSArrayInitSize
}
