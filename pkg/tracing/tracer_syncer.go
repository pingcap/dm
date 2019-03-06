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

package tracing

import (
	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/mysql"

	"github.com/pingcap/dm/dm/pb"
)

// CollectSyncerBinlogEvent collects syncer binlog event and returns the trace event traceID
func (t *Tracer) CollectSyncerBinlogEvent(source string, safeMode, tryReSync bool, globalPos, currentPos mysql.Position, eventType, opType int32) (*pb.SyncerBinlogEvent, error) {
	base, err := t.collectBaseEvent(source, "", "", pb.TraceType_BinlogEvent)
	if err != nil {
		return nil, errors.Trace(err)
	}
	syncerState := &pb.SyncerState{
		SafeMode:  safeMode,
		TryReSync: tryReSync,
		LastPos: &pb.MySQLPosition{
			Name: globalPos.Name,
			Pos:  globalPos.Pos,
		},
	}
	currentPos2 := &pb.MySQLPosition{
		Name: currentPos.Name,
		Pos:  currentPos.Pos,
	}

	event := &pb.SyncerBinlogEvent{
		Base:       base,
		State:      syncerState,
		EventType:  eventType,
		OpType:     opType,
		CurrentPos: currentPos2,
	}

	job := &Job{
		Tp:    EventSyncerBinlog,
		Event: event,
	}
	t.AddJob(job)

	return event, nil
}

// FinishedSyncerJobState returns pb.SyncerJobState according to given error
func (t *Tracer) FinishedSyncerJobState(err error) pb.SyncerJobState {
	if err != nil {
		return pb.SyncerJobState_error
	}
	return pb.SyncerJobState_success
}

// CollectSyncerJobEvent collects syncer job event and returns traceID
func (t *Tracer) CollectSyncerJobEvent(traceID string, traceGID string, opType int32, pos, currentPos mysql.Position, queueBucket, sql string, ddls []string, req *pb.ExecDDLRequest, state pb.SyncerJobState) (*pb.SyncerJobEvent, error) {
	base, err := t.collectBaseEvent("", traceID, traceGID, pb.TraceType_JobEvent)
	if err != nil {
		return nil, errors.Trace(err)
	}
	pos2 := &pb.MySQLPosition{
		Name: pos.Name,
		Pos:  pos.Pos,
	}
	currentPos2 := &pb.MySQLPosition{
		Name: currentPos.Name,
		Pos:  currentPos.Pos,
	}
	event := &pb.SyncerJobEvent{
		Base:        base,
		OpType:      opType,
		Pos:         pos2,
		CurrentPos:  currentPos2,
		Sql:         sql,
		Ddls:        ddls,
		QueueBucket: queueBucket,
		State:       state,
	}
	if req != nil {
		event.DdlInfo = &pb.ExecDDLInfo{
			LockID: req.LockID,
			Exec:   req.Exec,
		}
	}
	job := &Job{
		Tp:    EventSyncerJob,
		Event: event,
	}
	t.AddJob(job)

	return event, nil
}
