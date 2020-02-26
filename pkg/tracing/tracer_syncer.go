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
	"github.com/siddontang/go-mysql/mysql"

	"github.com/pingcap/dm/dm/pb"
)

func convertMySQLPos(pos mysql.Position) *pb.MySQLPosition {
	return &pb.MySQLPosition{
		Name: pos.Name,
		Pos:  pos.Pos,
	}
}

// CollectSyncerBinlogEvent collects syncer binlog event and returns the trace event traceID
func (t *Tracer) CollectSyncerBinlogEvent(source string, safeMode, tryReSync bool, globalPos, currentPos mysql.Position, eventType, opType int32) (*pb.SyncerBinlogEvent, error) {
	base, err := t.collectBaseEvent(source, "", "", pb.TraceType_BinlogEvent)
	if err != nil {
		return nil, err
	}
	syncerState := &pb.SyncerState{
		SafeMode:   safeMode,
		TryReSync:  tryReSync,
		LastPos:    convertMySQLPos(globalPos),
		CurrentPos: convertMySQLPos(currentPos),
	}

	event := &pb.SyncerBinlogEvent{
		Base:      base,
		State:     syncerState,
		EventType: eventType,
		OpType:    opType,
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
func (t *Tracer) CollectSyncerJobEvent(traceID string, traceGID string, opType int32, pos, currentPos mysql.Position, queueBucket, sql string, ddls []string, args []interface{}, state pb.SyncerJobState) (*pb.SyncerJobEvent, error) {
	base, err := t.collectBaseEvent("", traceID, traceGID, pb.TraceType_JobEvent)
	if err != nil {
		return nil, err
	}
	event := &pb.SyncerJobEvent{
		Base:        base,
		OpType:      opType,
		Pos:         convertMySQLPos(pos),
		CurrentPos:  convertMySQLPos(currentPos),
		Sql:         sql,
		Ddls:        ddls,
		QueueBucket: queueBucket,
		State:       state,
	}
	// only calculate args check if enabled in config, and in queued event
	if t.cfg.Checksum && args != nil {
		event.ArgsChecksum, _ = DataChecksum(args)
	}
	job := &Job{
		Tp:    EventSyncerJob,
		Event: event,
	}
	t.AddJob(job)

	return event, nil
}
