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

func (t *Tracer) CollectSyncerBinlogEvent(source string, safeMode, tryReSync bool, globalPos, currentPos mysql.Position, eventType, opType int32) error {
	file, line, err := GetTraceCode(2)
	if err != nil {
		return errors.Trace(err)
	}
	tso := t.GetTSO()
	traceID := t.GetTraceID(source)
	base := &pb.BaseEvent{
		Filename: file,
		Line:     int32(line),
		Tso:      tso,
		TraceID:  traceID,
		Type:     pb.TraceType_BinlogEvent,
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

	return nil
}
