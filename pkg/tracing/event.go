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
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/terror"
)

// EventType represents trace event type
type EventType byte

// EventType list
const (
	EventNull EventType = iota
	EventSyncerBinlog
	EventSyncerJob
	EventFlush // used to force upload tracing event
)

var dispatchEventType = []EventType{
	EventSyncerBinlog,
	EventSyncerJob,
}

func (e EventType) String() string {
	switch e {
	case EventNull:
		return "null"
	case EventSyncerBinlog:
		return "syncer binlog"
	case EventSyncerJob:
		return "syncer job"
	case EventFlush:
		return "flush"
	default:
		return "unknown event"
	}
}

// Job is used for tracing evnets collecting and batch uploading
type Job struct {
	Tp    EventType
	Event interface{}
}

// ProcessTraceEvents upload trace events to tracing service. ensure all jobs
// have same EventType
func (t *Tracer) ProcessTraceEvents(jobs []*Job) error {
	t.rpcWg.Add(1)
	defer t.rpcWg.Done()
	if len(jobs) == 0 {
		return nil
	}
	tp := jobs[0].Tp
	switch tp {
	case EventSyncerBinlog:
		events := make([]*pb.SyncerBinlogEvent, 0, len(jobs))
		for _, job := range jobs {
			event, ok := job.Event.(*pb.SyncerBinlogEvent)
			if !ok {
				return terror.ErrTracingEventDataNotValid.Generate(tp)
			}
			events = append(events, event)
		}
		req := &pb.UploadSyncerBinlogEventRequest{Events: events}
		resp, err := t.cli.UploadSyncerBinlogEvent(t.ctx, req)
		if err != nil {
			return terror.ErrTracingUploadData.Delegate(err)
		}
		if !resp.Result {
			return terror.ErrTracingUploadData.Generatef("upload syncer binlog event failed, msg: %s", resp.Msg)
		}
	case EventSyncerJob:
		events := make([]*pb.SyncerJobEvent, 0, len(jobs))
		for _, job := range jobs {
			event, ok := job.Event.(*pb.SyncerJobEvent)
			if !ok {
				return terror.ErrTracingEventDataNotValid.Generate(tp)
			}
			events = append(events, event)
		}
		req := &pb.UploadSyncerJobEventRequest{Events: events}
		resp, err := t.cli.UploadSyncerJobEvent(t.ctx, req)
		if err != nil {
			return terror.ErrTracingUploadData.Delegate(err)
		}
		if !resp.Result {
			return terror.ErrTracingUploadData.Generatef("upload syncer job event failed, msg: %s", resp.Msg)
		}
	default:
		return terror.ErrTracingEventTypeNotValid.Generate(tp)
	}

	return nil
}
