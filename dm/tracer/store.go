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

package tracer

import (
	"encoding/json"

	"github.com/pingcap/errors"

	"github.com/pingcap/dm/dm/pb"
)

// TraceEvent represents a sigle tracing event
type TraceEvent struct {
	Type  pb.TraceType `json:"type"`
	Event interface{}  `json:"event"`
}

func (e *TraceEvent) jsonify() (string, error) {
	switch e.Type {
	case pb.TraceType_BinlogEvent:
		sbe, ok := e.Event.(*pb.SyncerBinlogEvent)
		if !ok {
			return "", errors.NotValidf("trace event data")
		}
		js, err := json.MarshalIndent(sbe, "", " ")
		if err != nil {
			return "", errors.Trace(err)
		}
		return string(js), nil
	default:
		return "", errors.NotValidf("trace event type %d", e.Type)
	}
}

// EventStore stores all tracing events, mapping from TraceID -> a list of TraceEvent
// TraceEvents with the same TraceID can have different TraceType
type EventStore struct {
	events map[string][]*TraceEvent
}

// NewEventStore creates a new EventStore
func NewEventStore() *EventStore {
	return &EventStore{
		events: make(map[string][]*TraceEvent),
	}
}

func (store *EventStore) addNewEvent(e *TraceEvent) error {
	var traceID string
	switch e.Type {
	case pb.TraceType_BinlogEvent:
		sbe, ok := e.Event.(*pb.SyncerBinlogEvent)
		if !ok {
			return errors.NotValidf("trace event data")
		}
		traceID = sbe.Base.TraceID
	default:
		return errors.NotValidf("trace event type %d", e.Type)
	}

	traceEvents, ok := store.events[traceID]
	if !ok {
		store.events[traceID] = []*TraceEvent{e}
	} else {
		traceEvents = append(traceEvents, e)
	}

	return nil
}

// queryByTraceID queries trace event by given traceID
func (store *EventStore) queryByTraceID(traceID string) []*TraceEvent {
	return store.events[traceID]
}
