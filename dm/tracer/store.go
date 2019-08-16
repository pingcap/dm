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
	"sync"

	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/terror"
)

// TraceEvent represents a sigle tracing event
type TraceEvent struct {
	Type  pb.TraceType `json:"type"`
	Event interface{}  `json:"event"`
}

// EventStore stores all tracing events, mapping from TraceID -> a list of TraceEvent
// TraceEvents with the same TraceID can have different TraceType
// NOTE: this is a quick and dirty implement, we will refactor it later
type EventStore struct {
	sync.RWMutex
	ids    []string
	events map[string][]*TraceEvent
}

// NewEventStore creates a new EventStore
func NewEventStore() *EventStore {
	return &EventStore{
		ids:    make([]string, 0),
		events: make(map[string][]*TraceEvent),
	}
}

func (store *EventStore) addNewEvent(e *TraceEvent) error {
	var traceID string
	switch e.Type {
	case pb.TraceType_BinlogEvent:
		ev, ok := e.Event.(*pb.SyncerBinlogEvent)
		if !ok {
			return terror.ErrTracerEventAssertionFail.Generate(e.Type, e.Event)
		}
		traceID = ev.Base.TraceID
	case pb.TraceType_JobEvent:
		ev, ok := e.Event.(*pb.SyncerJobEvent)
		if !ok {
			return terror.ErrTracerEventAssertionFail.Generate(e.Type, e.Event)
		}
		traceID = ev.Base.TraceID
	default:
		return terror.ErrTracerEventTypeNotValid.Generate(e.Type)
	}

	store.Lock()
	_, ok := store.events[traceID]
	if !ok {
		store.ids = append(store.ids, traceID)
		store.events[traceID] = make([]*TraceEvent, 0)
	}
	store.events[traceID] = append(store.events[traceID], e)
	store.Unlock()

	return nil
}

// queryByTraceID queries trace event by given traceID
func (store *EventStore) queryByTraceID(traceID string) []*TraceEvent {
	store.RLock()
	defer store.RUnlock()
	// ensure read only
	return store.events[traceID]
}

func (store *EventStore) scan(offset, limit int64) [][]*TraceEvent {
	store.RLock()
	defer store.RUnlock()
	if offset > int64(len(store.ids)) {
		return nil
	}
	result := make([][]*TraceEvent, 0, limit)
	end := offset + limit
	if end > int64(len(store.ids)) {
		end = int64(len(store.ids))
	}
	for idx := offset; idx < end; idx++ {
		traceID := store.ids[idx]
		if event, ok := store.events[traceID]; ok {
			result = append(result, event)
		} else {
			// TODO: add lazy clean up mechanism
		}
	}
	// ensure read only
	return result
}

func (store *EventStore) removeByTraceID(traceID string) (removed bool) {
	store.Lock()
	defer store.Unlock()
	if _, ok := store.events[traceID]; ok {
		delete(store.events, traceID)
		removed = true
		for idx := range store.ids {
			if store.ids[idx] == traceID {
				store.ids = append(store.ids[:idx], store.ids[idx+1:]...)
				break
			}
		}
	}
	return
}

func (store *EventStore) truncate() {
	store.Lock()
	defer store.Unlock()
	store.events = make(map[string][]*TraceEvent)
	store.ids = make([]string, 0)
}
