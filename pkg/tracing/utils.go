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
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/pingcap/errors"
)

// tsoHolder is used for global tso cache
type tsoGenerator struct {
	sync.RWMutex
	localTS  int64 // local start ts before last synced
	syncedTS int64 // last synced ts from tso service
}

// idGenerator is a trace ID generator
type idGenerator struct {
	sync.Mutex
	m map[string]int64
}

func newTsoGenerator() *tsoGenerator {
	return &tsoGenerator{}
}

// newIDGen returns a new idGenerator
func newIDGen() *idGenerator {
	gen := &idGenerator{
		m: make(map[string]int64),
	}
	return gen
}

// nextTraceID returns a new TraceID under namespace of `source`, in the name of `source`-index
func (g *idGenerator) nextTraceID(source string, offset int64) string {
	g.Lock()
	defer g.Unlock()
	g.m[source] += offset + 1
	return fmt.Sprintf("%s.%d", source, g.m[source])
}

// GetTSO returns current tso
func (t *Tracer) GetTSO() int64 {
	t.tso.RLock()
	defer t.tso.RUnlock()
	currentTS := time.Now().UnixNano()
	return t.tso.syncedTS + currentTS - t.tso.localTS
}

// GetTraceID returns a new traceID for tracing
func (t *Tracer) GetTraceID(source string) string {
	return t.idGen.nextTraceID(source, 0)
}

// GetTraceCode returns file and line number information about function
// invocations on the calling goroutine's stack. The argument skip is the number
// of stack frames to ascend, with 0 identifying the caller of runtime.Caller.
func GetTraceCode(skip int) (string, int, error) {
	if _, file, line, ok := runtime.Caller(skip); ok {
		return file, line, nil
	}
	return "", 0, errors.New("failed to get code information from runtime.Caller")
}
