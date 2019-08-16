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
	"bytes"
	"encoding/gob"
	"fmt"
	"hash/crc32"
	"runtime"
	"sync"

	"github.com/pingcap/dm/pkg/terror"
)

// tsoHolder is used for global tso cache
type tsoGenerator struct {
	sync.RWMutex
	localTS  int64 // local start ts before last synced
	syncedTS int64 // last synced ts from tso service
}

// IDGenerator is a simple ID generator
type IDGenerator struct {
	sync.Mutex
	m map[string]int64
}

func newTsoGenerator() *tsoGenerator {
	return &tsoGenerator{}
}

// NewIDGen returns a new IDGenerator
func NewIDGen() *IDGenerator {
	gen := &IDGenerator{
		m: make(map[string]int64),
	}
	return gen
}

// NextID returns a new ID under namespace of `source`, in the name of `source`.index
func (g *IDGenerator) NextID(source string, offset int64) string {
	g.Lock()
	defer g.Unlock()
	g.m[source] += offset + 1
	return fmt.Sprintf("%s.%d", source, g.m[source])
}

// GetTraceCode returns file and line number information about function
// invocations on the calling goroutine's stack. The argument skip is the number
// of stack frames to ascend, with 0 identifying the caller of runtime.Caller.
func GetTraceCode(skip int) (string, int, error) {
	if _, file, line, ok := runtime.Caller(skip); ok {
		return file, line, nil
	}
	return "", 0, terror.ErrTracingGetTraceCode.Generate()
}

// DataChecksum calculates the checksum of a given interface{} slice. each
// interface{} is first converted to []byte via gob encoder, then combines all
// of them in sequence, at last calculate the crc32 using IEEE polynomial.
func DataChecksum(data []interface{}) (uint32, error) {
	getBytes := func(val interface{}) ([]byte, error) {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err := enc.Encode(val)
		if err != nil {
			return nil, terror.ErrTracingDataChecksum.Delegate(err)
		}
		return buf.Bytes(), nil
	}
	sumup := []byte{}
	for _, val := range data {
		if val == nil {
			val = []byte{}
		}
		b, err := getBytes(val)
		if err != nil {
			return 0, terror.ErrTracingDataChecksum.Delegate(err)
		}
		sumup = append(sumup, b...)
	}
	return crc32.ChecksumIEEE(sumup), nil
}
