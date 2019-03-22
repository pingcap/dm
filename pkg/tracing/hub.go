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
	"sync"
)

var (
	tracerHub *TracerHub
	once      sync.Once
)

// TracerHub holds a tracer
type TracerHub struct {
	tracer *Tracer
}

// InitTracerHub inits the singleton instance of TracerHub
func InitTracerHub(cfg Config) *Tracer {
	once.Do(func() {
		tracerHub = &TracerHub{
			tracer: NewTracer(cfg),
		}
	})
	return tracerHub.tracer
}

// GetTracer returns the tracer instance. If InitTracerHub is not called,
// returns a default disabled tracer.
func GetTracer() *Tracer {
	if tracerHub == nil {
		return NewTracer(Config{})
	}
	return tracerHub.tracer
}
