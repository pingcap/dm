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

package unit

import (
	"sync"

	"github.com/pingcap/dm/pkg/log"
)

// RollbackFunc records function used to rolling back some operations.
// It is currently used by units' Init to release resources when failing in the half-way.
type RollbackFunc struct {
	Name string
	Fn   func()
}

// RollbackFuncHolder holds some RollbackFuncs.
type RollbackFuncHolder struct {
	mu   sync.Mutex
	name string // used to make log clearer
	fns  []RollbackFunc
}

// NewRollbackHolder creates a new RollbackFuncHolder instance.
func NewRollbackHolder(name string) *RollbackFuncHolder {
	return &RollbackFuncHolder{
		name: name,
		fns:  make([]RollbackFunc, 0),
	}
}

// Add adds a func to the holder.
func (h *RollbackFuncHolder) Add(fn RollbackFunc) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.fns = append(h.fns, fn)
}

// RollbackReverseOrder executes rollback functions in reverse order
func (h *RollbackFuncHolder) RollbackReverseOrder() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := len(h.fns) - 1; i >= 0; i-- {
		fn := h.fns[i]
		log.Infof("[%s] rolling back %s", h.name, fn.Name)
		fn.Fn()
	}
}
