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

package mode

import (
	"fmt"
	"sync"

	"github.com/pingcap/errors"
	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/log"
)

// SafeMode controls whether enable safe-mode through a mechanism similar to reference-count
// indicates enabled excepting the count is 0
type SafeMode struct {
	mu     sync.RWMutex
	count  int32
	tables map[string]struct{}

	logger log.Logger
}

// NewSafeMode creates a new SafeMode instance
func NewSafeMode(logger log.Logger) *SafeMode {
	return &SafeMode{
		tables: make(map[string]struct{}),
		logger: logger,
	}
}

// Add adds n to the count, n can be negative
func (m *SafeMode) Add(n int32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return errors.Trace(m.setCount(m.count + n))
}

// IncrForTable tries to add 1 on the count if the table not added before
// can only be desc with DescForTable
func (m *SafeMode) IncrForTable(schema, table string) error {
	key := key(schema, table)

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tables[key]; !ok {
		m.tables[key] = struct{}{}
		return errors.Trace(m.setCount(m.count + 1))
	}
	return nil
}

// DescForTable tries to add -1 on the count if the table added before
func (m *SafeMode) DescForTable(schema, table string) error {
	key := key(schema, table)

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tables[key]; ok {
		delete(m.tables, key)
		return errors.Trace(m.setCount(m.count - 1))
	}
	return nil
}

// Reset resets to the state of not-enable
func (m *SafeMode) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.setCount(0)
	m.tables = make(map[string]struct{})
}

// Enable returns whether is enabled currently
func (m *SafeMode) Enable() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.count != 0
}

// setCount sets the count, called internal
func (m *SafeMode) setCount(n int32) error {
	if n < 0 {
		return errors.NotValidf("set negative count (%d) for safe-mode", m.count)
	}

	prev := m.count
	m.count = n
	m.logger.Info("change count", zap.Int32("previous count", prev), zap.Int32("new count", m.count))
	return nil
}

func key(schema, table string) string {
	if len(table) > 0 {
		return fmt.Sprintf("`%s`.`%s`", schema, table)
	}
	return fmt.Sprintf("`%s`", schema)
}
