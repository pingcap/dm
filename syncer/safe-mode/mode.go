package mode

import (
	"fmt"
	"sync"

	"github.com/juju/errors"
	"github.com/ngaut/log"
)

// SafeMode controls whether enable safe-mode through a mechanism similar to reference-count
// indicates enabled excepting the count is 0
type SafeMode struct {
	mu     sync.RWMutex
	count  int32
	tables map[string]struct{}
}

// NewSafeMode creates a new SafeMode instance
func NewSafeMode() *SafeMode {
	return &SafeMode{
		tables: make(map[string]struct{}),
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
	log.Infof("[safe-mode] change the count from %d to %d", prev, m.count)
	return nil
}

func key(schema, table string) string {
	if len(table) > 0 {
		return fmt.Sprintf("`%s`.`%s`", schema, table)
	}
	return fmt.Sprintf("`%s`", schema)
}
