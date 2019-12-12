package master

import (
	"github.com/pingcap/dm/dm/config"
	"sync"
)

// Worker is created for mysql task
type Worker interface {
	CreateMysqlTask(*config.WorkerConfig) error
	UpdateMysqlConfig(*config.WorkerConfig) error
	StopMysqlTask(string) error
}

// MysqlManager control mysql tasks running
type MysqlManager struct {
	configs    map[string]config.WorkerConfig
	workers    map[string]Worker
	scheduleCh chan string
	sync.Mutex
}

// NewMysqlManager creates new MysqlManager
func NewMysqlManager(ch chan string) *MysqlManager {
	m := MysqlManager{
		workers:    make(map[string]Worker),
		configs:    make(map[string]config.WorkerConfig),
		scheduleCh: ch,
	}
	return &m
}

// GetWorker is to get existed worker for mysql
func (m *MysqlManager) GetWorker(name string) Worker {
	m.Lock()
	defer m.Unlock()
	if w, ok := m.workers[name]; ok {
		return w
	}
	return nil
}

// GetWorkerConfig is to get existed WorkerConfig for mysql's source id
func (m *MysqlManager) GetWorkerConfig(name string) config.WorkerConfig {
	m.Lock()
	defer m.Unlock()
	c, _ := m.configs[name]
	return c
}

// ScheduleWorker is to create mysql task in a idle server
func (m *MysqlManager) ScheduleWorker(c *config.WorkerConfig) {
	m.Lock()
	defer m.Unlock()
	m.configs[c.SourceID] = *c
	m.scheduleCh <- c.SourceID
}

// AddWorker is to add worker for a server which has been running mysql task
func (m *MysqlManager) AddWorker(name string, w Worker) {
	m.Lock()
	defer m.Unlock()
	m.workers[name] = w
}
