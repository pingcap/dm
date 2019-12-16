package master

import (
	"context"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/master/workerrpc"
	"github.com/pingcap/dm/dm/pb"
	"sync"
	"time"
)

// Worker is created for mysql task
type Worker interface {
	CreateMysqlTask(ctx context.Context, c *config.WorkerConfig, d time.Duration) (*pb.MysqlTaskResponse, error)
	UpdateMysqlConfig(ctx context.Context, c *config.WorkerConfig, d time.Duration) (*pb.MysqlTaskResponse, error)
	StopMysqlTask(ctx context.Context, c string, d time.Duration) (*pb.MysqlTaskResponse, error)
	Address() string
}

// MysqlManager control mysql tasks running
type MysqlManager struct {
	configs    map[string]config.WorkerConfig
	workers    map[string]Worker
	scheduleCh chan string
	idleWorker map[string]Worker
	sync.Mutex
}

// NewMysqlManager creates new MysqlManager
func NewMysqlManager(ch chan string) *MysqlManager {
	m := MysqlManager{
		workers:    make(map[string]Worker),
		configs:    make(map[string]config.WorkerConfig),
		idleWorker: make(map[string]Worker),
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

// ScheduleMysqlWorker is to create mysql task in a idle server
func (m *MysqlManager) ScheduleMysqlWorker(ctx context.Context, c *config.WorkerConfig, d time.Duration) (*pb.MysqlTaskResponse, error) {
	m.Lock()
	defer m.Unlock()
	if len(m.idleWorker) > 0 {
		for k, mockWorker := range m.idleWorker {
			resp, err := mockWorker.CreateMysqlTask(ctx, c, d)
			if err != nil {
				return nil, err
			}
			if resp.Result {
				delete(m.idleWorker, k)
				m.workers[c.SourceID] = mockWorker
				m.configs[c.SourceID] = *c
			}
			return resp, nil
		}
	}
	return nil, nil
}

// StopMysqlWorker is to stop mysql task in a server
func (m *MysqlManager) StopMysqlWorker(ctx context.Context, c *config.WorkerConfig, d time.Duration) (*pb.MysqlTaskResponse, error) {
	m.Lock()
	defer m.Unlock()
	if w, ok := m.workers[c.SourceID]; ok {
		resp, err := w.StopMysqlTask(ctx, c.SourceID, d)
		if err == nil && resp != nil {
			delete(m.workers, c.SourceID)
			delete(m.configs, c.SourceID)
			m.idleWorker[w.Address()] = w
			return resp, nil
		}
		return resp, err
	}
	return nil, nil
}

// AddWorker is to add worker for a server which has been running mysql task
func (m *MysqlManager) AddWorker(name string, w Worker) {
	m.Lock()
	defer m.Unlock()
	m.idleWorker[name] = w
}

// MockWorker is created for mysql task
type MockWorker struct {
	adress string
	client workerrpc.Client
}

// CreateMysqlTask in a idle worker
func (w *MockWorker) CreateMysqlTask(ctx context.Context, c *config.WorkerConfig, d time.Duration) (*pb.MysqlTaskResponse, error) {
	content, err := c.Toml()
	if err != nil {
		return nil, err
	}
	ownerReq := &workerrpc.Request{
		Type: workerrpc.CmdCreateMysqlWorker,
		MysqlWorkerRequest: &pb.MysqlTaskRequest{
			Config: content,
		},
	}
	resp, err := w.client.SendRequest(ctx, ownerReq, d)
	return resp.MysqlWorker, err
}

// UpdateMysqlConfig update mysql config in worker
func (w *MockWorker) UpdateMysqlConfig(ctx context.Context, c *config.WorkerConfig, d time.Duration) (*pb.MysqlTaskResponse, error) {
	content, err := c.Toml()
	if err != nil {
		return nil, err
	}
	ownerReq := &workerrpc.Request{
		Type: workerrpc.CmdUpdateMysqlConfig,
		MysqlWorkerRequest: &pb.MysqlTaskRequest{
			Config: content,
		},
	}
	resp, err := w.client.SendRequest(ctx, ownerReq, d)
	return resp.MysqlWorker, err
}

// StopMysqlTask update mysql config in worker
func (w *MockWorker) StopMysqlTask(ctx context.Context, sourceID string, d time.Duration) (*pb.MysqlTaskResponse, error) {
	ownerReq := &workerrpc.Request{
		Type: workerrpc.CmdStopMysqlWorker,
		StopMysqlWorker: &pb.StopMysqlTaskRequest{
			SourceID: sourceID,
		},
	}
	resp, err := w.client.SendRequest(ctx, ownerReq, d)
	return resp.MysqlWorker, err
}

// Address return address of worker
func (w *MockWorker) Address() string {
	return w.adress
}
