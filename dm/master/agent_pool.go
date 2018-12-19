package master

import (
	"sync"
)

var (
	pool       *AgentPool // singleton instance
	once       sync.Once
	agentlimit = 20
)

// AgentPool is a pool to control communication with dm-workers
// caller shouldn't to hold agent to avoid deadlock
type AgentPool struct {
	limit  int
	agents chan *Agent
}

// Agent communicate with dm-workers
type Agent struct {
	ID int
}

// NewAgentPool returns a agent pool
func NewAgentPool(limit int) *AgentPool {
	agents := make(chan *Agent, limit)
	for i := 0; i < limit; i++ {
		agents <- &Agent{ID: i + 1}
	}

	return &AgentPool{
		limit:  limit,
		agents: agents,
	}
}

// Apply applies for a agent
func (pool *AgentPool) Apply() *Agent {
	agent := <-pool.agents
	return agent
}

// Recycle recycles agent
func (pool *AgentPool) Recycle(agent *Agent) {
	pool.agents <- agent
}

// GetAgentPool a singleton agent pool
func GetAgentPool() *AgentPool {
	once.Do(func() {
		pool = NewAgentPool(agentlimit)
	})
	return pool
}

// Emit apply for a agent to communicates with dm-worker
func Emit(fn func(args ...interface{}), args ...interface{}) {
	ap := GetAgentPool()
	agent := ap.Apply()
	defer ap.Recycle(agent)

	fn(args...)
}
