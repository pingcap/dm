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
