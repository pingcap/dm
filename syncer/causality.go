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

package syncer

import (
	"time"

	"github.com/pingcap/dm/syncer/metrics"
)

// Causality provides a simple mechanism to improve the concurrency of SQLs execution under the premise of ensuring correctness.
// causality groups sqls that maybe contain causal relationships, and syncer executes them linearly.
// if some conflicts exist in more than one groups, then syncer waits all SQLs that are grouped be executed and reset causality.
// this mechanism meets quiescent consistency to ensure correctness.
type Causality struct {
	relations   map[string]string
	CausalityCh chan *job
	in          chan *job

	// for metrics
	task   string
	source string
}

// RunCausality creates and runs causality.
func RunCausality(chanSize int, task, source string, in chan *job) *Causality {
	causality := &Causality{
		relations:   make(map[string]string),
		CausalityCh: make(chan *job, chanSize),
		in:          in,
		task:        task,
		source:      source,
	}
	go causality.run()
	return causality
}

func (c *Causality) add(keys []string) {
	if len(keys) == 0 {
		return
	}

	// find causal key
	selectedRelation := keys[0]
	var nonExistKeys []string
	for _, key := range keys {
		if val, ok := c.relations[key]; ok {
			selectedRelation = val
		} else {
			nonExistKeys = append(nonExistKeys, key)
		}
	}
	// set causal relations for those non-exist keys
	for _, key := range nonExistKeys {
		c.relations[key] = selectedRelation
	}
}

func (c *Causality) run() {
	defer c.close()

	for j := range c.in {
		startTime := time.Now()
		if j.tp != flush {
			if c.detectConflict(j.keys) {
				// s.tctx.L().Debug("meet causality key, will generate a flush job and wait all sqls executed", zap.Strings("keys", j.keys))
				c.CausalityCh <- newCausalityJob()
				c.reset()
			}
			c.add(j.keys)
			j.key = c.get(j.key)
			// s.tctx.L().Debug("key for keys", zap.String("key", j.key), zap.Strings("keys", j.keys))
		}
		metrics.ConflictDetectDurationHistogram.WithLabelValues(c.task, c.source).Observe(time.Since(startTime).Seconds())

		c.CausalityCh <- j
	}
}

func (c *Causality) close() {
	close(c.CausalityCh)
}

func (c *Causality) get(key string) string {
	return c.relations[key]
}

func (c *Causality) reset() {
	c.relations = make(map[string]string)
}

// detectConflict detects whether there is a conflict.
func (c *Causality) detectConflict(keys []string) bool {
	if len(keys) == 0 {
		return false
	}

	var existedRelation string
	for _, key := range keys {
		if val, ok := c.relations[key]; ok {
			if existedRelation != "" && val != existedRelation {
				return true
			}
			existedRelation = val
		}
	}

	return false
}
