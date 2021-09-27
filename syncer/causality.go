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
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/syncer/metrics"
)

// Causality provides a simple mechanism to improve the concurrency of SQLs execution under the premise of ensuring correctness.
// causality groups sqls that maybe contain causal relationships, and syncer executes them linearly.
// if some conflicts exist in more than one groups, causality generate a conflict job and reset.
// this mechanism meets quiescent consistency to ensure correctness.
type Causality struct {
	relations        map[string]string
	causalityCh      chan *job
	in               chan *job
	logger           log.Logger
	chanSize         int
	disableCausality bool

	// for metrics
	task   string
	source string
}

// newCausality creates a causality instance.
func newCausality(disableCausality bool, chanSize int, task, source string, pLogger *log.Logger) *Causality {
	causality := &Causality{
		relations:        make(map[string]string),
		disableCausality: disableCausality,
		task:             task,
		chanSize:         chanSize,
		source:           source,
		logger:           pLogger.WithFields(zap.String("component", "causality")),
	}
	return causality
}

// run runs a causality instance.
func (c *Causality) run(in chan *job) chan *job {
	c.in = in
	c.causalityCh = make(chan *job, c.chanSize)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.runCausality()
	}()

	go func() {
		defer c.close()
		wg.Wait()
	}()
	return c.causalityCh
}

// runCausality receives dml jobs and returns causality jobs
// When meet conflict, returns a conflict job.
func (c *Causality) runCausality() {
	for j := range c.in {
		metrics.QueueSizeGauge.WithLabelValues(c.task, "causality_input", c.source).Set(float64(len(c.in)))

		startTime := time.Now()
		if j.tp != flush {
			keys := j.keys
			var key string
			if len(keys) > 0 {
				key = keys[0]
			}

			if c.disableCausality {
				j.key = key
			} else {
				// detectConflict before add
				if c.detectConflict(keys) {
					c.logger.Debug("meet causality key, will generate a conflict job to flush all sqls", zap.Strings("keys", keys))
					c.causalityCh <- newCausalityJob()
					c.reset()
				}
				c.add(keys)
				j.key = c.get(key)
			}
			c.logger.Debug("key for keys", zap.String("key", key), zap.Strings("keys", keys))
		} else if !c.disableCausality {
			c.reset()
		}
		metrics.ConflictDetectDurationHistogram.WithLabelValues(c.task, c.source).Observe(time.Since(startTime).Seconds())

		c.causalityCh <- j
	}
}

// close closes output channel.
func (c *Causality) close() {
	close(c.causalityCh)
}

// add adds keys relation.
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

// get gets relation for a key.
func (c *Causality) get(key string) string {
	return c.relations[key]
}

// reset resets relations.
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
