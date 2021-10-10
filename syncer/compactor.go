// Copyright 2021 PingCAP, Inc.
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

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/syncer/metrics"
)

// compactor compact multiple statements into one statement.
type compactor struct {
	compactedCh    chan map[opType][]*job
	nonCompactedCh chan *job
	drainCh        chan struct{}
	in             chan *job
	drain          atomic.Bool
	counter        int
	bufferSize     int
	logger         log.Logger
	safeMode       bool

	// table -> pk -> job
	compactedBuffer map[string]map[string]*job

	// for metrics
	task   string
	source string
}

// compactorWrap creates and runs a Compactor instance.
func compactorWrap(in chan *job, syncer *Syncer) (chan map[opType][]*job, chan *job, chan struct{}) {
	compactor := &compactor{
		bufferSize:      syncer.cfg.QueueSize * syncer.cfg.WorkerCount,
		logger:          syncer.tctx.Logger.WithFields(zap.String("component", "compactor")),
		compactedBuffer: make(map[string]map[string]*job),
		task:            syncer.cfg.Name,
		source:          syncer.cfg.SourceID,
		in:              in,
		compactedCh:     make(chan map[opType][]*job),
		nonCompactedCh:  make(chan *job, syncer.cfg.QueueSize),
		drainCh:         make(chan struct{}),
	}
	compactor.run()
	return compactor.compactedCh, compactor.nonCompactedCh, compactor.drainCh
}

// run runs compactor.
func (c *compactor) run() {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.runCompactor()
	}()

	// no need to wait because it close by sender
	go func() {
		for range c.drainCh {
			c.drain.Store(true)
		}
	}()

	go func() {
		defer c.close()
		wg.Wait()
	}()
}

// runCompactor receive dml jobs and compact.
func (c *compactor) runCompactor() {
	for {
		select {
		case j, ok := <-c.in:
			if !ok {
				return
			}
			metrics.QueueSizeGauge.WithLabelValues(c.task, "compactor_input", c.source).Set(float64(len(c.in)))

			if j.tp == flush {
				c.flushBuffer()
				c.sendFlushJob(j)
				continue
			}

			// set safeMode when receive first job
			if c.counter == 0 {
				c.safeMode = j.dml.safeMode
			}
			if j.dml.identifyColumns() == nil {
				c.nonCompactedCh <- j
				continue
			}

			// if update job update its indentify keys, split it as delete + insert
			if j.tp == update && j.dml.updateIdentify() {
				delJob := j.clone()
				delJob.tp = del
				delJob.dml = j.dml.newDelDML()
				c.compactJob(delJob)

				insertJob := j.clone()
				insertJob.tp = insert
				insertJob.dml = j.dml.newInsertDML()
				c.compactJob(insertJob)
			} else {
				c.compactJob(j)
			}
			if c.drain.Load() {
				c.flushBuffer()
			}
		case <-time.After(waitTime):
			if c.drain.Load() && c.counter > 0 {
				c.flushBuffer()
			}
		}
	}
}

// close close out channels.
func (c *compactor) close() {
	close(c.compactedCh)
	close(c.nonCompactedCh)
}

// flushBuffer flush buffer and reset.
func (c *compactor) flushBuffer() {
	if c.counter == 0 {
		return
	}

	res := make(map[opType][]*job, 3)
	for _, tableJobs := range c.compactedBuffer {
		for _, j := range tableJobs {
			// if there is one job with safeMode(first one), we set safeMode for all other jobs
			if c.safeMode {
				j.dml.safeMode = c.safeMode
			}
			res[j.tp] = append(res[j.tp], j)
		}
	}
	c.drain.Store(false)
	c.compactedCh <- res
	c.counter = 0
	c.compactedBuffer = make(map[string]map[string]*job)
}

// sendFlushJob send flush job to all outer channel.
func (c *compactor) sendFlushJob(j *job) {
	c.drain.Store(false)
	c.compactedCh <- map[opType][]*job{flush: {j}}
	c.nonCompactedCh <- j
}

// compactJob compact jobs.
// insert + insert => X			‾|
// update + insert => X			 |=> anything + insert => insert
// delete + insert => INSERT	_|
// insert + delete => delete	‾|
// update + insert => delete	 |=> anything + delete => delete
// delete + insert => X			_|
// insert + update => insert	‾|
// update + update => update	 |=> insert + update => insert, update + update => update
// delete + update => X			_|
// .
func (c *compactor) compactJob(j *job) {
	tableName := j.dml.tableID
	tableJobs, ok := c.compactedBuffer[tableName]
	if !ok {
		c.compactedBuffer[tableName] = make(map[string]*job, c.bufferSize)
		tableJobs = c.compactedBuffer[tableName]
	}

	key := j.dml.identifyKey()
	prevJob, ok := tableJobs[key]
	if !ok {
		tableJobs[key] = j
		c.counter++
		return
	}

	if j.tp == update {
		if prevJob.tp == insert {
			j.tp = insert
			j.dml.oldValues = nil
			j.dml.originOldValues = nil
		} else if prevJob.tp == update {
			j.dml.oldValues = prevJob.dml.oldValues
			j.dml.originOldValues = prevJob.dml.originOldValues
		}
	}
	tableJobs[key] = j
}
