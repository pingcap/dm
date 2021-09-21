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

	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/syncer/metrics"
)

// Compactor compact multiple statements into one statement.
type Compactor struct {
	compactedCh    chan map[opType][]*job
	nonCompactedCh chan *job
	drainCh        chan struct{}
	in             chan *job
	counter        int
	bufferSize     int
	chanSize       int
	logger         log.Logger
	safeMode       bool

	// table -> pk -> job
	compactedBuffer map[string]map[string]*job

	// for metrics
	task   string
	source string
}

// newCompactor creates a Compactor instance.
func newCompactor(chanSize, bufferSize int, task, source string, pLogger *log.Logger) *Compactor {
	compactor := &Compactor{
		chanSize:        chanSize,
		bufferSize:      bufferSize,
		logger:          pLogger.WithFields(zap.String("component", "compactor")),
		compactedBuffer: make(map[string]map[string]*job),
		task:            task,
		source:          source,
	}
	return compactor
}

// run runs compactor.
func (c *Compactor) run(in chan *job) (chan map[opType][]*job, chan *job, chan struct{}) {
	c.in = in
	c.compactedCh = make(chan map[opType][]*job)
	c.nonCompactedCh = make(chan *job, c.chanSize)
	c.drainCh = make(chan struct{})

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.runCompactor()
	}()

	go func() {
		defer c.close()
		wg.Wait()
	}()
	return c.compactedCh, c.nonCompactedCh, c.drainCh
}

// runCompactor receive dml jobs and compact.
func (c *Compactor) runCompactor() {
	for {
		select {
		case <-c.drainCh:
			if c.counter > 0 {
				c.flushBuffer()
			}
		case j, ok := <-c.in:
			if !ok {
				return
			}
			metrics.QueueSizeGauge.WithLabelValues(c.task, "compactor_input", c.source).Set(float64(len(c.in)))

			if j.tp == flush {
				<-c.drainCh
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
			if c.counter >= c.bufferSize {
				<-c.drainCh
				c.flushBuffer()
			}
		}
	}
}

// close close out channels.
func (c *Compactor) close() {
	close(c.compactedCh)
	close(c.nonCompactedCh)
}

// flushBuffer flush buffer and reset.
func (c *Compactor) flushBuffer() {
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
	c.compactedCh <- res
	c.counter = 0
	c.compactedBuffer = make(map[string]map[string]*job)
}

// sendFlushJob send flush job to all outer channel.
func (c *Compactor) sendFlushJob(j *job) {
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
func (c *Compactor) compactJob(j *job) {
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
