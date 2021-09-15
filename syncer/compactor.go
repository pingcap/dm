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

	// table -> pk -> job
	compactedBuffer map[string]map[string]*job

	// for metrics
	task   string
	source string
}

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

func (c *Compactor) runCompactor() {
	defer c.close()

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

			if j.tp == flush {
				<-c.drainCh
				c.flushBuffer()
				c.sendFlushJob(j)
				continue
			}
			if j.dmlParam.identifyColumns() == nil {
				c.nonCompactedCh <- j
				continue
			}

			if j.tp != update {
				c.compactJob(j)
			} else {
				delJob := j.clone()
				delJob.tp = del
				delJob.dmlParam.values = nil
				c.compactJob(delJob)

				insertJob := j.clone()
				insertJob.tp = insert
				insertJob.dmlParam.oldValues = nil
				c.compactJob(insertJob)
			}
			if c.counter >= c.bufferSize {
				<-c.drainCh
				c.flushBuffer()
			}
		}
	}
}

func (c *Compactor) close() {
	close(c.compactedCh)
	close(c.nonCompactedCh)
}

func (c *Compactor) flushBuffer() {
	res := make(map[opType][]*job, 3)
	for _, tableJobs := range c.compactedBuffer {
		for _, j := range tableJobs {
			res[j.tp] = append(res[j.tp], j)
		}
	}
	c.compactedCh <- res
	c.counter = 0
}

func (c *Compactor) sendFlushJob(j *job) {
	c.compactedCh <- map[opType][]*job{flush: {j}}
	c.nonCompactedCh <- j
}

func (c *Compactor) compactJob(j *job) {
	tableName := j.targetTable
	tableJobs, ok := c.compactedBuffer[tableName]
	if !ok {
		c.compactedBuffer[tableName] = make(map[string]*job, c.bufferSize)
		tableJobs = c.compactedBuffer[tableName]
	}

	key := j.dmlParam.identifyKey()
	prevJob, ok := tableJobs[key]
	if !ok {
		tableJobs[key] = j
		c.counter++
		return
	}

	switch j.tp {
	case insert:
		// delete + insert => insert
		// if prevJob.tp != del {
		// 	// s.tctx.L().Warn("update-insert/insert-insert happen", zap.Reflect("before", oldDML), zap.Reflect("after", dml))
		// }
	case del:
		// insert/update + delete => delete
		// if prevJob.tp != insert && prevJob.tp != update {
		// 	// s.tctx.L().Warn("update-insert/insert-insert happen", zap.Reflect("before", oldDML), zap.Reflect("after", dml))
		// }
	case update:
		if prevJob.tp == insert {
			// insert + update ==> insert
			j.tp = insert
			j.dmlParam.oldValues = nil
		} else if prevJob.tp == update {
			// update + update ==> update
			j.dmlParam.oldValues = prevJob.dmlParam.oldValues
		} // else {
		// s.tctx.L().Warn("update-insert/insert-insert happen", zap.Reflect("before", oldDML), zap.Reflect("after", dml))
		// }
	}
	tableJobs[key] = j
}
