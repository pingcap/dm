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
	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/syncer/metrics"
)

// compactItem represents whether a job is compacted.
type compactItem struct {
	j         *job
	compacted bool
}

// newCompactItem creates a compactItem.
func newCompactItem(j *job) *compactItem {
	return &compactItem{j: j}
}

// compactor compacts multiple statements into one statement.
type compactor struct {
	inCh       chan *job
	outCh      chan *job
	bufferSize int
	logger     log.Logger
	safeMode   bool

	keyMap map[string]map[string]int // table -> pk -> pos
	buffer []*compactItem

	// for metrics
	task   string
	source string
}

// compactorWrap creates and runs a compactor instance.
func compactorWrap(inCh chan *job, syncer *Syncer) chan *job {
	bufferSize := syncer.cfg.QueueSize * syncer.cfg.WorkerCount / 4
	compactor := &compactor{
		inCh:       inCh,
		outCh:      make(chan *job, bufferSize),
		bufferSize: bufferSize,
		logger:     syncer.tctx.Logger.WithFields(zap.String("component", "compactor")),
		keyMap:     make(map[string]map[string]int),
		buffer:     make([]*compactItem, 0, bufferSize),
		task:       syncer.cfg.Name,
		source:     syncer.cfg.SourceID,
	}
	go func() {
		compactor.run()
		compactor.close()
	}()
	return compactor.outCh
}

// run runs a compactor instance.
func (c *compactor) run() {
	for j := range c.inCh {
		metrics.QueueSizeGauge.WithLabelValues(c.task, "compactor_input", c.source).Set(float64(len(c.inCh)))

		if j.tp == flush {
			c.flushBuffer()
			c.outCh <- j
			continue
		}

		// set safeMode when receive first job
		if len(c.buffer) == 0 {
			c.safeMode = j.dml.safeMode
		}
		// if dml has no PK/NOT NULL UK, do not compact it.
		if j.dml.identifyColumns() == nil {
			c.buffer = append(c.buffer, newCompactItem(j))
			continue
		}

		// if update job update its identify keys, turn it into delete + insert
		if j.dml.op == update && j.dml.updateIdentify() {
			delDML, insertDML := updateToDelAndInsert(j.dml)
			delJob := j.clone()
			delJob.tp = del
			delJob.dml = delDML

			insertJob := j.clone()
			insertJob.tp = insert
			insertJob.dml = insertDML

			c.compactJob(delJob)
			c.compactJob(insertJob)
		} else {
			c.compactJob(j)
		}

		// if no inner jobs, buffer is full or outer jobs less than half of bufferSize(chanSize), flush the buffer
		if len(c.inCh) == 0 || len(c.buffer) >= c.bufferSize || len(c.outCh) < c.bufferSize/2 {
			c.flushBuffer()
		}
	}
}

// close closes outer channels.
func (c *compactor) close() {
	close(c.outCh)
}

// flushBuffer flush buffer and reset compactor.
func (c *compactor) flushBuffer() {
	for _, item := range c.buffer {
		if !item.compacted {
			c.outCh <- item.j
		}
	}
	c.keyMap = make(map[string]map[string]int)
	c.buffer = c.buffer[0:0]
}

// compactJob compact jobs.
// INSERT + INSERT => X			‾|
// UPDATE + INSERT => X			 |=> anything + INSERT => INSERT
// DELETE + INSERT => INSERT	_|
// INSERT + DELETE => DELETE	‾|
// UPDATE + DELETE => DELETE	 |=> anything + DELETE => DELETE
// DELETE + DELETE => X			_|
// INSERT + UPDATE => INSERT	‾|
// UPDATE + UPDATE => UPDATE	 |=> INSERT + UPDATE => INSERT, UPDATE + UPDATE => UPDATE
// DELETE + UPDATE => X			_|
// .
func (c *compactor) compactJob(j *job) {
	tableName := j.dml.targetTableID
	tableKeyMap, ok := c.keyMap[tableName]
	if !ok {
		c.keyMap[tableName] = make(map[string]int, c.bufferSize)
		tableKeyMap = c.keyMap[tableName]
	}

	key := j.dml.identifyKey()
	prevPos, ok := tableKeyMap[key]
	// if no such key in the buffer, add it
	if !ok || prevPos >= len(c.buffer) {
		// should not happen, avoid panic
		if ok && prevPos >= len(c.buffer) {
			c.logger.Error("cannot find previous job by key", zap.String("key", key), zap.Int("pos", prevPos))
		}
		tableKeyMap[key] = len(c.buffer)
		c.buffer = append(c.buffer, newCompactItem(j))
		return
	}

	prevItem := c.buffer[prevPos]
	prevJob := prevItem.j

	c.logger.Debug("start to compact", zap.Stringer("previous dml", prevJob.dml), zap.Stringer("current dml", j.dml))

	if j.tp == update {
		if prevJob.tp == insert {
			// INSERT + UPDATE => INSERT
			j.tp = insert
			j.dml.oldValues = nil
			j.dml.originOldValues = nil
			j.dml.op = insert
		} else if prevJob.tp == update {
			// UPDATE + UPDATE => UPDATE
			j.dml.oldValues = prevJob.dml.oldValues
			j.dml.originOldValues = prevJob.dml.originOldValues
		}
	}

	// mark previous job as compacted, add new job
	prevItem.compacted = true
	tableKeyMap[key] = len(c.buffer)
	c.buffer = append(c.buffer, newCompactItem(j))
	c.logger.Debug("finish to compact", zap.Stringer("dml", j.dml))
}
