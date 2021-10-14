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

func newCompactItem(j *job) *compactItem {
	return &compactItem{j: j}
}

// compactor compact multiple statements into one statement.
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

// compactorWrap creates and runs a Compactor instance.
func compactorWrap(inCh chan *job, syncer *Syncer) chan *job {
	bufferSize := syncer.cfg.QueueSize * syncer.cfg.WorkerCount
	compactor := &compactor{
		inCh:       inCh,
		outCh:      make(chan *job, 1),
		bufferSize: bufferSize,
		logger:     syncer.tctx.Logger.WithFields(zap.String("component", "compactor")),
		keyMap:     make(map[string]map[string]int),
		buffer:     make([]*compactItem, 0, bufferSize),
		task:       syncer.cfg.Name,
		source:     syncer.cfg.SourceID,
	}
	go func() {
		compactor.runCompactor()
		compactor.close()
	}()
	return compactor.outCh
}

// runCompactor receive dml jobs and compact.
func (c *compactor) runCompactor() {
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
		if j.dml.identifyColumns() == nil {
			c.buffer = append(c.buffer, newCompactItem(j))
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

		if len(c.inCh) == 0 || len(c.buffer) >= c.bufferSize {
			c.flushBuffer()
		}
	}
}

// close close out channels.
func (c *compactor) close() {
	close(c.outCh)
}

// flushBuffer flush buffer and reset.
func (c *compactor) flushBuffer() {
	for _, item := range c.buffer {
		if !item.compacted {
			c.outCh <- item.j
		}
	}
	c.keyMap = make(map[string]map[string]int)
	c.buffer = make([]*compactItem, 0, c.bufferSize)
}

// compactJob compact jobs.
// INSERT + INSERT => X			‾|
// UPDATE + INSERT => X			 |=> anything + INSERT => INSERT
// DELETE + INSERT => INSERT	_|
// INSERT + DELETE => DELETE	‾|
// UPDATE + INSERT => DELETE	 |=> anything + DELETE => DELETE
// DELETE + INSERT => X			_|
// INSERT + UPDATE => INSERT	‾|
// UPDATE + UPDATE => UPDATE	 |=> INSERT + UPDATE => INSERT, UPDATE + UPDATE => UPDATE
// DELETE + UPDATE => X			_|
// .
func (c *compactor) compactJob(j *job) {
	tableName := j.dml.tableID
	tableMap, ok := c.keyMap[tableName]
	if !ok {
		c.keyMap[tableName] = make(map[string]int, c.bufferSize)
		tableMap = c.keyMap[tableName]
	}

	key := j.dml.identifyKey()
	prevPos, ok := tableMap[key]
	// if no such key in the buffer, add it
	if !ok {
		tableMap[key] = len(c.buffer)
		c.buffer = append(c.buffer, newCompactItem(j))
		return
	}

	// should not happen, avoid panic
	if prevPos >= len(c.buffer) {
		c.logger.Error("cannot find previous job by key", zap.String("key", key), zap.Int("pos", prevPos))
	}
	prevItem := c.buffer[prevPos]
	prevJob := prevItem.j

	if j.tp == update {
		if prevJob.tp == insert {
			j.tp = insert
			j.dml.oldValues = nil
			j.dml.originOldValues = nil
			j.dml.op = insert
		} else if prevJob.tp == update {
			j.dml.oldValues = prevJob.dml.oldValues
			j.dml.originOldValues = prevJob.dml.originOldValues
		}
	}

	// mark previous job as compacted, add new job
	prevItem.compacted = true
	tableMap[key] = len(c.buffer)
	prevQueries, prevArgs := prevItem.j.dml.genSQL()
	curQueries, curArgs := j.dml.genSQL()
	c.logger.Debug("compact dml", zap.Reflect("prev sqls", prevQueries), zap.Reflect("prev args", prevArgs), zap.Reflect("cur quries", curQueries), zap.Reflect("cur args", curArgs))
	c.buffer = append(c.buffer, newCompactItem(j))
}
