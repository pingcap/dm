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
	"context"
	"strings"

	"github.com/pingcap/dm/pkg/log"
)

type dmlType int

const (
	insertDML dmlType = iota + 1
	updateDML
	deleteDML
	replaceDML
)

// CompactorJob represents a job for compactor.
type CompactorJob struct {
	dmlJob *job
	oldKey string
	newKey string
	skip   bool
	pin    bool
}

// NewCompactorJob create a CompactorJob.
func NewCompactorJob(dmlJob *job) *CompactorJob {
	oldKey, newKey := extractKeys(dmlJob.dmlTp, dmlJob.keys)
	return &CompactorJob{
		dmlJob: dmlJob,
		oldKey: oldKey,
		newKey: newKey,
	}
}

// Compactor compact multiple statements into one statement.
type Compactor struct {
	compactorJobs []*CompactorJob
	keyMap        map[string]int
	batchSize     int
	bufferSize    int
	in            chan *job
	drainNotify   chan struct{}
	out           chan []*job
}

// NewCompactor creates a new Compactor.
func NewCompactor(batchSize int, bufferSize int, in chan *job) *Compactor {
	return &Compactor{
		batchSize:     batchSize,
		bufferSize:    bufferSize,
		compactorJobs: make([]*CompactorJob, 0, bufferSize),
		keyMap:        make(map[string]int, bufferSize),
		in:            in,
		out:           make(chan []*job, 1),
		drainNotify:   make(chan struct{}),
	}
}

func (c *Compactor) compact(compactorJob *CompactorJob) {
	c.compactorJobs = append(c.compactorJobs, compactorJob)
	curPos := len(c.compactorJobs) - 1

	prePos, exist := c.keyMap[compactorJob.newKey]
	switch compactorJob.dmlJob.dmlTp {
	case replaceDML:
		if exist && !c.compactorJobs[prePos].pin {
			c.compactorJobs[prePos].skip = true
		}
	case deleteDML:
		if exist {
			if !c.compactorJobs[prePos].pin {
				c.compactorJobs[prePos].skip = true
			}
			if c.compactorJobs[prePos].dmlJob.dmlTp == insertDML {
				c.compactorJobs[curPos].skip = true
			}
		}
	case updateDML:
		if exist && !c.compactorJobs[prePos].pin {
			c.compactorJobs[prePos].skip = true
		}
		if compactorJob.newKey != compactorJob.oldKey {
			c.compactorJobs[curPos].pin = true
		}
	default:
	}
	c.keyMap[compactorJob.newKey] = curPos
}

func (c *Compactor) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.L().Info("ctx done in run")
			return
		case sqlJob, ok := <-c.in:
			if !ok {
				return
			}

			if sqlJob.tp == flush {
				for len(c.compactorJobs) > 0 {
					c.sendJob()
				}
				c.sendFlushJob(sqlJob)
				continue
			}

			if len(sqlJob.sql) > 0 {
				c.compact(NewCompactorJob(sqlJob))
			}

			if len(c.compactorJobs) >= c.bufferSize {
				c.sendJob()
			}
		case <-c.drainNotify:
			c.sendJob()
		}
	}
}

func (c *Compactor) getJobs() []*job {
	if len(c.out) == 0 {
		c.drainNotify <- struct{}{}
	}
	return <-c.out
}

func (c *Compactor) sendJob() {
	jobs := make([]*job, 0, c.batchSize)
	idx := 0
	for _, compactorJob := range c.compactorJobs {
		idx++
		if !compactorJob.skip {
			jobs = append(jobs, compactorJob.dmlJob)
		}
		if len(jobs) >= c.batchSize {
			break
		}
	}

	c.compactorJobs = c.compactorJobs[idx:]
	for key, pos := range c.keyMap {
		if pos < idx {
			delete(c.keyMap, key)
		} else {
			c.keyMap[key] = pos - idx
		}
	}

	c.out <- jobs
}

func (c *Compactor) sendFlushJob(flushJob *job) {
	c.out <- []*job{flushJob}
}

func extractKeys(dmlTp dmlType, keys []string) (string, string) {
	var buf strings.Builder
	if dmlTp == updateDML {
		for i := 0; i < len(keys)/2; i++ {
			buf.WriteString(keys[i])
		}
		oldKey := buf.String()
		buf.Reset()
		for i := len(keys) / 2; i < len(keys); i++ {
			buf.WriteString(keys[i])
		}
		newKey := buf.String()
		return oldKey, newKey
	}

	for i := 0; i < len(keys); i++ {
		buf.WriteString(keys[i])
	}
	key := buf.String()
	return key, key
}
