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

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb-tools/pkg/filter"
	brutils "github.com/pingcap/tidb/br/pkg/utils"
	"go.uber.org/zap"

	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/pingcap/dm/syncer/dbconn"
	"github.com/pingcap/dm/syncer/metrics"
)

// DMLWorker is used to sync dml.
type DMLWorker struct {
	batch          int
	workerCount    int
	queueSize      int
	toDBConns      []*dbconn.DBConn
	tctx           *tcontext.Context
	causalityWg    sync.WaitGroup
	connectionPool *brutils.WorkerPool
	logger         log.Logger

	// for metrics
	task   string
	source string
	worker string

	// callback func
	// TODO: refine callback func
	successFunc  func(int, []*job)
	fatalFunc    func(*job, error)
	lagFunc      func(*job, int)
	addCountFunc func(bool, string, opType, int64, *filter.Table)

	// channel
	compactedCh chan map[opType][]*job
	pullCh      chan struct{}
	causalityCh chan *job
	flushCh     chan *job
}

func newDMLWorker(batch, workerCount, queueSize int, pLogger *log.Logger, task, source, worker string,
	successFunc func(int, []*job),
	fatalFunc func(*job, error),
	lagFunc func(*job, int),
	addCountFunc func(bool, string, opType, int64, *filter.Table),
) *DMLWorker {
	return &DMLWorker{
		batch:          batch,
		workerCount:    workerCount,
		queueSize:      queueSize,
		task:           task,
		source:         source,
		worker:         worker,
		connectionPool: brutils.NewWorkerPool(uint(workerCount), "dml_connection_pool"),
		logger:         pLogger.WithFields(zap.String("component", "dml_worker")),
		successFunc:    successFunc,
		fatalFunc:      fatalFunc,
		lagFunc:        lagFunc,
		addCountFunc:   addCountFunc,
	}
}

// run runs dml workers, return worker count and flush job channel.
func (w *DMLWorker) run(tctx *tcontext.Context, toDBConns []*dbconn.DBConn, compactedCh chan map[opType][]*job, pullCh chan struct{}, causalityCh chan *job) (int, chan *job) {
	w.tctx = tctx
	w.toDBConns = toDBConns
	w.compactedCh = compactedCh
	w.pullCh = pullCh
	w.causalityCh = causalityCh
	w.flushCh = make(chan *job)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		w.runCompactedDMLWorker()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		w.runCausalityDMLWorker()
	}()

	go func() {
		defer w.close()
		wg.Wait()
	}()

	return w.workerCount + 1, w.flushCh
}

func (w *DMLWorker) close() {
	close(w.flushCh)
}

// executeBatchJobs execute jobs with batch size.
func (w *DMLWorker) executeBatchJobs(queueID int, op opType, jobs []*job, clearFunc func()) func(uint64) {
	executeJobs := func(id uint64) {
		var (
			affect int
			db     = w.toDBConns[int(id)-1]
			err    error
		)

		defer func() {
			if err == nil {
				w.successFunc(queueID, jobs)
			} else {
				w.fatalFunc(jobs[affect], err)
			}
			clearFunc()
		}()

		if len(jobs) == 0 {
			return
		}
		failpoint.Inject("BlockExecuteSQLs", func(v failpoint.Value) {
			t := v.(int) // sleep time
			w.logger.Info("BlockExecuteSQLs", zap.Any("job", jobs[0]), zap.Int("sleep time", t))
			time.Sleep(time.Second * time.Duration(t))
		})

		failpoint.Inject("failSecondJob", func() {
			if failExecuteSQL && failOnce.CAS(false, true) {
				w.logger.Info("trigger failSecondJob")
				err = terror.ErrDBExecuteFailed.Delegate(errors.New("failSecondJob"), "mock")
				failpoint.Return()
			}
		})

		dmlParams := make([]*DMLParam, 0, len(jobs))
		for _, j := range jobs {
			dmlParams = append(dmlParams, j.dmlParam)
		}
		queries, args := genMultipleRowsSQL(op, dmlParams)
		failpoint.Inject("WaitUserCancel", func(v failpoint.Value) {
			t := v.(int)
			time.Sleep(time.Duration(t) * time.Second)
		})
		// use background context to execute sqls as much as possible
		ctctx, cancel := w.tctx.WithTimeout(maxDMLExecutionDuration)
		defer cancel()
		affect, err = db.ExecuteSQL(ctctx, queries, args...)
		failpoint.Inject("SafeModeExit", func(val failpoint.Value) {
			if intVal, ok := val.(int); ok && intVal == 4 && len(jobs) > 0 {
				w.logger.Warn("fail to exec DML", zap.String("failpoint", "SafeModeExit"))
				affect, err = 0, terror.ErrDBExecuteFailed.Delegate(errors.New("SafeModeExit"), "mock")
			}
		})
	}
	return executeJobs
}

// executeCausalityJobs execute jobs in same queueBucket
// All the jobs received should be executed consecutively.
func (w *DMLWorker) executeCausalityJobs(queueID int, jobCh chan *job) {
	jobs := make([]*job, 0, w.batch)
	workerJobIdx := dmlWorkerJobIdx(queueID)
	var wg sync.WaitGroup
	queueBucket := queueBucketName(queueID)
	for {
		select {
		case j, ok := <-jobCh:
			metrics.QueueSizeGauge.WithLabelValues(w.task, queueBucket, w.source).Set(float64(len(jobCh)))
			if !ok {
				if len(jobs) > 0 {
					w.logger.Warn("have unexecuted jobs when close job chan!", zap.Any("rest job", jobs))
				}
				return
			}
			if j.tp != flush && j.tp != conflict {
				if len(jobs) == 0 {
					// set job TS when received first job of this batch.
					w.lagFunc(j, workerJobIdx)
				}
				jobs = append(jobs, j)
				if len(jobs) < w.batch {
					continue
				}
			}

			// wait for previous jobs executed
			wg.Wait()
			batchJobs := jobs
			wg.Add(1)

			if j.tp == conflict {
				w.connectionPool.ApplyWithID(w.executeBatchJobs(queueID, null, batchJobs, func() {
					wg.Done()
					w.causalityWg.Done()
				}))
			} else {
				w.connectionPool.ApplyWithID(w.executeBatchJobs(queueID, null, batchJobs, func() { wg.Done() }))
			}

			if j.tp == flush {
				wg.Wait()
				w.flushCh <- j
			}
			jobs = make([]*job, 0, w.batch)
		case <-time.After(waitTime):
			if len(jobs) > 0 {
				failpoint.Inject("syncDMLTicker", func() {
					w.logger.Info("job queue not full, executeSQLs by ticker")
				})
				// wait for previous jobs executed
				wg.Wait()
				batchJobs := jobs
				wg.Add(1)
				w.connectionPool.ApplyWithID(w.executeBatchJobs(queueID, null, batchJobs, func() { wg.Done() }))
				jobs = make([]*job, 0, w.batch)
			} else {
				failpoint.Inject("noJobInQueueLog", func() {
					w.logger.Debug("no job in queue, update lag to zero", zap.Int(
						"workerJobIdx", workerJobIdx), zap.Int64("current ts", time.Now().Unix()))
				})
				w.lagFunc(nil, workerJobIdx)
			}
		}
	}
}

func (w *DMLWorker) executeCompactedJobsWithOpType(op opType, jobsMap map[opType][]*job) {
	jobs, ok := jobsMap[op]
	if !ok {
		return
	}
	var wg sync.WaitGroup
	for i := 0; i < len(jobs); i += w.batch {
		j := i + w.batch
		if j >= len(jobs) {
			j = len(jobs)
		}
		wg.Add(1)
		batchJobs := jobs[i:j]
		w.connectionPool.ApplyWithID(w.executeBatchJobs(w.workerCount, op, batchJobs, func() { wg.Done() }))
	}
	wg.Wait()
}

func (w *DMLWorker) runCompactedDMLWorker() {
	for {
		select {
		case jobsMap, ok := <-w.compactedCh:
			if !ok {
				return
			}
			w.executeCompactedJobsWithOpType(del, jobsMap)
			w.executeCompactedJobsWithOpType(insert, jobsMap)
			w.executeCompactedJobsWithOpType(update, jobsMap)

			if _, ok := jobsMap[flush]; ok {
				w.flushCh <- newFlushJob()
			}
		case <-time.After(waitTime):
			w.pullCh <- struct{}{}
		}
	}
}

// runCausalityDMLWorker distribute jobs by queueBucket.
func (w *DMLWorker) runCausalityDMLWorker() {
	causalityJobChs := make([]chan *job, w.workerCount)

	for i := 0; i < w.workerCount; i++ {
		causalityJobChs[i] = make(chan *job, w.queueSize)
		go w.executeCausalityJobs(i, causalityJobChs[i])
	}

	defer func() {
		for i := 0; i < w.workerCount; i++ {
			close(causalityJobChs[i])
		}
	}()

	queueBucketMapping := make([]string, w.workerCount)
	for i := 0; i < w.workerCount; i++ {
		queueBucketMapping[i] = queueBucketName(i)
	}

	for j := range w.causalityCh {
		metrics.QueueSizeGauge.WithLabelValues(w.task, "causality_output", w.source).Set(float64(len(w.causalityCh)))
		if j.tp == flush || j.tp == conflict {
			if j.tp == conflict {
				w.causalityWg.Add(w.workerCount)
			}
			// flush for every DML queue
			for i, causalityJobCh := range causalityJobChs {
				w.addCountFunc(false, queueBucketMapping[i], j.tp, 1, j.targetTable)
				startTime := time.Now()
				causalityJobCh <- j
				metrics.AddJobDurationHistogram.WithLabelValues(j.tp.String(), w.task, queueBucketMapping[i], w.source).Observe(time.Since(startTime).Seconds())
			}
			if j.tp == conflict {
				w.causalityWg.Wait()
			}
		} else {
			queueBucket := int(utils.GenHashKey(j.dmlParam.key)) % w.workerCount
			w.addCountFunc(false, queueBucketMapping[queueBucket], j.tp, 1, j.targetTable)
			startTime := time.Now()
			w.logger.Debug("queue for key", zap.Int("queue", queueBucket), zap.String("key", j.dmlParam.key))
			causalityJobChs[queueBucket] <- j
			metrics.AddJobDurationHistogram.WithLabelValues(j.tp.String(), w.task, queueBucketMapping[queueBucket], w.source).Observe(time.Since(startTime).Seconds())
		}
	}
}
