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
	batch       int
	workerCount int
	chanSize    int
	toDBConns   []*dbconn.DBConn
	tctx        *tcontext.Context
	wg          sync.WaitGroup // counts conflict/flush jobs in all DML job channels.
	logger      log.Logger

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
	inCh    chan *job
	flushCh chan *job
}

// dmlWorkerWrap creates and runs a dmlWorker instance and returns flush job channel.
func dmlWorkerWrap(inCh chan *job, syncer *Syncer) chan *job {
	dmlWorker := &DMLWorker{
		batch:        syncer.cfg.Batch,
		workerCount:  syncer.cfg.WorkerCount,
		chanSize:     syncer.cfg.QueueSize / 4,
		task:         syncer.cfg.Name,
		source:       syncer.cfg.SourceID,
		worker:       syncer.cfg.WorkerName,
		logger:       syncer.tctx.Logger.WithFields(zap.String("component", "dml_worker")),
		successFunc:  syncer.successFunc,
		fatalFunc:    syncer.fatalFunc,
		lagFunc:      syncer.updateReplicationJobTS,
		addCountFunc: syncer.addCount,
		tctx:         syncer.tctx,
		toDBConns:    syncer.toDBConns,
		inCh:         inCh,
		flushCh:      make(chan *job),
	}

	go func() {
		dmlWorker.run()
		dmlWorker.close()
	}()
	return dmlWorker.flushCh
}

// close closes outer channel.
func (w *DMLWorker) close() {
	close(w.flushCh)
}

// run distribute jobs by queueBucket.
func (w *DMLWorker) run() {
	jobChs := make([]chan *job, w.workerCount)

	for i := 0; i < w.workerCount; i++ {
		jobChs[i] = make(chan *job, w.chanSize)
		go w.executeJobs(i, jobChs[i])
	}

	defer func() {
		for i := 0; i < w.workerCount; i++ {
			close(jobChs[i])
		}
	}()

	queueBucketMapping := make([]string, w.workerCount)
	for i := 0; i < w.workerCount; i++ {
		queueBucketMapping[i] = queueBucketName(i)
	}

	for j := range w.inCh {
		metrics.QueueSizeGauge.WithLabelValues(w.task, "dml_worker_input", w.source).Set(float64(len(w.inCh)))
		if j.tp == flush || j.tp == conflict {
			if j.tp == conflict {
				w.addCountFunc(false, adminQueueName, j.tp, 1, j.targetTable)
			}
			w.wg.Add(w.workerCount)
			// flush for every DML queue
			for i, jobCh := range jobChs {
				startTime := time.Now()
				jobCh <- j
				metrics.AddJobDurationHistogram.WithLabelValues(j.tp.String(), w.task, queueBucketMapping[i], w.source).Observe(time.Since(startTime).Seconds())
			}
			w.wg.Wait()
			if j.tp == conflict {
				w.addCountFunc(true, adminQueueName, j.tp, 1, j.targetTable)
			} else {
				w.flushCh <- j
			}
		} else {
			queueBucket := int(utils.GenHashKey(j.dml.key)) % w.workerCount
			w.addCountFunc(false, queueBucketMapping[queueBucket], j.tp, 1, j.targetTable)
			startTime := time.Now()
			w.logger.Debug("queue for key", zap.Int("queue", queueBucket), zap.String("key", j.dml.key))
			jobChs[queueBucket] <- j
			metrics.AddJobDurationHistogram.WithLabelValues(j.tp.String(), w.task, queueBucketMapping[queueBucket], w.source).Observe(time.Since(startTime).Seconds())
		}
	}
}

// executeJobs execute jobs in same queueBucket
// All the jobs received should be executed consecutively.
func (w *DMLWorker) executeJobs(queueID int, jobCh chan *job) {
	jobs := make([]*job, 0, w.batch)
	workerJobIdx := dmlWorkerJobIdx(queueID)
	queueBucket := queueBucketName(queueID)
	for j := range jobCh {
		metrics.QueueSizeGauge.WithLabelValues(w.task, queueBucket, w.source).Set(float64(len(jobCh)))

		if j.tp != flush && j.tp != conflict {
			if len(jobs) == 0 {
				// set job TS when received first job of this batch.
				w.lagFunc(j, workerJobIdx)
			}
			jobs = append(jobs, j)
			if len(jobs) < w.batch && len(jobCh) > 0 {
				continue
			}
		}

		failpoint.Inject("syncDMLBatchNotFull", func() {
			if len(jobCh) == 0 && len(jobs) < w.batch {
				w.logger.Info("execute not full job queue")
			}
		})

		w.executeBatchJobs(queueID, jobs)
		if j.tp == conflict || j.tp == flush {
			w.wg.Done()
		}

		jobs = jobs[0:0]
		if len(jobCh) == 0 {
			failpoint.Inject("noJobInQueueLog", func() {
				w.logger.Debug("no job in queue, update lag to zero", zap.Int(
					"workerJobIdx", workerJobIdx), zap.Int64("current ts", time.Now().Unix()))
			})
			w.lagFunc(nil, workerJobIdx)
		}
	}
}

// executeBatchJobs execute jobs with batch size.
func (w *DMLWorker) executeBatchJobs(queueID int, jobs []*job) {
	var (
		affect int
		db     = w.toDBConns[queueID]
		err    error
	)

	defer func() {
		if err == nil {
			w.successFunc(queueID, jobs)
		} else {
			w.fatalFunc(jobs[affect], err)
		}
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

	queries := make([]string, 0, len(jobs))
	args := make([][]interface{}, 0, len(jobs))
	for _, j := range jobs {
		query, arg := j.dml.genSQL()
		queries = append(queries, query...)
		args = append(args, arg...)
	}
	failpoint.Inject("WaitUserCancel", func(v failpoint.Value) {
		t := v.(int)
		time.Sleep(time.Duration(t) * time.Second)
	})
	// use background context to execute sqls as much as possible
	ctx, cancel := w.tctx.WithTimeout(maxDMLExecutionDuration)
	defer cancel()
	affect, err = db.ExecuteSQL(ctx, queries, args...)
	failpoint.Inject("SafeModeExit", func(val failpoint.Value) {
		if intVal, ok := val.(int); ok && intVal == 4 && len(jobs) > 0 {
			w.logger.Warn("fail to exec DML", zap.String("failpoint", "SafeModeExit"))
			affect, err = 0, terror.ErrDBExecuteFailed.Delegate(errors.New("SafeModeExit"), "mock")
		}
	})
}
