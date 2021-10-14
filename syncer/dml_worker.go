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
	chanSize       int
	multipleRows   bool
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
	inCh    chan *job
	flushCh chan *job
}

// dmlWorkerWrap creates and runs a dmlWorker instance and return all workers count and flush job channel.
func dmlWorkerWrap(inCh chan *job, syncer *Syncer) (int, chan *job) {
	dmlWorker := &DMLWorker{
		batch:          syncer.cfg.Batch,
		workerCount:    syncer.cfg.WorkerCount,
		chanSize:       syncer.cfg.QueueSize,
		multipleRows:   syncer.cfg.MultipleRows,
		task:           syncer.cfg.Name,
		source:         syncer.cfg.SourceID,
		worker:         syncer.cfg.WorkerName,
		connectionPool: brutils.NewWorkerPool(uint(syncer.cfg.WorkerCount), "dml_connection_pool"),
		logger:         syncer.tctx.Logger.WithFields(zap.String("component", "dml_worker")),
		successFunc:    syncer.successFunc,
		fatalFunc:      syncer.fatalFunc,
		lagFunc:        syncer.updateReplicationJobTS,
		addCountFunc:   syncer.addCount,
		tctx:           syncer.tctx,
		toDBConns:      syncer.toDBConns,
		inCh:           inCh,
		flushCh:        make(chan *job),
	}

	go func() {
		dmlWorker.runDMLWorker()
		dmlWorker.close()
	}()
	return dmlWorker.workerCount, dmlWorker.flushCh
}

// close closes outer channel.
func (w *DMLWorker) close() {
	close(w.flushCh)
}

// runDMLWorker distribute jobs by queueBucket.
func (w *DMLWorker) runDMLWorker() {
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
				w.causalityWg.Add(w.workerCount)
			}
			// flush for every DML queue
			for i, jobCh := range jobChs {
				startTime := time.Now()
				jobCh <- j
				metrics.AddJobDurationHistogram.WithLabelValues(j.tp.String(), w.task, queueBucketMapping[i], w.source).Observe(time.Since(startTime).Seconds())
			}
			if j.tp == conflict {
				w.causalityWg.Wait()
				w.addCountFunc(true, adminQueueName, j.tp, 1, j.targetTable)
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

// genMultipleRowsSQL generate multiple row values SQL by different opType.
// insert into tb values(1,1)	‾|
// insert into tb values(2,2)	 |=> insert into tb values (1,1),(2,2),(3,3)
// insert into tb values(3,3)	_|
// update tb set b=1 where a=1	‾|
// update tb set b=2 where a=2	 |=> replace into tb values(1,1),(2,2),(3,3)
// update tb set b=3 where a=3	_|
// delete from tb where a=1,b=1	‾|
// delete from tb where a=2,b=2	 |=> delete from tb where (a,b) in (1,1),(2,2),(3,3)
// delete from tb where a=3,b=3	_|
// group by [opType => table name => columns].
func genMultipleRowsSQL(op opType, dmls []*DML) ([]string, [][]interface{}) {
	// for compacted jobs, all dmlParmas already has same opType
	// so group by table
	if op != null {
		return gendmlsWithSameTable(op, dmls)
	}
	// for causality jobs, group dmls with opType
	return gendmlsWithSameTp(dmls)
}

// genSingleRowSQL generate single row value SQL.
func genSingleRowSQL(dmls []*DML) ([]string, [][]interface{}) {
	queries := make([]string, 0, len(dmls))
	args := make([][]interface{}, 0, len(dmls))
	for _, dml := range dmls {
		query, arg := dml.genSQL()
		queries = append(queries, query...)
		args = append(args, arg...)
	}
	return queries, args
}

// executeJobs execute jobs in same queueBucket
// All the jobs received should be executed consecutively.
func (w *DMLWorker) executeJobs(queueID int, jobCh chan *job) {
	jobs := make([]*job, 0, w.batch)
	workerJobIdx := dmlWorkerJobIdx(queueID)
	var wg sync.WaitGroup
	queueBucket := queueBucketName(queueID)
	for {
		select {
		case j, ok := <-jobCh:
			if !ok {
				if len(jobs) > 0 {
					w.logger.Warn("have unexecuted jobs when close job chan!", zap.Any("rest job", jobs))
				}
				return
			}
			metrics.QueueSizeGauge.WithLabelValues(w.task, queueBucket, w.source).Set(float64(len(jobCh)))

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

		dmls := make([]*DML, 0, len(jobs))
		for _, j := range jobs {
			dmls = append(dmls, j.dml)
		}
		var queries []string
		var args [][]interface{}
		if w.multipleRows {
			queries, args = genMultipleRowsSQL(op, dmls)
		} else {
			queries, args = genSingleRowSQL(dmls)
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
	return executeJobs
}
