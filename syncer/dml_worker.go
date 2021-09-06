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
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/unit"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/pingcap/dm/syncer/metrics"
)

// RunDMLWorker runs dml workers.
func (s *Syncer) RunDMLWorker(compactedCh chan map[opType][]*job, pullCh chan struct{}, causalityCh chan *job) {
	go s.runCompactedDMLWorker(compactedCh, pullCh)
	go s.runCausalityDMLWorker(causalityCh)
}

func (s *Syncer) successFunc(queueID int, jobs []*job) {
	queueBucket := queueBucketName(queueID)
	if len(jobs) > 0 {
		// NOTE: we can use the first job of job queue to calculate lag because when this job committed,
		// every event before this job's event in this queue has already commit.
		// and we can use this job to maintain the oldest binlog event ts among all workers.
		j := jobs[0]
		switch j.tp {
		case ddl:
			metrics.BinlogEventCost.WithLabelValues(metrics.BinlogEventCostStageDDLExec, s.cfg.Name, s.cfg.WorkerName, s.cfg.SourceID).Observe(time.Since(j.jobAddTime).Seconds())
		case insert, update, del:
			metrics.BinlogEventCost.WithLabelValues(metrics.BinlogEventCostStageDMLExec, s.cfg.Name, s.cfg.WorkerName, s.cfg.SourceID).Observe(time.Since(j.jobAddTime).Seconds())
			// metric only increases by 1 because dm batches sql jobs in a single transaction.
			metrics.FinishedTransactionTotal.WithLabelValues(s.cfg.SourceID, s.cfg.WorkerName, s.cfg.SourceID).Inc()
		}
	}

	for _, sqlJob := range jobs {
		s.addCount(true, queueBucket, sqlJob.tp, 1, sqlJob.targetSchema, sqlJob.targetTable)
	}
	s.updateReplicationJobTS(nil, dmlWorkerJobIdx(queueID))
	metrics.ReplicationTransactionBatch.WithLabelValues(s.cfg.WorkerName, s.cfg.Name, s.cfg.SourceID, queueBucket).Observe(float64(len(jobs)))
}

func (s *Syncer) fatalFunc(job *job, err error) {
	s.execError.Store(err)
	if !utils.IsContextCanceledError(err) {
		err = s.handleEventError(err, job.startLocation, job.currentLocation, false, "")
		s.runFatalChan <- unit.NewProcessError(err)
	}
}

func (s *Syncer) executeBatchJobs(jobs []*job, clearFunc func()) func(uint64) {
	executeJobs := func(workerID uint64) {
		var (
			queueID = int(workerID - 1)
			affect  int
			db      = s.toDBConns[queueID]
			err     error
		)

		defer func() {
			if err == nil {
				s.successFunc(queueID, jobs)
			} else {
				s.fatalFunc(jobs[affect], err)
			}
			clearFunc()
		}()

		if len(jobs) == 0 {
			return
		}
		failpoint.Inject("failSecondJob", func() {
			if failExecuteSQL && failOnce.CAS(false, true) {
				// s.tctx.L().Info("trigger failSecondJob")
				err = terror.ErrDBExecuteFailed.Delegate(errors.New("failSecondJob"), "mock")
				failpoint.Return()
			}
		})

		queries := make([]string, 0, len(jobs))
		args := make([][]interface{}, 0, len(jobs))
		for _, j := range jobs {
			queries = append(queries, j.sql)
			args = append(args, j.args)
		}
		failpoint.Inject("WaitUserCancel", func(v failpoint.Value) {
			t := v.(int)
			time.Sleep(time.Duration(t) * time.Second)
		})
		// use background context to execute sqls as much as possible
		ctctx, cancel := s.tctx.WithTimeout(maxDMLExecutionDuration)
		defer cancel()
		affect, err = db.ExecuteSQL(ctctx, queries, args...)
		failpoint.Inject("SafeModeExit", func(val failpoint.Value) {
			if intVal, ok := val.(int); ok && intVal == 4 && len(jobs) > 0 {
				s.tctx.L().Warn("fail to exec DML", zap.String("failpoint", "SafeModeExit"))
				affect, err = 0, terror.ErrDBExecuteFailed.Delegate(errors.New("SafeModeExit"), "mock")
			}
		})
	}
	return executeJobs
}

// func (s *Syncer) mergeValues(jobs []*job) []*job {
// 	results := make([]*job, 0, len(jobs))
// 	curTable := ""
// 	curTp := null
// 	for _, j := range jobs {
// 		if j.tp == update && !j.UpdateKeys() {
// 			j.tp = replace
// 		}
// 		if curTable != j.TableName() || curTp != j.tp || j.tp == update {
// 			curTable = j.TableName
// 			curTp = j.tp
// 			j.sql = genSQL()
// 			results = append(results, j)
// 			continue
// 		}
// 		j.sql = appendValue()
// 		j.args = appendArgs()
// 	}
// 	return results
// }

func (s *Syncer) executeCompactedJobs(jobsMap map[opType][]*job) {
	var wg sync.WaitGroup
	opOrder := []opType{del, insert, update}
	for _, op := range opOrder {
		jobs := jobsMap[op]
		for i := 0; i < len(jobs); i += s.cfg.Batch {
			j := i + s.cfg.Batch
			if j >= len(jobs) {
				j = len(jobs)
			}
			batchJobs := jobs[i:j]
			wg.Add(1)
			s.connectionPool.ApplyWithID(s.executeBatchJobs(batchJobs, func() { wg.Done() }))
		}
		wg.Wait()
	}
}

func (s *Syncer) executeCausalityJobs(jobCh chan *job) {
	jobs := make([]*job, 0, s.cfg.Batch)
	var wg sync.WaitGroup
	for {
		select {
		case j, ok := <-jobCh:
			if !ok {
				// if len(jobs) > 0 {
				// 	// s.tctx.L().Warn("have unexecuted jobs when close job chan!", zap.Any("rest job", allJobs))
				// }
				return
			}
			if j.tp != flush && j.tp != conflict && len(j.sql) > 0 {
				jobs = append(jobs, j)
				if len(jobs) < s.cfg.Batch {
					continue
				}
			}

			// wait for previous jobs executed
			wg.Wait()
			// wait for previous causality jobs
			s.causalityWg.Wait()

			batchJobs := jobs
			wg.Add(1)

			if j.tp == conflict {
				s.connectionPool.ApplyWithID(s.executeBatchJobs(batchJobs, func() {
					wg.Done()
					s.causalityWg.Done()
				}))
			} else {
				s.connectionPool.ApplyWithID(s.executeBatchJobs(batchJobs, func() { wg.Done() }))
			}

			// TODO: waiting for async flush
			if j.tp == flush {
				wg.Wait()
				//	flushCh <- j
			}
			jobs = make([]*job, 0, s.cfg.Batch)
		case <-time.After(waitTime):
			if len(jobs) > 0 {
				// failpoint.Inject("syncDMLTicker", func() {
				// 	s.tctx.L().Info("job queue not full, executeSQLs by ticker")
				// })
				batchJobs := jobs
				wg.Add(1)
				s.connectionPool.ApplyWithID(s.executeBatchJobs(batchJobs, func() { wg.Done() }))
				jobs = make([]*job, 0, s.cfg.Batch)
			}
			//	// waiting #2060
			//	failpoint.Inject("noJobInQueueLog", func() {
			//	s.tctx.L().Debug("no job in queue, update lag to zero", zap.Int64("current ts", time.Now().Unix()))
			//	})
			//	// update lag metric even if there is no job in the queue
			//	// s.updateReplicationLag(nil, workerLagKey)
		}
	}
}

func (s *Syncer) runCompactedDMLWorker(compactedCh chan map[opType][]*job, pullCh chan struct{}) {
	for jobsMap := range compactedCh {
		if _, ok := jobsMap[flush]; ok {
		} else {
			s.executeCompactedJobs(jobsMap)
		}
		pullCh <- struct{}{}
	}
}

func (s *Syncer) runCausalityDMLWorker(causalityCh chan *job) {
	causalityJobChs := make([]chan *job, s.cfg.WorkerCount)

	for i := 0; i < s.cfg.WorkerCount; i++ {
		causalityJobChs[i] = make(chan *job, s.cfg.QueueSize)
		go s.executeCausalityJobs(causalityJobChs[i])
	}

	for j := range causalityCh {
		if j.tp == flush || j.tp == conflict {
			for _, causalityJobCh := range causalityJobChs {
				causalityJobCh <- j
			}
		} else {
			causalityJobChs[int(utils.GenHashKey(j.key))%s.cfg.WorkerCount] <- j
		}
	}
}
