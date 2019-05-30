// Copyright 2019 PingCAP, Inc.
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
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go/sync2"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/tracing"
	"github.com/pingcap/dm/pkg/utils"
)

var _ Pipe = &SyncPipe{}

// SyncPipe can sync your MySQL data to another MySQL database.
type SyncPipe struct {
	sync.RWMutex

	taskName string

	maxRetry int

	sgk *ShardingGroupKeeper // keeper to keep all sharding (sub) group in this syncer

	ddlExecInfo *DDLExecInfo // DDL execute (ignore) info

	wg    sync.WaitGroup
	jobWg sync.WaitGroup

	fromDB *Conn
	toDBs  []*Conn
	ddlDB  *Conn

	closed sync2.AtomicBool

	count *sync2.AtomicInt64

	checkpoint CheckPoint

	// record process error rather than log.Fatal
	errCh chan error

	// record whether error occurred when execute SQLs
	execErrorDetected sync2.AtomicBool

	execErrors *execErrors

	tracer *tracing.Tracer

	flavor string

	workerCount int
	batch       int

	isSharding bool

	disableCausality bool

	c *causality

	input  chan *PipeData
	output chan *PipeData

	queueBucketMapping []string

	jobs         []chan *job
	jobsClosed   sync2.AtomicBool
	jobsChanLock sync.Mutex

	doAfterFlushCheckpoint func(pos mysql.Position) error

	resolveFunc func()

	closeCh chan interface{}
}

func (s *SyncPipe) newJobChans(count int) {
	s.closeJobChans()
	s.jobs = make([]chan *job, 0, count)
	for i := 0; i < count; i++ {
		s.jobs = append(s.jobs, make(chan *job, 1000))
	}
	s.jobsClosed.Set(false)
}

func (s *SyncPipe) closeJobChans() {
	s.jobsChanLock.Lock()
	defer s.jobsChanLock.Unlock()
	if s.jobsClosed.Get() {
		return
	}
	for _, ch := range s.jobs {
		close(ch)
	}
	s.jobsClosed.Set(true)
}

// Init implements the Pipe interface
// don't forget do something in initFunc, like checkpoint
func (s *SyncPipe) Init(cfg *config.SubTaskConfig, initFunc func() error) (err error) {
	s.taskName = cfg.Name
	s.maxRetry = cfg.MaxRetry
	s.flavor = cfg.Flavor
	s.workerCount = cfg.WorkerCount
	s.batch = cfg.Batch
	s.isSharding = cfg.IsSharding
	s.disableCausality = cfg.DisableCausality

	if err := initFunc(); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// Name implements pipe interface
func (s *SyncPipe) Name() string {
	return "sync"
}

// Update implements pipe interface
func (s *SyncPipe) Update(cfg *config.SubTaskConfig) {
	return
}

// Input implements pipe interface
func (s *SyncPipe) Input() chan *PipeData {
	return s.input
}

// Output implements pipe interface
func (s *SyncPipe) Output() chan *PipeData {
	return s.output
}

// SetResolveFunc implements pipe interface
func (s *SyncPipe) SetResolveFunc(resolveFunc func()) {
	s.resolveFunc = resolveFunc
}

// Wait implements pipe interface
func (s *SyncPipe) Wait() {
	s.jobWg.Wait()
}

func (s *SyncPipe) reportErr(err error) {
	log.Warnf("pipe %s meet error %v", s.Name(), err)
	select {
	case s.errCh <- err:
	case <-s.closeCh:
	}
}

func (s *SyncPipe) addCount(isFinished bool, queueBucket string, tp opType, n int64) {
	m := addedJobsTotal
	if isFinished {
		m = finishedJobsTotal
	}

	switch tp {
	case insert:
		m.WithLabelValues("insert", s.taskName, queueBucket).Add(float64(n))
	case update:
		m.WithLabelValues("update", s.taskName, queueBucket).Add(float64(n))
	case del:
		m.WithLabelValues("del", s.taskName, queueBucket).Add(float64(n))
	case ddl:
		m.WithLabelValues("ddl", s.taskName, queueBucket).Add(float64(n))
	case xid:
		// ignore xid jobs
	case flush:
		m.WithLabelValues("flush", s.taskName, queueBucket).Add(float64(n))
	case skip:
		// ignore skip jobs
	default:
		log.Warnf("unknown optype %v", tp)
	}

	s.count.Add(n)
}

func (s *SyncPipe) checkWait(job *job) bool {
	if job.tp == ddl {
		return true
	}

	if s.checkpoint.CheckGlobalPoint() {
		return true
	}

	return false
}

func (s *SyncPipe) addJob(job *job) error {
	var (
		queueBucket int
		execDDLReq  *pb.ExecDDLRequest
	)
	switch job.tp {
	case xid:
		s.saveGlobalPoint(job.pos)
		return nil
	case flush:
		addedJobsTotal.WithLabelValues("flush", s.taskName, adminQueueName).Inc()
		// ugly code addJob and sync, refine it later
		s.jobWg.Add(s.workerCount)
		for i := 0; i < s.workerCount; i++ {
			s.jobs[i] <- job
		}
		s.jobWg.Wait()
		finishedJobsTotal.WithLabelValues("flush", s.taskName, adminQueueName).Inc()
		return errors.Trace(s.flushCheckPoints())
	case ddl:
		s.jobWg.Wait()
		addedJobsTotal.WithLabelValues("ddl", s.taskName, adminQueueName).Inc()
		s.jobWg.Add(1)
		queueBucket = s.workerCount
		s.jobs[queueBucket] <- job
		if job.ddlExecItem != nil {
			execDDLReq = job.ddlExecItem.req
		}

	case insert, update, del:
		s.jobWg.Add(1)
		queueBucket = int(utils.GenHashKey(job.key)) % s.workerCount
		s.addCount(false, s.queueBucketMapping[queueBucket], job.tp, 1)
		s.jobs[queueBucket] <- job
	}

	if s.tracer.Enable() {
		_, err := s.tracer.CollectSyncerJobEvent(job.traceID, job.traceGID, int32(job.tp), job.pos, job.currentPos, s.queueBucketMapping[queueBucket], job.sql, job.ddls, job.args, execDDLReq, pb.SyncerJobState_queued)
		if err != nil {
			log.Errorf("[syncer] trace error: %s", err)
		}
	}

	wait := s.checkWait(job)
	if wait {
		s.jobWg.Wait()
		s.c.reset()
	}

	switch job.tp {
	case ddl:
		// only save checkpoint for DDL and XID (see above)
		s.saveGlobalPoint(job.pos)
		if len(job.sourceSchema) > 0 {
			log.Infof("save checkpoint %v, schema: %s, table: %s, pos: %v", s.checkpoint, job.sourceSchema, job.sourceTable, job.pos)
			s.checkpoint.SaveTablePoint(job.sourceSchema, job.sourceTable, job.pos)
		}
		// reset sharding group after checkpoint saved
		s.resetShardingGroup(job.targetSchema, job.targetTable)
	case insert, update, del:
		// save job's current pos for DML events
		if len(job.sourceSchema) > 0 {
			log.Infof("save checkpoint %v, schema: %s, table: %s, pos: %v", s.checkpoint, job.sourceSchema, job.sourceTable, job.currentPos)
			s.checkpoint.SaveTablePoint(job.sourceSchema, job.sourceTable, job.currentPos)
		}
	}

	if wait {
		return errors.Trace(s.flushCheckPoints())
	}

	return nil
}

func (s *SyncPipe) saveGlobalPoint(globalPoint mysql.Position) {
	if s.isSharding {
		globalPoint = s.sgk.AdjustGlobalPoint(globalPoint)
	}
	s.checkpoint.SaveGlobalPoint(globalPoint)
}

func (s *SyncPipe) resetShardingGroup(schema, table string) {
	if s.isSharding {
		// for DDL sharding group, reset group after checkpoint saved
		group := s.sgk.Group(schema, table)
		if group != nil {
			group.Reset()
		}
	}
}

// flushCheckPoints flushes previous saved checkpoint in memory to persistent storage, like TiDB
// we flush checkpoints in three cases:
//   1. DDL executed
//   2. at intervals (and job executed)
//   3. pausing / stopping the sync (driven by `s.flushJobs`)
// but when error occurred, we can not flush checkpoint, otherwise data may lost
// and except rejecting to flush the checkpoint, we also need to rollback the checkpoint saved before
//   this should be handled when `s.Run` returned
//
// we may need to refactor the concurrency model to make the work-flow more clearer later
func (s *SyncPipe) flushCheckPoints() error {
	if s.execErrorDetected.Get() {
		log.Warnf("[syncer] error detected when executing SQL job, skip flush checkpoint (%s)", s.checkpoint)
		return nil
	}

	var exceptTables [][]string
	if s.isSharding {
		// flush all checkpoints except tables which are unresolved for sharding DDL
		exceptTables = s.sgk.UnresolvedTables()
		log.Infof("[syncer] flush checkpoints except for tables %v", exceptTables)
	}
	err := s.checkpoint.FlushPointsExcept(exceptTables)
	if err != nil {
		return errors.Annotatef(err, "flush checkpoint %s", s.checkpoint)
	}
	log.Infof("[syncer] flushed checkpoint %s", s.checkpoint)

	// update current active relay log after checkpoint flushed
	err = s.doAfterFlushCheckpoint(s.checkpoint.GlobalPoint())
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// Run implements Pipe interface
func (s *SyncPipe) Run() {
	s.closeCh = make(chan interface{})
	s.input = make(chan *PipeData, 10)
	s.c = newCausality()
	s.newJobChans(s.workerCount + 1)
	s.jobsClosed.Set(true) // not open yet
	s.execErrorDetected.Set(false)
	s.resetExecErrors()

	go func() {
		for {
			select {
			case pipeData, ok := <-s.input:
				log.Debugf("sync_pipe get pipeData: %v", pipeData)
				if !ok {
					return
				}
				if err := s.commitJobs(pipeData); err != nil {
					s.reportErr(err)
					break
				}
				s.resolveFunc()
			case <-s.closeCh:
				break
			}
		}
	}()

	s.queueBucketMapping = make([]string, 0, s.workerCount+1)
	for i := 0; i < s.workerCount; i++ {
		s.wg.Add(1)
		name := queueBucketName(i)
		s.queueBucketMapping = append(s.queueBucketMapping, name)
		go func(i int, n string) {
			s.sync(n, s.toDBs[i], s.jobs[i])
		}(i, name)
	}

	s.queueBucketMapping = append(s.queueBucketMapping, adminQueueName)
	s.wg.Add(1)
	go func() {
		s.sync(adminQueueName, s.ddlDB, s.jobs[s.workerCount])
	}()
}

func (s *SyncPipe) sync(queueBucket string, db *Conn, jobChan chan *job) {
	defer s.wg.Done()

	idx := 0
	count := s.batch
	jobs := make([]*job, 0, count)
	tpCnt := make(map[opType]int64)

	clearF := func() {
		for i := 0; i < idx; i++ {
			s.jobWg.Done()
		}

		idx = 0
		jobs = jobs[0:0]
		for tpName, v := range tpCnt {
			s.addCount(true, queueBucket, tpName, v)
			tpCnt[tpName] = 0
		}
	}

	fatalF := func(err error, errType pb.ErrorType) {
		s.execErrorDetected.Set(true)
		s.reportErr(err)
		clearF()
	}

	executeSQLs := func() error {
		if len(jobs) == 0 {
			return nil
		}
		errCtx := db.executeSQLJob(jobs, s.maxRetry)
		var err error
		if errCtx != nil {
			err = errCtx.err
			s.appendExecErrors(errCtx)
		}

		if s.tracer.Enable() {
			syncerJobState := s.tracer.FinishedSyncerJobState(err)
			for _, job := range jobs {
				_, err2 := s.tracer.CollectSyncerJobEvent(job.traceID, job.traceGID, int32(job.tp), job.pos, job.currentPos, queueBucket, job.sql, job.ddls, nil, nil, syncerJobState)
				if err2 != nil {
					log.Errorf("[syncer] trace error: %s", err2)
				}
			}
		}

		return errors.Trace(err)
	}

	var err error

	for {
		select {
		case sqlJob, ok := <-jobChan:
			if !ok {
				return
			}
			idx++

			if sqlJob.tp == ddl {
				err = executeSQLs()
				if err != nil {
					fatalF(err, pb.ErrorType_ExecSQL)
					break
				}

				if sqlJob.ddlExecItem != nil && sqlJob.ddlExecItem.req != nil && !sqlJob.ddlExecItem.req.Exec {
					log.Infof("[syncer] ignore sharding DDLs %v", sqlJob.ddls)
				} else {
					args := make([][]interface{}, len(sqlJob.ddls))
					err = db.executeSQL(sqlJob.ddls, args, 1)
					if err != nil && ignoreDDLError(err) {
						err = nil
					}

					if s.tracer.Enable() {
						syncerJobState := s.tracer.FinishedSyncerJobState(err)
						var execDDLReq *pb.ExecDDLRequest
						if sqlJob.ddlExecItem != nil {
							execDDLReq = sqlJob.ddlExecItem.req
						}
						_, err := s.tracer.CollectSyncerJobEvent(sqlJob.traceID, sqlJob.traceGID, int32(sqlJob.tp), sqlJob.pos, sqlJob.currentPos, queueBucket, sqlJob.sql, sqlJob.ddls, nil, execDDLReq, syncerJobState)
						if err != nil {
							log.Errorf("[syncer] trace error: %s", err)
						}
					}
				}
				if err != nil {
					s.appendExecErrors(&ExecErrorContext{
						err:  err,
						pos:  sqlJob.currentPos,
						jobs: fmt.Sprintf("%v", sqlJob.ddls),
					})
				}

				if s.isSharding {
					// for sharding DDL syncing, send result back
					if sqlJob.ddlExecItem != nil {
						sqlJob.ddlExecItem.resp <- errors.Trace(err)
					}
					s.ddlExecInfo.ClearBlockingDDL()
				}
				if err != nil {
					// error then pause.
					fatalF(err, pb.ErrorType_ExecSQL)
					break
				}

				tpCnt[sqlJob.tp] += int64(len(sqlJob.ddls))
				clearF()

			} else if sqlJob.tp != flush && len(sqlJob.sql) > 0 {
				jobs = append(jobs, sqlJob)
				tpCnt[sqlJob.tp]++
			}

			if idx >= count || sqlJob.tp == flush {
				err = executeSQLs()
				if err != nil {
					fatalF(err, pb.ErrorType_ExecSQL)
					break
				}
				clearF()
			}

		case <-s.closeCh:
			log.Infof("[sync_pipe] stop sync %s", queueBucket)
			return

		default:
			if len(jobs) > 0 {
				err = executeSQLs()
				if err != nil {
					fatalF(err, pb.ErrorType_ExecSQL)
					break
				}
				clearF()
			} else {
				time.Sleep(waitTime)
			}
		}
	}
}

func (s *SyncPipe) commitJob(tp opType, sourceSchema, sourceTable, targetSchema, targetTable, sql string, args []interface{}, keys []string, retry bool, pos, cmdPos mysql.Position, gs gtid.Set, traceID string) error {
	key, err := s.resolveCasuality(keys)
	if err != nil {
		return errors.Errorf("resolve karam error %v", err)
	}
	job := newJob(tp, sourceSchema, sourceTable, targetSchema, targetTable, sql, args, key, pos, cmdPos, gs, traceID)
	err = s.addJob(job)
	return errors.Trace(err)
}

func (s *SyncPipe) resolveCasuality(keys []string) (string, error) {
	if s.disableCausality {
		if len(keys) > 0 {
			return keys[0], nil
		}
		return "", nil
	}
	if s.c.detectConflict(keys) {
		log.Debug("[causality] meet causality key, will generate a flush job and wait all sqls executed")
		if err := s.flushJobs(); err != nil {
			return "", errors.Trace(err)
		}
		s.c.reset()
	}
	if err := s.c.add(keys); err != nil {
		return "", errors.Trace(err)
	}
	var key string
	if len(keys) > 0 {
		key = keys[0]
	}
	return s.c.get(key), nil
}

// record skip ddl/dml sqls' position
// make newJob's sql argument empty to distinguish normal sql and skips sql
func (s *SyncPipe) recordSkipSQLsPos(pos mysql.Position, gtidSet gtid.Set) error {
	job := newSkipJob(pos, gtidSet)
	err := s.addJob(job)
	return errors.Trace(err)
}

func (s *SyncPipe) flushJobs() error {
	log.Infof("flush all jobs, global checkpoint=%s", s.checkpoint)
	job := newFlushJob()
	err := s.addJob(job)
	return errors.Trace(err)
}

func (s *SyncPipe) isClosed() bool {
	return s.closed.Get()
}

// Close implements Pipe interface
func (s *SyncPipe) Close() {
	close(s.closeCh)
	s.closeJobChans()
	s.wg.Wait()
}

// SetErrorChan implements Pipe interface
func (s *SyncPipe) SetErrorChan(errCh chan error) {
	s.errCh = errCh
}

// Pause pauses the process, and it can be resumed later
// should cancel context from external
// TODO: it is not a true-meaning Pause because you can't stop it by calling Pause only.
func (s *SyncPipe) Pause() {
}

// Resume resumes the paused process
func (s *SyncPipe) Resume(ctx context.Context, pr chan pb.ProcessResult) {
}

// appendExecErrors appends syncer execErrors with new value
func (s *SyncPipe) appendExecErrors(errCtx *ExecErrorContext) {
	s.execErrors.Lock()
	defer s.execErrors.Unlock()
	s.execErrors.errors = append(s.execErrors.errors, errCtx)
}

// resetExecErrors resets syncer execErrors
func (s *SyncPipe) resetExecErrors() {
	s.execErrors.Lock()
	defer s.execErrors.Unlock()
	s.execErrors.errors = make([]*ExecErrorContext, 0)
}

func (s *SyncPipe) commitJobs(pipeData *PipeData) error {
	switch pipeData.tp {
	case null:
		return nil
	case insert, update, del:
		for i, sql := range pipeData.sqls {
			err := s.commitJob(pipeData.tp, pipeData.sourceSchema, pipeData.sourceTable, pipeData.targetSchema, pipeData.targetTable, sql, pipeData.args[i], pipeData.keys[i], true, pipeData.pos, pipeData.currentPos, pipeData.gtidSet, pipeData.traceID)
			if err != nil {
				return errors.Trace(err)
			}
		}
	case ddl:
		ddlJob := newDDLJob(pipeData.sourceSchema, pipeData.sourceTable, pipeData.targetSchema, pipeData.targetTable, pipeData.ddls, pipeData.pos, pipeData.currentPos, pipeData.gtidSet, pipeData.ddlExecItem, pipeData.traceID)
		err := s.addJob(ddlJob)
		if err != nil {
			return errors.Trace(err)
		}
	case xid:
		err := s.addJob(newXIDJob(pipeData.currentPos, pipeData.currentPos, nil, pipeData.traceID))
		if err != nil {
			return errors.Trace(err)
		}
	case flush:
		err := s.addJob(newFlushJob())
		if err != nil {
			return errors.Trace(err)
		}
	case skip:
		err := s.addJob(newSkipJob(pipeData.pos, nil))
		if err != nil {
			return errors.Trace(err)
		}
	case rotate:
		return nil
	}

	return nil
}
