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
	"github.com/pingcap/dm/dm/unit"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/streamer"
	"github.com/pingcap/dm/pkg/tracing"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/pingcap/dm/syncer/sql-operator"
	sm "github.com/pingcap/dm/syncer/safe-mode"
	"github.com/pingcap/failpoint"
)

var (
//retryTimeout    = 3 * time.Second
//waitTime        = 10 * time.Millisecond
//eventTimeout    = 1 * time.Minute
//maxEventTimeout = 1 * time.Hour
//statusTime      = 30 * time.Second

// MaxDDLConnectionTimeoutMinute also used by SubTask.ExecuteDDL
//MaxDDLConnectionTimeoutMinute = 10

//maxDMLConnectionTimeout = "1m"
//maxDDLConnectionTimeout = fmt.Sprintf("%dm", MaxDDLConnectionTimeoutMinute)

//adminQueueName     = "admin queue"
//defaultBucketCount = 8
)

var _ Pipe = &SyncPipe{}

// SyncPipe can sync your MySQL data to another MySQL database.
type SyncPipe struct {
	sync.RWMutex

	taskName string

	maxRetry int

	//cfg     *config.SubTaskConfig

	//syncer *Syncer
	sgk *ShardingGroupKeeper // keeper to keep all sharding (sub) group in this syncer


	ddlInfoCh   chan *pb.DDLInfo // DDL info pending to sync, only support sync one DDL lock one time, refine if needed
	ddlExecInfo *DDLExecInfo     // DDL execute (ignore) info

	wg    sync.WaitGroup
	jobWg sync.WaitGroup

	tables map[string]*table // table cache: `target-schema`.`target-table` -> table

	fromDB *Conn
	toDBs  []*Conn
	ddlDB  *Conn



	closed sync2.AtomicBool

	//start    time.Time
	//lastTime time.Time

	lastCount sync2.AtomicInt64
	count     sync2.AtomicInt64
	totalTps  sync2.AtomicInt64
	tps       sync2.AtomicInt64

	done chan struct{}

	checkpoint CheckPoint
	onlineDDL  OnlinePlugin

	// record process error rather than log.Fatal
	runFatalChan chan *pb.ProcessError
	// record whether error occurred when execute SQLs
	execErrorDetected sync2.AtomicBool

	execErrors struct {
		sync.Mutex
		errors []*ExecErrorContext
	}

	sqlOperatorHolder *operator.Holder

	heartbeat *Heartbeat

	readerHub *streamer.ReaderHub

	tracer *tracing.Tracer

	currentPosMu struct {
		sync.RWMutex
		currentPos mysql.Position // use to calc remain binlog size
	}

	safeMode bool
	flavor string

	workerCount int
	batch int

	isSharding bool

	disableCausality bool

	c *causality

	input chan *PipeData

	once sync.Once

	queueBucketMapping []string

	jobs               []chan *job
	jobsClosed         sync2.AtomicBool
	jobsChanLock       sync.Mutex

	lastPos mysql.Position

	ctx  context.Context
}

/*
// NewSyncPipe creates a new SyncPipe.
func NewSyncPipe(cfg *config.SubTaskConfig) *SyncPipe {
	syncer := new(SyncPipe)
	syncer.cfg = cfg
	syncer.jobsClosed.Set(true) // not open yet
	syncer.closed.Set(false)
	syncer.lastCount.Set(0)
	syncer.count.Set(0)
	syncer.tables = make(map[string]*table)
	syncer.c = newCausality()
	syncer.done = make(chan struct{})
	syncer.checkpoint = NewRemoteCheckPoint(cfg, syncer.checkpointID())
	syncer.tracer = tracing.GetTracer()

	syncer.sqlOperatorHolder = operator.NewHolder()
	syncer.readerHub = streamer.GetReaderHub()

	if cfg.isSharding {
		// only need to sync DDL in sharding mode
		// for sharding group's config, we should use a different ServerID
		// now, use 2**32 -1 - config's ServerID simply
		// maybe we can refactor to remove RemoteBinlog support in DM
		//syncer.shardingSyncCfg = syncer.syncCfg
		//syncer.shardingSyncCfg.ServerID = math.MaxUint32 - syncer.syncCfg.ServerID
		syncer.sgk = NewShardingGroupKeeper()
		syncer.ddlInfoCh = make(chan *pb.DDLInfo, 1)
		syncer.ddlExecInfo = NewDDLExecInfo()
	}

	return syncer
}
*/

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
// don't forget init checkpoint and sgk
func (s *SyncPipe) Init(cfg *config.SubTaskConfig, initFuc func() error) error {
	//s.cfg = cfg
	s.taskName = cfg.Name
	s.maxRetry = cfg.MaxRetry
	s.flavor = cfg.Flavor

	s.workerCount = cfg.WorkerCount
	s.batch = cfg.Batch

	s.isSharding = cfg.IsSharding

	s.c = newCausality()

	return initFuc()
}

// Name is the pipe's name
func (s *SyncPipe) Name() string {
	return "sync"
}

// Update ...
func (s *SyncPipe) Update() {
	return
}

// Input data
func (s *SyncPipe) Input() chan *PipeData {
	inCh := make(chan *PipeData)

	return inCh
}

// Process ...
func (s *SyncPipe) Process(pipeData *PipeData) {
	once.Do(s.runBackground)
	
	// transform pipeData to jobs
	if err := s.commitJobs(pipeData); err != nil {
		s.reportErr(err)
	}

	return
}

// SetNextPipe ...
func (s *SyncPipe) SetNextPipe(pipe Pipe) {
	
	return
}

// Report ...
func (s *SyncPipe) Report() {
	return
}

func (s *SyncPipe) reportErr(err error) {
	log.Warn("pipe %s meet error %v", s.Name(), err)
	// TODO: send error to channel and stop sync
}

/*
// Process implements the dm.Unit interface.
func (s *SyncPipe) process(ctx context.Context, pr chan pb.ProcessResult) {
	syncerExitWithErrorCounter.WithLabelValues(s.taskName).Add(0)

	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// create new done chan
	s.done = make(chan struct{})
	// create new job chans
	s.newJobChans(s.s.workerCount + 1)

	s.runFatalChan = make(chan *pb.ProcessError, s.s.workerCount+1)
	s.execErrorDetected.Set(false)
	s.resetExecErrors()
	errs := make([]*pb.ProcessError, 0, 2)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			err, ok := <-s.runFatalChan
			if !ok {
				return
			}
			cancel() // cancel s.Run
			syncerExitWithErrorCounter.WithLabelValues(s.taskName).Inc()
			errs = append(errs, err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-newCtx.Done() // ctx or newCtx
		if s.ddlExecInfo != nil {
			s.ddlExecInfo.Close() // let Run can return
		}
	}()

	s.closeJobChans()     // Run returned, all jobs sent, we can close s.jobs
	s.wg.Wait()           // wait for sync goroutine to return
	close(s.runFatalChan) // Run returned, all potential fatal sent to s.runFatalChan
	wg.Wait()             // wait for receive all fatal from s.runFatalChan

	isCanceled := false
	if len(errs) == 0 {
		select {
		case <-ctx.Done():
			isCanceled = true
		default:
		}
	} else {
		// pause because of error occurred
		s.Pause()
	}

	// try to rollback checkpoints, if they already flushed, no effect
	prePos := s.checkpoint.GlobalPoint()
	s.checkpoint.Rollback()
	currPos := s.checkpoint.GlobalPoint()
	if prePos.Compare(currPos) != 0 {
		log.Warnf("[syncer] rollback global checkpoint from %v to %v because error occurred", prePos, currPos)
	}

	pr <- pb.ProcessResult{
		IsCanceled: isCanceled,
		Errors:     errs,
	}
}
*/

func (s *SyncPipe) getMasterStatus() (mysql.Position, gtid.Set, error) {
	return utils.GetMasterStatus(s.fromDB.db, s.flavor)
}

func (s *SyncPipe) getTableFromDB(db *Conn, schema string, name string) (*table, error) {
	table := &table{}
	table.schema = schema
	table.name = name
	table.indexColumns = make(map[string][]*column)

	err := getTableColumns(db, table, s.maxRetry)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = getTableIndex(db, table, s.maxRetry)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(table.columns) == 0 {
		return nil, errors.Errorf("invalid table %s.%s", schema, name)
	}

	return table, nil
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
		//execDDLReq  *pb.ExecDDLRequest
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
		/*
			if job.ddlExecItem != nil {
				execDDLReq = job.ddlExecItem.req
			}
		*/
	case insert, update, del:
		s.jobWg.Add(1)
		queueBucket = int(utils.GenHashKey(job.key)) % s.workerCount
		s.addCount(false, s.queueBucketMapping[queueBucket], job.tp, 1)
		s.jobs[queueBucket] <- job
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
			s.checkpoint.SaveTablePoint(job.sourceSchema, job.sourceTable, job.pos)
		}
		// reset sharding group after checkpoint saved
		s.resetShardingGroup(job.targetSchema, job.targetTable)
	case insert, update, del:
		// save job's current pos for DML events
		if len(job.sourceSchema) > 0 {
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

	/*
		// update current active relay log after checkpoint flushed
		err = s.updateActiveRelayLog(s.checkpoint.GlobalPoint())
		if err != nil {
			return errors.Trace(err)
		}
	*/
	return nil
}

func (s *SyncPipe) runBackground() {
	s.queueBucketMapping = make([]string, 0, s.workerCount+1)
	for i := 0; i < s.workerCount; i++ {
		s.wg.Add(1)
		name := queueBucketName(i)
		s.queueBucketMapping = append(s.queueBucketMapping, name)
		go func(i int, n string) {
			ctx1, cancel := context.WithCancel(s.ctx)
			s.sync(ctx1, n, s.toDBs[i], s.jobs[i])
			cancel()
		}(i, name)
	}

	s.queueBucketMapping = append(s.queueBucketMapping, adminQueueName)
	s.wg.Add(1)
	go func() {
		ctx2, cancel := context.WithCancel(s.ctx)
		s.sync(ctx2, adminQueueName, s.ddlDB, s.jobs[s.workerCount])
		cancel()
	}()
}

func (s *SyncPipe) sync(ctx context.Context, queueBucket string, db *Conn, jobChan chan *job) {
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
		s.runFatalChan <- unit.NewProcessError(errType, errors.ErrorStack(err))
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
					continue
				}

				if sqlJob.ddlExecItem != nil && sqlJob.ddlExecItem.req != nil && !sqlJob.ddlExecItem.req.Exec {
					log.Infof("[syncer] ignore sharding DDLs %v", sqlJob.ddls)
				} else {
					args := make([][]interface{}, len(sqlJob.ddls))
					err = db.executeSQL(sqlJob.ddls, args, 1)
					if err != nil && ignoreDDLError(err) {
						err = nil
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
					// errro then pause.
					fatalF(err, pb.ErrorType_ExecSQL)
					continue
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
					continue
				}
				clearF()
			}

		default:
			if len(jobs) > 0 {
				err = executeSQLs()
				if err != nil {
					fatalF(err, pb.ErrorType_ExecSQL)
					continue
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

// Close closes syncer.
func (s *SyncPipe) Close() {

}

// Pause pauses the process, and it can be resumed later
// should cancel context from external
// TODO: it is not a true-meaning Pause because you can't stop it by calling Pause only.
func (s *SyncPipe) Pause() {
}

// Resume resumes the paused process
func (s *SyncPipe) Resume(ctx context.Context, pr chan pb.ProcessResult) {
}

// ExecuteDDL executes or skips a hanging-up DDL when in sharding
func (s *SyncPipe) ExecuteDDL(ctx context.Context, execReq *pb.ExecDDLRequest) (<-chan error, error) {
	if len(s.ddlExecInfo.BlockingDDLs()) == 0 {
		return nil, errors.New("process unit not waiting for sharding DDL to sync")
	}
	item := newDDLExecItem(execReq)
	err := s.ddlExecInfo.Send(ctx, item)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return item.resp, nil
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

func (s *SyncPipe) enableSafeModeInitializationPhase(ctx context.Context, safeMode *sm.SafeMode) {
	safeMode.Reset() // in initialization phase, reset first
	safeMode.Add(1)  // try to enable

	if s.safeMode {
		safeMode.Add(1) // add 1 but should no corresponding -1
		log.Info("[syncer] enable safe-mode by config")
	}

	go func() {
		defer func() {
			err := safeMode.Add(-1) // try to disable after 5 minutes
			if err != nil {
				// send error to the fatal chan to interrupt the process
				s.runFatalChan <- unit.NewProcessError(pb.ErrorType_UnknownError, errors.ErrorStack(err))
			}
		}()

		initPhaseSeconds := 300

		failpoint.Inject("SafeModeInitPhaseSeconds", func(val failpoint.Value) {
			seconds, _ := val.(int)
			initPhaseSeconds = seconds
			log.Infof("[failpoint] set initPhaseSeconds to %d", seconds)
		})

		select {
		case <-ctx.Done():
		case <-time.After(time.Duration(initPhaseSeconds) * time.Second):
		}
	}()
}

func (s *SyncPipe) commitJobs(pipeData *PipeData) error {
	switch pipeData.tp {
	case null:
		return nil
	case insert, update, del:
		for i, sql := range pipeData.sqls {
			// lastPos
			err := s.commitJob(pipeData.tp, pipeData.sourceSchema, pipeData.sourceTable, pipeData.targetSchema, pipeData.targetTable, sql, pipeData.args[i], pipeData.keys[i], true, s.lastPos, pipeData.currentPos, pipeData.gtidSet, pipeData.traceID)
			if err != nil {
				return errors.Trace(err)
			}
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
		err := s.addJob(newSkipJob(s.lastPos, nil))
		if err != nil {
			return errors.Trace(err)
		}
	case rotate:
		return nil
	}

	return nil
}