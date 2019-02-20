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
	"fmt"
	"math"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	cm "github.com/pingcap/tidb-tools/pkg/column-mapping"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/sync2"
	"golang.org/x/net/context"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/unit"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/streamer"
	"github.com/pingcap/dm/pkg/utils"
	sm "github.com/pingcap/dm/syncer/safe-mode"
	"github.com/pingcap/dm/syncer/sql-operator"
)

var (
	maxRetryCount = 100

	retryTimeout    = 3 * time.Second
	waitTime        = 10 * time.Millisecond
	eventTimeout    = 1 * time.Minute
	maxEventTimeout = 1 * time.Hour
	statusTime      = 30 * time.Second

	// MaxDDLConnectionTimeoutMinute also used by SubTask.ExecuteDDL
	MaxDDLConnectionTimeoutMinute = 10

	maxDMLConnectionTimeout = "1m"
	maxDDLConnectionTimeout = fmt.Sprintf("%dm", MaxDDLConnectionTimeoutMinute)

	adminQueueName     = "admin queue"
	defaultBucketCount = 8
	queueBucketMapping []string
)

// BinlogType represents binlog sync type
type BinlogType uint8

// binlog sync type
const (
	RemoteBinlog BinlogType = iota + 1
	LocalBinlog
)

// Syncer can sync your MySQL data to another MySQL database.
type Syncer struct {
	sync.RWMutex

	cfg     *config.SubTaskConfig
	syncCfg replication.BinlogSyncerConfig

	shardingSyncCfg replication.BinlogSyncerConfig // used by sharding group to re-consume DMLs
	sgk             *ShardingGroupKeeper           // keeper to keep all sharding (sub) group in this syncer
	ddlInfoCh       chan *pb.DDLInfo               // DDL info pending to sync, only support sync one DDL lock one time, refine if needed
	ddlExecInfo     *DDLExecInfo                   // DDL execute (ignore) info
	injectEventCh   chan *replication.BinlogEvent  // extra binlog event chan, used to inject binlog event into the main for loop

	// TODO: extract to interface?
	syncer      *replication.BinlogSyncer
	localReader *streamer.BinlogReader
	binlogType  BinlogType

	wg    sync.WaitGroup
	jobWg sync.WaitGroup

	tables       map[string]*table   // table cache: `target-schema`.`target-table` -> table
	cacheColumns map[string][]string // table columns cache: `target-schema`.`target-table` -> column names list

	fromDB *Conn
	toDBs  []*Conn
	ddlDB  *Conn

	jobs         []chan *job
	jobsClosed   sync2.AtomicBool
	jobsChanLock sync.Mutex

	c *causality

	tableRouter   *router.Table
	binlogFilter  *bf.BinlogEvent
	columnMapping *cm.Mapping
	bwList        *filter.Filter

	closed sync2.AtomicBool

	start    time.Time
	lastTime time.Time
	timezone *time.Location

	binlogSizeCount     sync2.AtomicInt64
	lastBinlogSizeCount sync2.AtomicInt64

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

	currentPosMu struct {
		sync.RWMutex
		currentPos mysql.Position // use to calc remain binlog size
	}
}

// NewSyncer creates a new Syncer.
func NewSyncer(cfg *config.SubTaskConfig) *Syncer {
	syncer := new(Syncer)
	syncer.cfg = cfg
	syncer.jobsClosed.Set(true) // not open yet
	syncer.closed.Set(false)
	syncer.lastBinlogSizeCount.Set(0)
	syncer.binlogSizeCount.Set(0)
	syncer.lastCount.Set(0)
	syncer.count.Set(0)
	syncer.tables = make(map[string]*table)
	syncer.cacheColumns = make(map[string][]string)
	syncer.c = newCausality()
	syncer.tableRouter, _ = router.NewTableRouter(cfg.CaseSensitive, []*router.TableRule{})
	syncer.done = make(chan struct{})
	syncer.bwList = filter.New(cfg.CaseSensitive, cfg.BWList)
	syncer.checkpoint = NewRemoteCheckPoint(cfg, syncer.checkpointID())
	syncer.injectEventCh = make(chan *replication.BinlogEvent)
	syncer.setTimezone()

	syncer.syncCfg = replication.BinlogSyncerConfig{
		ServerID:                uint32(syncer.cfg.ServerID),
		Flavor:                  syncer.cfg.Flavor,
		Host:                    syncer.cfg.From.Host,
		Port:                    uint16(syncer.cfg.From.Port),
		User:                    syncer.cfg.From.User,
		Password:                syncer.cfg.From.Password,
		UseDecimal:              true,
		VerifyChecksum:          true,
		TimestampStringLocation: syncer.timezone,
	}

	syncer.binlogType = toBinlogType(cfg.BinlogType)
	syncer.sqlOperatorHolder = operator.NewHolder()
	syncer.readerHub = streamer.GetReaderHub()

	if cfg.IsSharding {
		// only need to sync DDL in sharding mode
		// for sharding group's config, we should use a different ServerID
		// now, use 2**32 -1 - config's ServerID simply
		// maybe we can refactor to remove RemoteBinlog support in DM
		syncer.shardingSyncCfg = syncer.syncCfg
		syncer.shardingSyncCfg.ServerID = math.MaxUint32 - syncer.syncCfg.ServerID
		syncer.sgk = NewShardingGroupKeeper()
		syncer.ddlInfoCh = make(chan *pb.DDLInfo, 1)
		syncer.ddlExecInfo = NewDDLExecInfo()
	}

	return syncer
}

func (s *Syncer) newJobChans(count int) {
	s.closeJobChans()
	s.jobs = make([]chan *job, 0, count)
	for i := 0; i < count; i++ {
		s.jobs = append(s.jobs, make(chan *job, 1000))
	}
	s.jobsClosed.Set(false)
}

func (s *Syncer) closeJobChans() {
	if s.jobsClosed.Get() {
		return
	}

	s.jobsChanLock.Lock()
	for _, ch := range s.jobs {
		close(ch)
	}
	s.jobsChanLock.Unlock()

	s.jobsClosed.Set(true)
}

// Type implements Unit.Type
func (s *Syncer) Type() pb.UnitType {
	return pb.UnitType_Sync
}

// Init initializes syncer for a sync task, but not start Process.
// if fail, it should not call s.Close.
// some check may move to checker later.
func (s *Syncer) Init() error {
	err := s.createDBs()
	if err != nil {
		return errors.Trace(err)
	}

	s.binlogFilter, err = bf.NewBinlogEvent(s.cfg.CaseSensitive, s.cfg.FilterRules)
	if err != nil {
		return errors.Trace(err)
	}

	if len(s.cfg.ColumnMappingRules) > 0 {
		s.columnMapping, err = cm.NewMapping(s.cfg.CaseSensitive, s.cfg.ColumnMappingRules)
		if err != nil {
			return errors.Trace(err)
		}
	}

	if s.cfg.OnlineDDLScheme != "" {
		fn, ok := OnlineDDLSchemes[s.cfg.OnlineDDLScheme]
		if !ok {
			return errors.NotSupportedf("online ddl scheme (%s)", s.cfg.OnlineDDLScheme)
		}
		s.onlineDDL, err = fn(s.cfg)
		if err != nil {
			return errors.Trace(err)
		}
	}

	err = s.genRouter()
	if err != nil {
		return errors.Trace(err)
	}

	if s.cfg.IsSharding {
		err = s.initShardingGroups()
		if err != nil {
			return errors.Trace(err)
		}
	}

	err = s.checkpoint.Init()
	if err != nil {
		return errors.Trace(err)
	}

	if s.cfg.RemoveMeta {
		err = s.checkpoint.Clear()
		if err != nil {
			return errors.Annotate(err, "clear checkpoint in syncer")
		}

		if s.onlineDDL != nil {
			err = s.onlineDDL.Clear()
			if err != nil {
				return errors.Annotate(err, "clear online ddl in syncer")
			}
		}
		log.Info("[syncer] all previous meta cleared")
	}

	err = s.checkpoint.Load()
	if err != nil {
		return errors.Trace(err)
	}
	if s.cfg.EnableHeartbeat {
		s.heartbeat, err = GetHeartbeat(&HeartbeatConfig{
			serverID:  s.cfg.ServerID,
			masterCfg: s.cfg.From})
		if err != nil {
			return errors.Trace(err)
		}
		err = s.heartbeat.AddTask(s.cfg.Name)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// when Init syncer, set active relay log info
	err = s.setInitActiveRelayLog()
	if err != nil {
		return errors.Trace(err)
	}

	// init successfully, close done chan to make Syncer can be closed
	// when Process started, we will re-create done chan again
	// NOTE: we should refactor the Concurrency Model some day
	s.done = make(chan struct{})
	close(s.done)
	return nil
}

// initShardingGroups initializes sharding groups according to source MySQL, filter rules and router rules
// NOTE: now we don't support modify router rules after task has started
func (s *Syncer) initShardingGroups() error {
	// fetch tables from source and filter them
	sourceTables, err := utils.FetchAllDoTables(s.fromDB.db, s.bwList)
	if err != nil {
		return errors.Trace(err)
	}

	// clear old sharding group
	s.sgk.Clear()

	// convert according to router rules
	// target-schema -> target-table -> source-IDs
	mapper := make(map[string]map[string][]string, len(sourceTables))
	for schema, tables := range sourceTables {
		for _, table := range tables {
			targetSchema, targetTable := s.renameShardingSchema(schema, table)
			mSchema, ok := mapper[targetSchema]
			if !ok {
				mapper[targetSchema] = make(map[string][]string, len(tables))
				mSchema = mapper[targetSchema]
			}
			_, ok = mSchema[targetTable]
			if !ok {
				mSchema[targetTable] = make([]string, 0, len(tables))
			}
			ID, _ := GenTableID(schema, table)
			mSchema[targetTable] = append(mSchema[targetTable], ID)
		}
	}

	// add sharding group
	for targetSchema, mSchema := range mapper {
		for targetTable, sourceIDs := range mSchema {
			_, _, _, _, err := s.sgk.AddGroup(targetSchema, targetTable, sourceIDs, false)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}

	log.Debugf("[syncer] initial sharding groups(%d): %v", len(s.sgk.Groups()), s.sgk.Groups())

	return nil
}

// IsFreshTask implements Unit.IsFreshTask
func (s *Syncer) IsFreshTask() (bool, error) {
	globalPoint := s.checkpoint.GlobalPoint()
	return globalPoint.Compare(minCheckpoint) <= 0, nil
}

// Process implements the dm.Unit interface.
func (s *Syncer) Process(ctx context.Context, pr chan pb.ProcessResult) {
	syncerExitWithErrorCounter.WithLabelValues(s.cfg.Name).Add(0)

	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	if s.binlogType == RemoteBinlog {
		// create new binlog-syncer
		if s.syncer != nil {
			s.closeBinlogSyncer(s.syncer)
		}
		s.syncer = replication.NewBinlogSyncer(s.syncCfg)
	} else if s.binlogType == LocalBinlog {
		s.localReader = streamer.NewBinlogReader(&streamer.BinlogReaderConfig{
			RelayDir: s.cfg.RelayDir,
			Timezone: s.timezone,
		})
	}
	// create new done chan
	s.done = make(chan struct{})
	// create new job chans
	s.newJobChans(s.cfg.WorkerCount + 1)
	// clear tables info
	s.clearAllTables()

	s.runFatalChan = make(chan *pb.ProcessError, s.cfg.WorkerCount+1)
	s.execErrorDetected.Set(false)
	s.resetExecErrors()
	errs := make([]*pb.ProcessError, 0, 2)

	if s.cfg.IsSharding {
		// every time start to re-sync from resume, we reset status to make it like a fresh syncing
		s.sgk.ResetGroups()
		s.ddlExecInfo.Renew()
	}

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
			syncerExitWithErrorCounter.WithLabelValues(s.cfg.Name).Inc()
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

	wg.Add(1)
	go func() {
		s.runBackgroundJob(newCtx)
		wg.Done()
	}()

	err := s.Run(newCtx)
	if err != nil {
		// returned error rather than sent to runFatalChan
		// cancel goroutines created in s.Run
		cancel()
	}
	s.closeJobChans()     // Run returned, all jobs sent, we can close s.jobs
	s.wg.Wait()           // wait for sync goroutine to return
	close(s.runFatalChan) // Run returned, all potential fatal sent to s.runFatalChan
	wg.Wait()             // wait for receive all fatal from s.runFatalChan

	if err != nil {
		syncerExitWithErrorCounter.WithLabelValues(s.cfg.Name).Inc()
		errs = append(errs, unit.NewProcessError(pb.ErrorType_UnknownError, errors.ErrorStack(err)))
	}

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

func (s *Syncer) getMasterStatus() (mysql.Position, gtid.Set, error) {
	return utils.GetMasterStatus(s.fromDB.db, s.cfg.Flavor)
}

// clearTables is used for clear table cache of given table. this function must
// be called when DDL is applied to this table.
func (s *Syncer) clearTables(schema, table string) {
	key := dbutil.TableName(schema, table)
	delete(s.tables, key)
	delete(s.cacheColumns, key)
}

func (s *Syncer) clearAllTables() {
	s.tables = make(map[string]*table)
	s.cacheColumns = make(map[string][]string)
}

func (s *Syncer) getTableFromDB(db *Conn, schema string, name string) (*table, error) {
	table := &table{}
	table.schema = schema
	table.name = name
	table.indexColumns = make(map[string][]*column)

	err := getTableColumns(db, table, s.cfg.MaxRetry)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = getTableIndex(db, table, s.cfg.MaxRetry)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(table.columns) == 0 {
		return nil, errors.Errorf("invalid table %s.%s", schema, name)
	}

	return table, nil
}

func (s *Syncer) getTable(schema string, table string) (*table, []string, error) {
	key := dbutil.TableName(schema, table)

	value, ok := s.tables[key]
	if ok {
		return value, s.cacheColumns[key], nil
	}

	db := s.toDBs[len(s.toDBs)-1]
	t, err := s.getTableFromDB(db, schema, table)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	// compute cache column list for column mapping
	columns := make([]string, 0, len(t.columns))
	for _, c := range t.columns {
		columns = append(columns, c.name)
	}

	s.tables[key] = t
	s.cacheColumns[key] = columns
	return t, columns, nil
}

func (s *Syncer) addCount(isFinished bool, queueBucket string, tp opType, n int64) {
	m := addedJobsTotal
	if isFinished {
		m = finishedJobsTotal
	}

	switch tp {
	case insert:
		m.WithLabelValues("insert", s.cfg.Name, queueBucket).Add(float64(n))
	case update:
		m.WithLabelValues("update", s.cfg.Name, queueBucket).Add(float64(n))
	case del:
		m.WithLabelValues("del", s.cfg.Name, queueBucket).Add(float64(n))
	case ddl:
		m.WithLabelValues("ddl", s.cfg.Name, queueBucket).Add(float64(n))
	case xid:
		// ignore xid jobs
	case flush:
		m.WithLabelValues("flush", s.cfg.Name, queueBucket).Add(float64(n))
	case skip:
		// ignore skip jobs
	default:
		log.Warnf("unknown optype %v", tp)
	}

	s.count.Add(n)
}

func (s *Syncer) checkWait(job *job) bool {
	if job.tp == ddl {
		return true
	}

	if s.checkpoint.CheckGlobalPoint() {
		return true
	}

	return false
}

func (s *Syncer) addJob(job *job) error {
	switch job.tp {
	case xid:
		s.saveGlobalPoint(job.pos)
		return nil
	case flush:
		addedJobsTotal.WithLabelValues("flush", s.cfg.Name, adminQueueName).Inc()
		// ugly code addJob and sync, refine it later
		s.jobWg.Add(s.cfg.WorkerCount)
		for i := 0; i < s.cfg.WorkerCount; i++ {
			s.jobs[i] <- job
		}
		s.jobWg.Wait()
		finishedJobsTotal.WithLabelValues("flush", s.cfg.Name, adminQueueName).Inc()
		return errors.Trace(s.flushCheckPoints())
	case ddl:
		s.jobWg.Wait()
		addedJobsTotal.WithLabelValues("ddl", s.cfg.Name, adminQueueName).Inc()
		s.jobWg.Add(1)
		s.jobs[s.cfg.WorkerCount] <- job
	case insert, update, del:
		s.jobWg.Add(1)
		idx := int(utils.GenHashKey(job.key)) % s.cfg.WorkerCount
		s.addCount(false, queueBucketMapping[idx], job.tp, 1)
		s.jobs[idx] <- job
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

func (s *Syncer) saveGlobalPoint(globalPoint mysql.Position) {
	if s.cfg.IsSharding {
		globalPoint = s.sgk.AdjustGlobalPoint(globalPoint)
	}
	s.checkpoint.SaveGlobalPoint(globalPoint)
}

func (s *Syncer) resetShardingGroup(schema, table string) {
	if s.cfg.IsSharding {
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
func (s *Syncer) flushCheckPoints() error {
	if s.execErrorDetected.Get() {
		log.Warnf("[syncer] error detected when executing SQL job, skip flush checkpoint (%s)", s.checkpoint)
		return nil
	}

	var exceptTables [][]string
	if s.cfg.IsSharding {
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
	err = s.updateActiveRelayLog(s.checkpoint.GlobalPoint())
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *Syncer) sync(ctx context.Context, queueBucket string, db *Conn, jobChan chan *job) {
	defer s.wg.Done()

	idx := 0
	count := s.cfg.Batch
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
		errCtx := db.executeSQLJob(jobs, s.cfg.MaxRetry)
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
				if s.cfg.IsSharding {
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

// Run starts running for sync, we should guarantee it can rerun when paused.
func (s *Syncer) Run(ctx context.Context) (err error) {
	defer func() {
		close(s.done)
	}()

	parser2, err := utils.GetParser(s.fromDB.db, s.cfg.EnableANSIQuotes)
	if err != nil {
		return errors.Trace(err)
	}

	fresh, err := s.IsFreshTask()
	if err != nil {
		return errors.Trace(err)
	} else if fresh {
		// for fresh task, we try to load checkpoints from meta (file or config item)
		err = s.checkpoint.LoadMeta()
		if err != nil {
			return errors.Trace(err)
		}
	}

	// currentPos is the pos for current received event (End_log_pos in `show binlog events` for mysql)
	// lastPos is the pos for last received (ROTATE / QUERY / XID) event (End_log_pos in `show binlog events` for mysql)
	// we use currentPos to replace and skip binlog event of specfied position and update table checkpoint in sharding ddl
	// we use lastPos to update global checkpoint and table checkpoint
	var (
		currentPos = s.checkpoint.GlobalPoint() // also init to global checkpoint
		lastPos    = s.checkpoint.GlobalPoint()
	)
	log.Infof("replicate binlog from latest checkpoint %+v", lastPos)

	var globalStreamer streamer.Streamer
	if s.binlogType == RemoteBinlog {
		globalStreamer, err = s.getBinlogStreamer(s.syncer, lastPos)
	} else if s.binlogType == LocalBinlog {
		globalStreamer, err = s.getBinlogStreamer(s.localReader, lastPos)
	}
	if err != nil {
		return errors.Trace(err)
	}

	for i := 0; i < s.cfg.WorkerCount; i++ {
		s.wg.Add(1)
		name := queueBucketName(i)
		queueBucketMapping = append(queueBucketMapping, name)
		go func(i int, n string) {
			ctx2, cancel := context.WithCancel(ctx)
			s.sync(ctx2, n, s.toDBs[i], s.jobs[i])
			cancel()
		}(i, name)
	}

	s.wg.Add(1)
	go func() {
		ctx2, cancel := context.WithCancel(ctx)
		s.sync(ctx2, adminQueueName, s.ddlDB, s.jobs[s.cfg.WorkerCount])
		cancel()
	}()

	s.wg.Add(1)
	go func() {
		ctx2, cancel := context.WithCancel(ctx)
		s.printStatus(ctx2)
		cancel()
	}()

	defer func() {
		if err1 := recover(); err1 != nil {
			log.Errorf("panic. err: %s, stack: %s", err1, debug.Stack())
			err = errors.Errorf("panic error: %v", err1)
		}
		// flush the jobs channels, but if error occurred, we should not flush the checkpoints
		if err1 := s.flushJobs(); err1 != nil {
			log.Errorf("fail to finish all jobs error: %v", err1)
		}
	}()

	s.start = time.Now()
	s.lastTime = s.start
	tryReSync := true

	// safeMode makes syncer reentrant.
	// we make each operator reentrant to make syncer reentrant.
	// `replace` and `delete` are naturally reentrant.
	// use `delete`+`replace` to represent `update` can make `update`  reentrant.
	// but there are no ways to make `update` idempotent,
	// if we start syncer at an early position, database must bear a period of inconsistent state,
	// it's eventual consistency.
	safeMode := sm.NewSafeMode()
	s.enableSafeModeInitializationPhase(ctx, safeMode)

	// syncing progress with sharding DDL group
	// 1. use the global streamer to sync regular binlog events
	// 2. sharding DDL synced for some sharding groups
	//    * record first pos, last pos, target schema, target table as re-sync info
	// 3. use the re-sync info recorded in step.2 to create a new streamer
	// 4. use the new streamer re-syncing for this sharding group
	// 5. in sharding group's re-syncing
	//    * ignore other tables' binlog events
	//    * compare last pos with current binlog's pos to determine whether re-sync completed
	// 6. use the global streamer to continue the syncing
	var (
		shardingSyncer      *replication.BinlogSyncer
		shardingReader      *streamer.BinlogReader
		shardingStreamer    streamer.Streamer
		shardingReSyncCh    = make(chan *ShardingReSync, 10)
		shardingReSync      *ShardingReSync
		savedGlobalLastPos  mysql.Position
		latestOp            opType // latest job operation tp
		eventTimeoutCounter time.Duration
	)

	closeShardingSyncer := func() {
		if shardingSyncer != nil {
			s.closeBinlogSyncer(shardingSyncer)
			shardingSyncer = nil
		}
		if shardingReader != nil {
			shardingReader.Close()
			shardingReader = nil
		}
		shardingStreamer = nil
		shardingReSync = nil
		lastPos = savedGlobalLastPos // restore global last pos
	}
	defer func() {
		closeShardingSyncer()
	}()

	for {
		s.currentPosMu.Lock()
		s.currentPosMu.currentPos = currentPos
		s.currentPosMu.Unlock()

		// if there are sharding groups need to re-sync previous ignored DMLs, we use another special streamer
		if shardingStreamer == nil && len(shardingReSyncCh) > 0 {
			// some sharding groups need to re-syncing
			shardingReSync = <-shardingReSyncCh
			savedGlobalLastPos = lastPos // save global last pos
			lastPos = shardingReSync.currPos

			if s.binlogType == RemoteBinlog {
				shardingSyncer = replication.NewBinlogSyncer(s.shardingSyncCfg)
				shardingStreamer, err = s.getBinlogStreamer(shardingSyncer, shardingReSync.currPos)
			} else if s.binlogType == LocalBinlog {
				shardingReader = streamer.NewBinlogReader(&streamer.BinlogReaderConfig{
					RelayDir: s.cfg.RelayDir,
					Timezone: s.timezone,
				})
				shardingStreamer, err = s.getBinlogStreamer(shardingReader, shardingReSync.currPos)
			}
			log.Debugf("[syncer] start using a  special streamer to re-sync DMLs for sharding group %+v", shardingReSync)
		}

		var (
			e   *replication.BinlogEvent
			err error
		)

		// we only inject sqls  in global streaming to avoid DDL position confusion
		if shardingReSync == nil {
			e = s.tryInject(latestOp, currentPos)
			latestOp = null
		}
		if e == nil {
			ctx2, cancel := context.WithTimeout(ctx, eventTimeout)
			if shardingStreamer != nil {
				// use sharding group's special streamer to get binlog event
				e, err = shardingStreamer.GetEvent(ctx2)
			} else {
				e, err = globalStreamer.GetEvent(ctx2)
			}
			cancel()
		}

		startTime := time.Now()
		if err == context.Canceled {
			log.Infof("ready to quit! [%v]", lastPos)
			return nil
		} else if err == context.DeadlineExceeded {
			log.Info("deadline exceeded.")
			eventTimeoutCounter += eventTimeout
			if eventTimeoutCounter < maxEventTimeout {
				err = s.flushJobs()
				if err != nil {
					return errors.Trace(err)
				}
				continue
			}

			eventTimeoutCounter = 0
			if s.needResync() {
				log.Info("timeout, resync")
				if shardingStreamer != nil {
					shardingStreamer, err = s.reopenWithRetry(s.shardingSyncCfg)
				} else {
					globalStreamer, err = s.reopenWithRetry(s.syncCfg)
				}
				if err != nil {
					return errors.Trace(err)
				}
			}
			continue
		}

		if err != nil {
			log.Errorf("get binlog error %v", err)
			// try to re-sync in gtid mode
			if tryReSync && s.cfg.EnableGTID && isBinlogPurgedError(err) && s.cfg.AutoFixGTID {
				time.Sleep(retryTimeout)
				if shardingStreamer != nil {
					shardingStreamer, err = s.reSyncBinlog(s.shardingSyncCfg)
				} else {
					globalStreamer, err = s.reSyncBinlog(s.syncCfg)
				}
				if err != nil {
					return errors.Trace(err)
				}
				tryReSync = false
				continue
			}

			return errors.Trace(err)
		}
		// get binlog event, reset tryReSync, so we can re-sync binlog while syncer meets errors next time
		tryReSync = true
		binlogPosGauge.WithLabelValues("syncer", s.cfg.Name).Set(float64(e.Header.LogPos))
		index, err := streamer.GetBinlogFileIndex(lastPos.Name)
		if err != nil {
			log.Errorf("parse binlog file err %v", err)
		} else {
			binlogFileGauge.WithLabelValues("syncer", s.cfg.Name).Set(index)
		}
		s.binlogSizeCount.Add(int64(e.Header.EventSize))

		log.Debugf("[syncer] receive binlog event with header %+v", e.Header)
		switch ev := e.Event.(type) {
		case *replication.RotateEvent:
			currentPos = mysql.Position{
				Name: string(ev.NextLogName),
				Pos:  uint32(ev.Position),
			}
			if currentPos.Name > lastPos.Name {
				lastPos = currentPos
			}

			if shardingReSync != nil {
				if currentPos.Compare(shardingReSync.currPos) == 1 {
					shardingReSync.currPos = currentPos
				}

				if shardingReSync.currPos.Compare(shardingReSync.latestPos) >= 0 {
					log.Infof("[syncer] sharding group %+v re-syncing completed", shardingReSync)
					closeShardingSyncer()
				} else {
					log.Debugf("[syncer] rotate binlog to %v when re-syncing sharding group %+v", currentPos, shardingReSync)
				}
				continue
			}

			log.Infof("rotate binlog to %v", currentPos)
		case *replication.RowsEvent:
			originSchema, originTable := string(ev.Table.Schema), string(ev.Table.Table)
			schemaName, tableName := s.renameShardingSchema(originSchema, originTable)
			currentPos = mysql.Position{
				Name: lastPos.Name,
				Pos:  e.Header.LogPos,
			}

			if shardingReSync != nil {
				shardingReSync.currPos.Pos = e.Header.LogPos
				if shardingReSync.currPos.Compare(shardingReSync.latestPos) >= 0 {
					log.Infof("[syncer] sharding group %v re-syncing completed", shardingReSync)
					closeShardingSyncer()
					continue
				}
				if shardingReSync.targetSchema != schemaName || shardingReSync.targetTable != tableName {
					// in re-syncing, ignore non current sharding group's events
					log.Debugf("[syncer] skip row event when re-syncing sharding group %+v", shardingReSync)
					continue
				}
			}

			if !s.checkpoint.IsNewerTablePoint(string(ev.Table.Schema), string(ev.Table.Table), currentPos) {
				log.Debugf("[syncer] ignore obsolete row event in %s that is old than checkpoint of table %s.%s", currentPos, string(ev.Table.Schema), string(ev.Table.Table))
				continue
			}

			log.Debugf("source-db:%s table:%s; target-db:%s table:%s, pos: %v, RowsEvent data: %v", originSchema, originTable, schemaName, tableName, currentPos, ev.Rows)

			if s.cfg.EnableHeartbeat {
				s.heartbeat.TryUpdateTaskTs(s.cfg.Name, originSchema, originTable, ev.Rows)
			}

			ignore, err := s.skipDMLEvent(originSchema, originTable, e.Header.EventType)
			if err != nil {
				return errors.Trace(err)
			}
			if ignore {
				binlogSkippedEventsTotal.WithLabelValues("rows", s.cfg.Name).Inc()
				// for RowsEvent, we should record lastPos rather than currentPos
				if err = s.recordSkipSQLsPos(lastPos, nil); err != nil {
					return errors.Trace(err)
				}

				continue
			}

			if s.cfg.IsSharding {
				source, _ := GenTableID(string(ev.Table.Schema), string(ev.Table.Table))
				if s.sgk.InSyncing(schemaName, tableName, source) {
					// current source is in sharding DDL syncing, ignore DML
					log.Debugf("[syncer] source %s is in sharding DDL syncing, ignore Rows event %v", source, currentPos)
					continue
				}
			}

			table, columns, err := s.getTable(schemaName, tableName)
			if err != nil {
				return errors.Trace(err)
			}
			rows, err := s.mappingDML(originSchema, originTable, columns, ev.Rows)
			if err != nil {
				return errors.Trace(err)
			}

			var (
				applied bool
				sqls    []string
				keys    [][]string
				args    [][]interface{}
			)

			// for RowsEvent, one event may have multi SQLs and multi keys, (eg. INSERT INTO t1 VALUES (11, 12), (21, 22) )
			// to cover them dispatched to different channels, we still apply operator here
			// ugly, but I have no better solution yet.
			applied, sqls, err = s.tryApplySQLOperator(currentPos, nil) // forbidden sql-pattern for DMLs
			if err != nil {
				return errors.Trace(err)
			}

			switch e.Header.EventType {
			case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				if !applied {
					sqls, keys, args, err = genInsertSQLs(table.schema, table.name, rows, table.columns, table.indexColumns)
					if err != nil {
						return errors.Errorf("gen insert sqls failed: %v, schema: %s, table: %s", errors.Trace(err), table.schema, table.name)
					}
				}
				binlogEvent.WithLabelValues("write_rows", s.cfg.Name).Observe(time.Since(startTime).Seconds())

				for i := range sqls {
					var arg []interface{}
					var key []string
					if args != nil {
						arg = args[i]
					}
					if keys != nil {
						key = keys[i]
					}
					err = s.commitJob(insert, string(ev.Table.Schema), string(ev.Table.Table), table.schema, table.name, sqls[i], arg, key, true, lastPos, currentPos, nil)
					if err != nil {
						return errors.Trace(err)
					}
				}
			case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				if !applied {
					sqls, keys, args, err = genUpdateSQLs(table.schema, table.name, rows, table.columns, table.indexColumns, safeMode.Enable())
					if err != nil {
						return errors.Errorf("gen update sqls failed: %v, schema: %s, table: %s", err, table.schema, table.name)
					}
				}
				binlogEvent.WithLabelValues("update_rows", s.cfg.Name).Observe(time.Since(startTime).Seconds())

				for i := range sqls {
					var arg []interface{}
					var key []string
					if args != nil {
						arg = args[i]
					}
					if keys != nil {
						key = keys[i]
					}

					err = s.commitJob(update, string(ev.Table.Schema), string(ev.Table.Table), table.schema, table.name, sqls[i], arg, key, true, lastPos, currentPos, nil)
					if err != nil {
						return errors.Trace(err)
					}
				}
			case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				if !applied {
					sqls, keys, args, err = genDeleteSQLs(table.schema, table.name, rows, table.columns, table.indexColumns)
					if err != nil {
						return errors.Errorf("gen delete sqls failed: %v, schema: %s, table: %s", err, table.schema, table.name)
					}
				}
				binlogEvent.WithLabelValues("delete_rows", s.cfg.Name).Observe(time.Since(startTime).Seconds())

				for i := range sqls {
					var arg []interface{}
					var key []string
					if args != nil {
						arg = args[i]
					}
					if keys != nil {
						key = keys[i]
					}

					err = s.commitJob(del, string(ev.Table.Schema), string(ev.Table.Table), table.schema, table.name, sqls[i], arg, key, true, lastPos, currentPos, nil)
					if err != nil {
						return errors.Trace(err)
					}
				}
			}
		case *replication.QueryEvent:
			currentPos = mysql.Position{
				Name: lastPos.Name,
				Pos:  e.Header.LogPos,
			}
			sql := strings.TrimSpace(string(ev.Query))
			parseResult, err := s.parseDDLSQL(sql, parser2, string(ev.Schema))
			if err != nil {
				log.Infof("[query]%s [last pos]%v [current pos]%v [current gtid set]%v", sql, lastPos, currentPos, ev.GSet)
				log.Errorf("fail to be parsed, error %v", err)
				return errors.Trace(err)
			}

			if parseResult.ignore {
				binlogSkippedEventsTotal.WithLabelValues("query", s.cfg.Name).Inc()
				log.Warnf("[skip query-sql]%s [schema]:%s", sql, ev.Schema)
				lastPos = currentPos // before record skip pos, update lastPos
				if err = s.recordSkipSQLsPos(lastPos, nil); err != nil {
					return errors.Trace(err)
				}
				continue
			}
			if !parseResult.isDDL {
				// skipped sql maybe not a DDL (like `BEGIN`)
				continue
			}

			if shardingReSync != nil {
				shardingReSync.currPos.Pos = e.Header.LogPos
				if shardingReSync.currPos.Compare(shardingReSync.latestPos) >= 0 {
					log.Infof("[syncer] sharding group %v re-syncing completed", shardingReSync)
					closeShardingSyncer()
				} else {
					// in re-syncing, we can simply skip all DDLs
					// only update lastPos when the query is a real DDL
					lastPos = shardingReSync.currPos
					log.Debugf("[syncer] skip query event when re-syncing sharding group %+v", shardingReSync)
				}
				continue
			}

			log.Infof("[query]%s [last pos]%v [current pos]%v [current gtid set]%v", sql, lastPos, currentPos, ev.GSet)
			lastPos = currentPos // update lastPos, because we have checked `isDDL`
			latestOp = ddl

			var (
				sqls                []string
				onlineDDLTableNames map[string]*filter.Table
			)

			// for DDL, we don't apply operator until we try to execute it.
			// so can handle sharding cases
			sqls, onlineDDLTableNames, _, err = s.resolveDDLSQL(sql, parser2, string(ev.Schema))
			if err != nil {
				log.Infof("[query]%s [last pos]%v [current pos]%v [current gtid set]%v", sql, lastPos, currentPos, ev.GSet)
				log.Errorf("fail to be parsed, error %v", err)
				return errors.Trace(err)
			}

			if len(onlineDDLTableNames) > 1 {
				return errors.NotSupportedf("online ddl changes on multiple table: %s", string(ev.Query))
			}

			binlogEvent.WithLabelValues("query", s.cfg.Name).Observe(time.Since(startTime).Seconds())

			/*
				we construct a application transaction for ddl. we save checkpoint after we execute all ddls
				Here's a brief discussion for implement:
				* non sharding table: make no difference
				* sharding table - we limit one ddl event only contains operation for same table
				  * drop database / drop table / truncate table: we ignore these operations
				  * create database / create table / create index / drop index / alter table:
					operation is only for same table,  make no difference
				  * rename table
					* online ddl: we would ignore rename ghost table,  make no difference
					* other rename: we don't allow user to execute more than one rename operation in one ddl event, then it would make no difference
			*/
			var (
				ddlInfo        *shardingDDLInfo
				needHandleDDLs []string
				targetTbls     = make(map[string]*filter.Table)
			)
			for _, sql := range sqls {
				sqlDDL, tableNames, stmt, err := s.handleDDL(parser2, string(ev.Schema), sql)
				if err != nil {
					return errors.Trace(err)
				}
				if len(sqlDDL) == 0 {
					binlogSkippedEventsTotal.WithLabelValues("query", s.cfg.Name).Inc()
					log.Warnf("[query-sql]%s [schema]:%s", sql, string(ev.Schema))
					continue
				}

				// for DDL, we wait it to be executed, so we can check if event is newer in this syncer's main process goroutine
				// ignore obsolete DDL here can avoid to try-sync again for already synced DDLs
				if !s.checkpoint.IsNewerTablePoint(tableNames[0][0].Schema, tableNames[0][0].Name, currentPos) {
					log.Infof("[syncer] ignore obsolete DDL %s in pos %v", sql, currentPos)
					continue
				}

				if s.cfg.IsSharding {
					switch stmt.(type) {
					case *ast.DropDatabaseStmt:
						err := s.dropSchemaInSharding(tableNames[0][0].Schema)
						if err != nil {
							return errors.Trace(err)
						}
						continue
					case *ast.DropTableStmt:
						sourceID, _ := GenTableID(tableNames[0][0].Schema, tableNames[0][0].Name)
						err = s.sgk.LeaveGroup(tableNames[1][0].Schema, tableNames[1][0].Name, []string{sourceID})
						if err != nil {
							return errors.Trace(err)
						}
						err = s.checkpoint.DeleteTablePoint(tableNames[0][0].Schema, tableNames[0][0].Name)
						if err != nil {
							return errors.Trace(err)
						}
						continue
					case *ast.TruncateTableStmt:
						log.Infof("[syncer] ignore truncate table statement %s in sharding group", sqlDDL)
						continue
					}

					// in sharding mode, we only support to do one ddl in one event
					if ddlInfo == nil {
						ddlInfo = &shardingDDLInfo{
							name:       tableNames[0][0].String(),
							tableNames: tableNames,
							stmt:       stmt,
						}
					} else {
						if ddlInfo.name != tableNames[0][0].String() {
							return errors.NotSupportedf("ddl on multiple table: %s", string(ev.Query))
						}
					}
				}

				needHandleDDLs = append(needHandleDDLs, sqlDDL)
				targetTbls[tableNames[1][0].String()] = tableNames[1][0]
			}

			log.Infof("need handled ddls %v in position %v", needHandleDDLs, currentPos)
			if len(needHandleDDLs) == 0 {
				log.Infof("skip query %s in position %v", string(ev.Query), currentPos)
				if err = s.recordSkipSQLsPos(lastPos, nil); err != nil {
					return errors.Trace(err)
				}
				continue
			}

			if !s.cfg.IsSharding {
				log.Infof("[start] execute need handled ddls %v in position %v", needHandleDDLs, currentPos)
				// try apply SQL operator before addJob. now, one query event only has one DDL job, if updating to multi DDL jobs, refine this.
				applied, appliedSQLs, err := s.tryApplySQLOperator(currentPos, needHandleDDLs)
				if err != nil {
					return errors.Annotatef(err, "try apply SQL operator on binlog-pos %s with DDLs %v", currentPos, needHandleDDLs)
				}
				if applied {
					needHandleDDLs = appliedSQLs // maybe nil
					log.Infof("[convert] execute need handled ddls converted to %v in position %s by sql operator", needHandleDDLs, currentPos)
				}
				job := newDDLJob(nil, needHandleDDLs, lastPos, currentPos, nil, nil)
				err = s.addJob(job)
				if err != nil {
					return errors.Trace(err)
				}
				log.Infof("[end] execute need handled ddls %v in position %v", needHandleDDLs, currentPos)

				for _, tbl := range targetTbls {
					s.clearTables(tbl.Schema, tbl.Name)
					// save checkpoint of each table
					s.checkpoint.SaveTablePoint(tbl.Schema, tbl.Name, currentPos)
				}

				for _, table := range onlineDDLTableNames {
					log.Infof("finish online ddl %v for table %s.%s", needHandleDDLs, table.Schema, table.Name)
					err = s.onlineDDL.Finish(table.Schema, table.Name)
					if err != nil {
						return errors.Annotatef(err, "finish online ddl on %s.%s", table.Schema, table.Name)
					}
				}

				continue
			}

			// handle sharding ddl
			var (
				needShardingHandle bool
				group              *ShardingGroup
				synced             bool
				remain             int
				source             string
				ddlExecItem        *DDLExecItem
			)
			// for sharding DDL, the firstPos should be the `Pos` of the binlog, not the `End_log_pos`
			// so when restarting before sharding DDLs synced, this binlog can be re-sync again to trigger the TrySync
			startPos := mysql.Position{
				Name: currentPos.Name,
				Pos:  currentPos.Pos - e.Header.EventSize,
			}
			source, _ = GenTableID(ddlInfo.tableNames[0][0].Schema, ddlInfo.tableNames[0][0].Name)

			switch ddlInfo.stmt.(type) {
			case *ast.CreateDatabaseStmt:
				// for CREATE DATABASE, we do nothing. when CREATE TABLE under this DATABASE, sharding groups will be added
			case *ast.CreateTableStmt:
				// for CREATE TABLE, we add it to group
				needShardingHandle, group, synced, remain, err = s.sgk.AddGroup(ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name, []string{source}, true)
				if err != nil {
					return errors.Trace(err)
				}
				log.Infof("[syncer] add table %s to shard group (%v)", source, needShardingHandle)
			default:
				needShardingHandle, group, synced, remain, err = s.sgk.TrySync(ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name, source, startPos, currentPos, needHandleDDLs)
				if err != nil {
					return errors.Trace(err)
				}
				log.Infof("[syncer] try to sync table %s to shard group (%v)", source, needShardingHandle)
			}

			if needShardingHandle {
				log.Infof("[syncer] query event %v for source %v is in sharding, synced: %v, remain: %d", startPos, source, synced, remain)
				err = safeMode.IncrForTable(ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name) // try enable safe-mode when starting syncing for sharding group
				if err != nil {
					return errors.Trace(err)
				}

				// save checkpoint in memory, don't worry, if error occurred, we can rollback it
				// for non-last sharding DDL's table, this checkpoint will be used to skip binlog event when re-syncing
				// NOTE: when last sharding DDL executed, all this checkpoints will be flushed in the same txn
				s.checkpoint.SaveTablePoint(ddlInfo.tableNames[0][0].Schema, ddlInfo.tableNames[0][0].Name, currentPos)
				if !synced {
					log.Infof("[syncer] source %s is in sharding DDL syncing, ignore DDL %v", source, startPos)
					continue
				}

				log.Infof("[syncer] source %s sharding group synced in pos %v", source, startPos)
				err = safeMode.DescForTable(ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name) // try disable safe-mode after sharding group synced
				if err != nil {
					return errors.Trace(err)
				}
				// maybe multi-groups' sharding DDL synced in this for-loop (one query-event, multi tables)
				if cap(shardingReSyncCh) < len(sqls) {
					shardingReSyncCh = make(chan *ShardingReSync, len(sqls))
				}
				firstEndPos := group.FirstEndPosUnresolved()
				if firstEndPos == nil {
					return errors.Errorf("no valid End_log_pos of the first DDL exists for sharding group with source %s", source)
				}
				shardingReSyncCh <- &ShardingReSync{
					currPos:      *firstEndPos,
					latestPos:    currentPos,
					targetSchema: ddlInfo.tableNames[1][0].Schema,
					targetTable:  ddlInfo.tableNames[1][0].Name,
				}

				// Don't send new DDLInfo to dm-master until all local sql jobs finished
				s.jobWg.Wait()

				// NOTE: if we need singleton Syncer (without dm-master) to support sharding DDL sync
				// we should add another config item to differ, and do not save DDLInfo, and not wait for ddlExecInfo

				ddlInfo1 := &pb.DDLInfo{
					Task:   s.cfg.Name,
					Schema: ddlInfo.tableNames[1][0].Schema, // use target schema / table name
					Table:  ddlInfo.tableNames[1][0].Name,
					DDLs:   needHandleDDLs,
				}
				s.ddlInfoCh <- ddlInfo1 // save DDLInfo, and dm-worker will fetch it

				// block and wait DDL lock to be synced
				var ok bool
				ddlExecItem, ok = <-s.ddlExecInfo.Chan(needHandleDDLs)
				if !ok {
					// chan closed
					log.Info("[syncer] cancel to add DDL to job because of canceled from external")
					return nil
				}
				if ddlExecItem.req.Exec {
					log.Infof("[syncer] add DDL %v to job, request is %+v", ddlInfo1.DDLs, ddlExecItem.req)
				} else {
					log.Infof("[syncer] ignore DDL %v, request is %+v", ddlInfo1.DDLs, ddlExecItem.req)
				}
			}

			log.Infof("[ddl][schema]%s [start] sql %s, need handled sqls %v", string(ev.Schema), string(ev.Query), needHandleDDLs)
			// try apply SQL operator before addJob. now, one query event only has one DDL job, if updating to multi DDL jobs, refine this.
			applied, appliedSQLs, err := s.tryApplySQLOperator(currentPos, needHandleDDLs)
			if err != nil {
				return errors.Annotatef(err, "try apply SQL operator on binlog-pos %s with DDLs %v", currentPos, needHandleDDLs)
			}
			if applied {
				needHandleDDLs = appliedSQLs // maybe nil
				log.Infof("[convert] execute need handled ddls converted to %v in position %s by sql operator", needHandleDDLs, currentPos)
			}
			job := newDDLJob(ddlInfo, needHandleDDLs, lastPos, currentPos, nil, ddlExecItem)
			err = s.addJob(job)
			if err != nil {
				return errors.Trace(err)
			}

			if len(onlineDDLTableNames) > 0 {
				s.clearOnlineDDL(ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name)
			}

			log.Infof("[ddl][end]%v", needHandleDDLs)

			s.clearTables(ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name)
		case *replication.XIDEvent:
			if shardingReSync != nil {
				shardingReSync.currPos.Pos = e.Header.LogPos
				lastPos = shardingReSync.currPos
				if shardingReSync.currPos.Compare(shardingReSync.latestPos) >= 0 {
					log.Infof("[syncer] sharding group %v re-syncing completed", shardingReSync)
					closeShardingSyncer()
					continue
				}
			}

			latestOp = xid
			currentPos.Pos = e.Header.LogPos
			log.Debugf("[XID event][last_pos]%v [current_pos]%v [gtid set]%v", lastPos, currentPos, ev.GSet)
			lastPos.Pos = e.Header.LogPos // update lastPos

			job := newXIDJob(currentPos, currentPos, nil)
			err = s.addJob(job)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (s *Syncer) commitJob(tp opType, sourceSchema, sourceTable, targetSchema, targetTable, sql string, args []interface{}, keys []string, retry bool, pos, cmdPos mysql.Position, gs gtid.Set) error {
	key, err := s.resolveCasuality(keys)
	if err != nil {
		return errors.Errorf("resolve karam error %v", err)
	}
	job := newJob(tp, sourceSchema, sourceTable, targetSchema, targetTable, sql, args, key, pos, cmdPos, gs)
	err = s.addJob(job)
	return errors.Trace(err)
}

func (s *Syncer) resolveCasuality(keys []string) (string, error) {
	if s.cfg.DisableCausality {
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

func (s *Syncer) genRouter() error {
	for _, rule := range s.cfg.RouteRules {
		err := s.tableRouter.AddRule(rule)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (s *Syncer) printStatus(ctx context.Context) {
	defer s.wg.Done()

	timer := time.NewTicker(statusTime)
	defer timer.Stop()

	var (
		err                 error
		latestMasterPos     mysql.Position
		latestmasterGTIDSet gtid.Set
	)

	for {
		select {
		case <-ctx.Done():
			log.Infof("print status exits, err:%s", ctx.Err())
			return
		case <-timer.C:
			now := time.Now()
			seconds := now.Unix() - s.lastTime.Unix()
			totalSeconds := now.Unix() - s.start.Unix()
			last := s.lastCount.Get()
			total := s.count.Get()

			totalBinlogSize := s.binlogSizeCount.Get()
			lastBinlogSize := s.lastBinlogSizeCount.Get()

			tps, totalTps := int64(0), int64(0)
			if seconds > 0 {
				tps = (total - last) / seconds
				totalTps = total / totalSeconds

				s.currentPosMu.RLock()
				currentPos := s.currentPosMu.currentPos
				s.currentPosMu.RUnlock()

				remainingSize, err2 := countBinaryLogsSize(currentPos, s.fromDB.db)
				if err2 != nil {
					// log the error, but still handle the rest operation
					log.Errorf("[syncer] count remaining binlog size err %v", errors.ErrorStack(err2))
				} else {
					bytesPerSec := (totalBinlogSize - lastBinlogSize) / seconds
					if bytesPerSec > 0 {
						remainingSeconds := remainingSize / bytesPerSec
						log.Infof("totalBinlogSize %d, lastBinlogSize %d, seconds %d,  bytesPerSec %d, remainingSize %d, remaining seconds %d", totalBinlogSize, lastBinlogSize, seconds, bytesPerSec, remainingSize, remainingSeconds)
						remainingTimeGauge.WithLabelValues(s.cfg.Name).Set(float64(remainingSeconds))
					}
				}
			}

			latestMasterPos, latestmasterGTIDSet, err = s.getMasterStatus()
			if err != nil {
				log.Errorf("[syncer] get master status error %s", err)
			} else {
				binlogPosGauge.WithLabelValues("master", s.cfg.Name).Set(float64(latestMasterPos.Pos))
				index, err := streamer.GetBinlogFileIndex(latestMasterPos.Name)
				if err != nil {
					log.Errorf("[syncer] parse binlog file err %v", err)
				} else {
					binlogFileGauge.WithLabelValues("master", s.cfg.Name).Set(index)
				}
			}

			log.Infof("[syncer]total events = %d, total tps = %d, recent tps = %d, master-binlog = %v, master-binlog-gtid=%v, syncer-binlog=%s",
				total, totalTps, tps, latestMasterPos, latestmasterGTIDSet, s.checkpoint)

			s.lastCount.Set(total)
			s.lastBinlogSizeCount.Set(totalBinlogSize)
			s.lastTime = time.Now()
			s.totalTps.Set(totalTps)
			s.tps.Set(tps)
		}
	}
}

// NOTE: refactor with remote and local streamer later
func (s *Syncer) getBinlogStreamer(syncerOrReader interface{}, pos mysql.Position) (streamer.Streamer, error) {
	if s.binlogType == RemoteBinlog {
		return s.getRemoteBinlogStreamer(syncerOrReader, pos)
	}
	return s.getLocalBinlogStreamer(syncerOrReader, pos)
}

func (s *Syncer) getLocalBinlogStreamer(syncerOrReader interface{}, pos mysql.Position) (streamer.Streamer, error) {
	reader, ok := syncerOrReader.(*streamer.BinlogReader)
	if !ok {
		return nil, errors.NotValidf("BinlogReader %v", syncerOrReader)
	}
	return reader.StartSync(pos)
}

func (s *Syncer) getRemoteBinlogStreamer(syncerOrReader interface{}, pos mysql.Position) (streamer.Streamer, error) {
	syncer, ok := syncerOrReader.(*replication.BinlogSyncer)
	if !ok {
		return nil, errors.NotValidf("replication.BinlogSyncer %v", syncerOrReader)
	}
	defer func() {
		lastSlaveConnectionID := syncer.LastConnectionID()
		log.Infof("[syncer] last slave connection id %d", lastSlaveConnectionID)
	}()
	if s.cfg.EnableGTID {
		// NOTE: our (per-table based) checkpoint does not support GTID yet
		return nil, errors.New("[syncer] now support GTID mode yet")
	}

	return s.startSyncByPosition(syncer, pos)
}

func (s *Syncer) createDBs() error {
	var err error
	s.fromDB, err = createDB(s.cfg, s.cfg.From, maxDMLConnectionTimeout)
	if err != nil {
		return errors.Trace(err)
	}

	s.toDBs = make([]*Conn, 0, s.cfg.WorkerCount)
	s.toDBs, err = createDBs(s.cfg, s.cfg.To, s.cfg.WorkerCount, maxDMLConnectionTimeout)
	if err != nil {
		return errors.Trace(err)
	}
	// db for ddl
	s.ddlDB, err = createDB(s.cfg, s.cfg.To, maxDDLConnectionTimeout)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// record skip ddl/dml sqls' position
// make newJob's sql argument empty to distinguish normal sql and skips sql
func (s *Syncer) recordSkipSQLsPos(pos mysql.Position, gtidSet gtid.Set) error {
	job := newSkipJob(pos, gtidSet)
	err := s.addJob(job)
	return errors.Trace(err)
}

func (s *Syncer) flushJobs() error {
	log.Infof("flush all jobs, global checkpoint=%s", s.checkpoint)
	job := newFlushJob()
	err := s.addJob(job)
	return errors.Trace(err)
}

func (s *Syncer) reSyncBinlog(cfg replication.BinlogSyncerConfig) (streamer.Streamer, error) {
	err := s.retrySyncGTIDs()
	if err != nil {
		return nil, errors.Trace(err)
	}
	// close still running sync
	return s.reopenWithRetry(cfg)
}

func (s *Syncer) reopenWithRetry(cfg replication.BinlogSyncerConfig) (streamer streamer.Streamer, err error) {
	for i := 0; i < maxRetryCount; i++ {
		streamer, err = s.reopen(cfg)
		if err == nil {
			return
		}
		if needRetryReplicate(err) {
			log.Infof("[syncer] retry open binlog streamer %v", err)
			time.Sleep(retryTimeout)
			continue
		}
		break
	}
	return nil, errors.Trace(err)
}

func (s *Syncer) reopen(cfg replication.BinlogSyncerConfig) (streamer.Streamer, error) {
	if s.syncer != nil {
		err := s.closeBinlogSyncer(s.syncer)
		s.syncer = nil
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	// TODO: refactor to support relay
	s.syncer = replication.NewBinlogSyncer(cfg)
	return s.getBinlogStreamer(s.syncer, s.checkpoint.GlobalPoint())
}

func (s *Syncer) startSyncByPosition(syncer *replication.BinlogSyncer, pos mysql.Position) (streamer.Streamer, error) {
	streamer, err := syncer.StartSync(pos)
	return streamer, errors.Trace(err)
}

func (s *Syncer) renameShardingSchema(schema, table string) (string, string) {
	if schema == "" {
		return schema, table
	}
	targetSchema, targetTable, err := s.tableRouter.Route(schema, table)
	if err != nil {
		log.Error(errors.ErrorStack(err)) // log the error, but still continue
	}
	if targetSchema == "" {
		return schema, table
	}
	if targetTable == "" {
		targetTable = table
	}

	return targetSchema, targetTable
}

func (s *Syncer) isClosed() bool {
	return s.closed.Get()
}

// Close closes syncer.
func (s *Syncer) Close() {
	s.Lock()
	defer s.Unlock()

	if s.isClosed() {
		return
	}

	if s.cfg.EnableHeartbeat {
		s.heartbeat.RemoveTask(s.cfg.Name)
	}

	s.stopSync()

	if s.ddlInfoCh != nil {
		close(s.ddlInfoCh)
		s.ddlInfoCh = nil
	}

	closeDBs(s.fromDB)
	closeDBs(s.toDBs...)
	closeDBs(s.ddlDB)

	s.checkpoint.Close()

	if s.onlineDDL != nil {
		s.onlineDDL.Close()
		s.onlineDDL = nil
	}

	// when closing syncer by `stop-task`, remove active relay log from hub
	s.removeActiveRelayLog()

	s.closed.Set(true)
}

// stopSync stops syncing, now it used by Close and Pause
// maybe we can refine the workflow more clear
func (s *Syncer) stopSync() {
	<-s.done // wait Run to return
	s.closeJobChans()
	s.wg.Wait() // wait job workers to return

	// before re-write workflow for s.syncer, simply close it
	// when resuming, re-create s.syncer
	if s.syncer != nil {
		s.closeBinlogSyncer(s.syncer)
		s.syncer = nil
	}
	if s.localReader != nil {
		s.localReader.Close()
	}
}

func (s *Syncer) closeBinlogSyncer(syncer *replication.BinlogSyncer) error {
	if syncer == nil {
		return nil
	}

	lastSlaveConnectionID := syncer.LastConnectionID()
	defer syncer.Close()
	if lastSlaveConnectionID > 0 {
		err := utils.KillConn(s.fromDB.db, lastSlaveConnectionID)
		if err != nil {
			log.Errorf("[syncer] kill last connection %d err %v", lastSlaveConnectionID, err)
			if !utils.IsNoSuchThreadError(err) {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// Pause pauses the process, and it can be resumed later
// should cancel context from external
// TODO: it is not a true-meaning Pause because you can't stop it by calling Pause only.
func (s *Syncer) Pause() {
	if s.isClosed() {
		log.Warn("[syncer] try to pause, but already closed")
		return
	}

	s.stopSync()
}

// Resume resumes the paused process
func (s *Syncer) Resume(ctx context.Context, pr chan pb.ProcessResult) {
	if s.isClosed() {
		log.Warn("[syncer] try to resume, but already closed")
		return
	}

	// continue the processing
	s.Process(ctx, pr)
}

// Update implements Unit.Update
// now, only support to update config for routes, filters, column-mappings, black-white-list
// now no config diff implemented, so simply re-init use new config
func (s *Syncer) Update(cfg *config.SubTaskConfig) error {
	if s.cfg.IsSharding {
		tables := s.sgk.UnresolvedTables()
		if len(tables) > 0 {
			return errors.NotSupportedf("try update config when some tables' (%v) sharding DDL not synced", tables)
		}
	}

	var (
		err              error
		oldBwList        *filter.Filter
		oldTableRouter   *router.Table
		oldBinlogFilter  *bf.BinlogEvent
		oldColumnMapping *cm.Mapping
	)

	defer func() {
		if err == nil {
			return
		}
		if oldBwList != nil {
			s.bwList = oldBwList
		}
		if oldTableRouter != nil {
			s.tableRouter = oldTableRouter
		}
		if oldBinlogFilter != nil {
			s.binlogFilter = oldBinlogFilter
		}
		if oldColumnMapping != nil {
			s.columnMapping = oldColumnMapping
		}
	}()

	// update black-white-list
	oldBwList = s.bwList
	s.bwList = filter.New(cfg.CaseSensitive, cfg.BWList)

	// update route
	oldTableRouter = s.tableRouter
	s.tableRouter, err = router.NewTableRouter(cfg.CaseSensitive, cfg.RouteRules)
	if err != nil {
		return errors.Trace(err)
	}

	// update binlog filter
	oldBinlogFilter = s.binlogFilter
	s.binlogFilter, err = bf.NewBinlogEvent(cfg.CaseSensitive, cfg.FilterRules)
	if err != nil {
		return errors.Trace(err)
	}

	// update column-mappings
	oldColumnMapping = s.columnMapping
	s.columnMapping, err = cm.NewMapping(cfg.CaseSensitive, cfg.ColumnMappingRules)
	if err != nil {
		return errors.Trace(err)
	}

	if s.cfg.IsSharding {
		// re-init sharding group
		s.initShardingGroups()
	}

	// update l.cfg
	s.cfg.BWList = cfg.BWList
	s.cfg.RouteRules = cfg.RouteRules
	s.cfg.FilterRules = cfg.FilterRules
	s.cfg.ColumnMappingRules = cfg.ColumnMappingRules
	s.cfg.Timezone = cfg.Timezone

	// update timezone
	s.setTimezone()

	return nil
}

func (s *Syncer) needResync() bool {
	masterPos, _, err := s.getMasterStatus()
	if err != nil {
		log.Errorf("get master status err:%s", err)
		return false
	}

	// Why 190 ?
	// +------------------+-----+----------------+-----------+-------------+-------------------------------------------------------------------+
	// | Log_name         | Pos | Event_type     | Server_id | End_log_pos | Info                                                              |
	// +------------------+-----+----------------+-----------+-------------+-------------------------------------------------------------------+
	// | mysql-bin.000002 |   4 | Format_desc    |         1 |         123 | Server ver: 5.7.18-log, Binlog ver: 4                             |
	// | mysql-bin.000002 | 123 | Previous_gtids |         1 |         194 | 00020393-1111-1111-1111-111111111111:1-7
	//
	// Currently, syncer doesn't handle Format_desc and Previous_gtids events. When binlog rotate to new file with only two events like above,
	// syncer won't save pos to 194. Actually it save pos 4 to meta file. So We got a experience value of 194 - 4 = 190.
	// If (mpos.Pos - spos.Pos) > 190, we could say that syncer is not up-to-date.
	return utils.CompareBinlogPos(masterPos, s.checkpoint.GlobalPoint(), 190) == 1
}

// assume that reset master before switching to new master, and only the new master would write
// it's a weak function to try best to fix gtid set while switching master/slave
func (s *Syncer) retrySyncGTIDs() error {
	// NOTE: our (per-table based) checkpoint does not support GTID yet, implement it if needed
	log.Warn("[syncer] our (per-table based) checkpoint does not support GTID yet")
	return nil
}

// checkpointID returns ID which used for checkpoint table
func (s *Syncer) checkpointID() string {
	if len(s.cfg.SourceID) > 0 {
		return s.cfg.SourceID
	}
	return strconv.Itoa(s.cfg.ServerID)
}

// DDLInfo returns a chan from which can receive DDLInfo
func (s *Syncer) DDLInfo() <-chan *pb.DDLInfo {
	s.RLock()
	defer s.RUnlock()
	return s.ddlInfoCh
}

// ExecuteDDL executes or skips a hanging-up DDL when in sharding
func (s *Syncer) ExecuteDDL(ctx context.Context, execReq *pb.ExecDDLRequest) (<-chan error, error) {
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

// UpdateFromConfig updates config for `From`
func (s *Syncer) UpdateFromConfig(cfg *config.SubTaskConfig) error {
	s.Lock()
	defer s.Unlock()
	s.fromDB.close()

	s.cfg.From = cfg.From

	var err error
	s.fromDB, err = createDB(s.cfg, s.cfg.From, maxDMLConnectionTimeout)
	if err != nil {
		log.Warnf("[syncer] %+v", err)
		return err
	}
	return nil
}

// appendExecErrors appends syncer execErrors with new value
func (s *Syncer) appendExecErrors(errCtx *ExecErrorContext) {
	s.execErrors.Lock()
	defer s.execErrors.Unlock()
	s.execErrors.errors = append(s.execErrors.errors, errCtx)
}

// resetExecErrors resets syncer execErrors
func (s *Syncer) resetExecErrors() {
	s.execErrors.Lock()
	defer s.execErrors.Unlock()
	s.execErrors.errors = make([]*ExecErrorContext, 0)
}

func (s *Syncer) setTimezone() {
	var loc *time.Location

	if s.cfg.Timezone != "" {
		loc, _ = time.LoadLocation(s.cfg.Timezone)
	}
	if loc == nil {
		loc = time.Now().Location()
		log.Warnf("[syncer] use system default time location")
	}
	log.Infof("[syncer] use timezone: %s", loc)
	s.timezone = loc
}
