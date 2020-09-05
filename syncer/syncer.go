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
	"crypto/tls"
	"fmt"
	"os"
	"path"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/model"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	cm "github.com/pingcap/tidb-tools/pkg/column-mapping"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/sync2"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"

	toolutils "github.com/pingcap/tidb-tools/pkg/utils"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/unit"
	"github.com/pingcap/dm/pkg/atomic2"
	"github.com/pingcap/dm/pkg/binlog"
	"github.com/pingcap/dm/pkg/binlog/common"
	"github.com/pingcap/dm/pkg/binlog/event"
	"github.com/pingcap/dm/pkg/conn"
	tcontext "github.com/pingcap/dm/pkg/context"
	fr "github.com/pingcap/dm/pkg/func-rollback"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/schema"
	"github.com/pingcap/dm/pkg/shardddl/pessimism"
	"github.com/pingcap/dm/pkg/streamer"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/tracing"
	"github.com/pingcap/dm/pkg/utils"
	operator "github.com/pingcap/dm/syncer/err-operator"
	sm "github.com/pingcap/dm/syncer/safe-mode"
	"github.com/pingcap/dm/syncer/shardddl"
)

var (
	maxRetryCount = 100

	retryTimeout = 3 * time.Second
	waitTime     = 10 * time.Millisecond
	statusTime   = 30 * time.Second

	// MaxDDLConnectionTimeoutMinute also used by SubTask.ExecuteDDL
	MaxDDLConnectionTimeoutMinute = 5

	maxDMLConnectionTimeout = "5m"
	maxDDLConnectionTimeout = fmt.Sprintf("%dm", MaxDDLConnectionTimeoutMinute)

	maxDMLConnectionDuration, _ = time.ParseDuration(maxDMLConnectionTimeout)

	adminQueueName     = "admin queue"
	defaultBucketCount = 8
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

	tctx *tcontext.Context

	cfg     *config.SubTaskConfig
	syncCfg replication.BinlogSyncerConfig

	sgk       *ShardingGroupKeeper // keeper to keep all sharding (sub) group in this syncer
	pessimist *shardddl.Pessimist  // shard DDL pessimist
	optimist  *shardddl.Optimist   // shard DDL optimist

	binlogType         BinlogType
	streamerController *StreamerController
	enableRelay        bool

	wg    sync.WaitGroup
	jobWg sync.WaitGroup

	schemaTracker *schema.Tracker

	fromDB *UpStreamConn

	toDB      *conn.BaseDB
	toDBConns []*DBConn
	ddlDB     *conn.BaseDB
	ddlDBConn *DBConn

	jobs               []chan *job
	jobsClosed         sync2.AtomicBool
	jobsChanLock       sync.Mutex
	queueBucketMapping []string

	c *causality

	tableRouter   *router.Table
	binlogFilter  *bf.BinlogEvent
	columnMapping *cm.Mapping
	baList        *filter.Filter

	closed sync2.AtomicBool

	start    time.Time
	lastTime struct {
		sync.RWMutex
		t time.Time
	}

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
	execError atomic2.AtomicError

	execErrors struct {
		sync.Mutex
		errors []*ExecErrorContext
	}

	heartbeat *Heartbeat

	readerHub *streamer.ReaderHub

	errOperatorHolder *operator.Holder

	isReplacingErr bool // true if we are in replace events by handle-error

	// TODO: re-implement tracer flow for binlog event later.
	tracer *tracing.Tracer

	currentLocationMu struct {
		sync.RWMutex
		currentLocation binlog.Location // use to calc remain binlog size
	}

	errLocation struct {
		sync.RWMutex
		startLocation *binlog.Location
		endLocation   *binlog.Location
	}

	addJobFunc func(*job) error
}

// NewSyncer creates a new Syncer.
func NewSyncer(cfg *config.SubTaskConfig, etcdClient *clientv3.Client) *Syncer {
	logger := log.With(zap.String("task", cfg.Name), zap.String("unit", "binlog replication"))
	syncer := &Syncer{
		pessimist: shardddl.NewPessimist(&logger, etcdClient, cfg.Name, cfg.SourceID),
		optimist:  shardddl.NewOptimist(&logger, etcdClient, cfg.Name, cfg.SourceID),
	}
	syncer.cfg = cfg
	syncer.tctx = tcontext.Background().WithLogger(logger)
	syncer.jobsClosed.Set(true) // not open yet
	syncer.closed.Set(false)
	syncer.lastBinlogSizeCount.Set(0)
	syncer.binlogSizeCount.Set(0)
	syncer.lastCount.Set(0)
	syncer.count.Set(0)
	syncer.c = newCausality()
	syncer.done = nil
	syncer.tracer = tracing.GetTracer()
	syncer.setTimezone()
	syncer.addJobFunc = syncer.addJob
	syncer.enableRelay = cfg.UseRelay

	syncer.checkpoint = NewRemoteCheckPoint(syncer.tctx, cfg, syncer.checkpointID())

	syncer.setSyncCfg()

	syncer.binlogType = toBinlogType(cfg.UseRelay)
	syncer.errOperatorHolder = operator.NewHolder(&logger)
	syncer.readerHub = streamer.GetReaderHub()

	if cfg.ShardMode == config.ShardPessimistic {
		// only need to sync DDL in sharding mode
		syncer.sgk = NewShardingGroupKeeper(syncer.tctx, cfg)
	}

	var err error
	syncer.schemaTracker, err = schema.NewTracker(cfg.To.Session)
	if err != nil {
		syncer.tctx.L().DPanic("cannot create schema tracker", zap.Error(err))
	}

	return syncer
}

func (s *Syncer) newJobChans(count int) {
	s.closeJobChans()
	s.jobs = make([]chan *job, 0, count)
	for i := 0; i < count; i++ {
		s.jobs = append(s.jobs, make(chan *job, s.cfg.QueueSize))
	}
	s.jobsClosed.Set(false)
}

func (s *Syncer) closeJobChans() {
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

// Type implements Unit.Type
func (s *Syncer) Type() pb.UnitType {
	return pb.UnitType_Sync
}

// Init initializes syncer for a sync task, but not start Process.
// if fail, it should not call s.Close.
// some check may move to checker later.
func (s *Syncer) Init(ctx context.Context) (err error) {
	rollbackHolder := fr.NewRollbackHolder("syncer")
	defer func() {
		if err != nil {
			rollbackHolder.RollbackReverseOrder()
		}
	}()

	tctx := s.tctx.WithContext(ctx)

	err = s.createDBs()
	if err != nil {
		return err
	}
	rollbackHolder.Add(fr.FuncRollback{Name: "close-DBs", Fn: s.closeDBs})

	s.streamerController = NewStreamerController(tctx, s.syncCfg, s.cfg.EnableGTID, s.fromDB, s.binlogType, s.cfg.RelayDir, s.timezone)

	s.baList, err = filter.New(s.cfg.CaseSensitive, s.cfg.BAList)
	if err != nil {
		return terror.ErrSyncerUnitGenBAList.Delegate(err)
	}

	s.binlogFilter, err = bf.NewBinlogEvent(s.cfg.CaseSensitive, s.cfg.FilterRules)
	if err != nil {
		return terror.ErrSyncerUnitGenBinlogEventFilter.Delegate(err)
	}

	if len(s.cfg.ColumnMappingRules) > 0 {
		s.columnMapping, err = cm.NewMapping(s.cfg.CaseSensitive, s.cfg.ColumnMappingRules)
		if err != nil {
			return terror.ErrSyncerUnitGenColumnMapping.Delegate(err)
		}
	}

	if s.cfg.OnlineDDLScheme != "" {
		fn, ok := OnlineDDLSchemes[s.cfg.OnlineDDLScheme]
		if !ok {
			return terror.ErrSyncerUnitOnlineDDLSchemeNotSupport.Generate(s.cfg.OnlineDDLScheme)
		}
		s.onlineDDL, err = fn(tctx, s.cfg)
		if err != nil {
			return err
		}
		rollbackHolder.Add(fr.FuncRollback{Name: "close-onlineDDL", Fn: s.closeOnlineDDL})
	}

	err = s.genRouter()
	if err != nil {
		return err
	}

	switch s.cfg.ShardMode {
	case config.ShardPessimistic:
		err = s.sgk.Init()
		if err != nil {
			return err
		}

		err = s.initShardingGroups()
		if err != nil {
			return err
		}
		rollbackHolder.Add(fr.FuncRollback{Name: "close-sharding-group-keeper", Fn: s.sgk.Close})
	case config.ShardOptimistic:
		err = s.initOptimisticShardDDL()
		if err != nil {
			return err
		}
	}

	err = s.checkpoint.Init(tctx)
	if err != nil {
		return err
	}
	rollbackHolder.Add(fr.FuncRollback{Name: "close-checkpoint", Fn: s.checkpoint.Close})

	err = s.checkpoint.Load(tctx, s.schemaTracker)
	if err != nil {
		return err
	}
	if s.cfg.EnableHeartbeat {
		s.heartbeat, err = GetHeartbeat(&HeartbeatConfig{
			serverID:       s.cfg.ServerID,
			primaryCfg:     s.cfg.From,
			updateInterval: int64(s.cfg.HeartbeatUpdateInterval),
			reportInterval: int64(s.cfg.HeartbeatReportInterval),
		})
		if err != nil {
			return err
		}
		err = s.heartbeat.AddTask(s.cfg.Name)
		if err != nil {
			return err
		}
		rollbackHolder.Add(fr.FuncRollback{Name: "remove-heartbeat", Fn: s.removeHeartbeat})
	}

	// when Init syncer, set active relay log info
	err = s.setInitActiveRelayLog()
	if err != nil {
		return err
	}
	rollbackHolder.Add(fr.FuncRollback{Name: "remove-active-realylog", Fn: s.removeActiveRelayLog})

	s.reset()
	return nil
}

// initShardingGroups initializes sharding groups according to source MySQL, filter rules and router rules
// NOTE: now we don't support modify router rules after task has started
func (s *Syncer) initShardingGroups() error {
	// fetch tables from source and filter them
	sourceTables, err := s.fromDB.fetchAllDoTables(s.baList)
	if err != nil {
		return err
	}

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

	loadMeta, err2 := s.sgk.LoadShardMeta(s.cfg.Flavor, s.cfg.EnableGTID)
	if err2 != nil {
		return err2
	}

	// add sharding group
	for targetSchema, mSchema := range mapper {
		for targetTable, sourceIDs := range mSchema {
			tableID, _ := GenTableID(targetSchema, targetTable)
			_, _, _, _, err := s.sgk.AddGroup(targetSchema, targetTable, sourceIDs, loadMeta[tableID], false)
			if err != nil {
				return err
			}
		}
	}

	shardGroup := s.sgk.Groups()
	s.tctx.L().Debug("initial sharding groups", zap.Int("shard group length", len(shardGroup)), zap.Reflect("shard group", shardGroup))

	return nil
}

// IsFreshTask implements Unit.IsFreshTask
func (s *Syncer) IsFreshTask(ctx context.Context) (bool, error) {
	globalPoint := s.checkpoint.GlobalPoint()
	tablePoint := s.checkpoint.TablePoint()
	return binlog.CompareLocation(globalPoint, binlog.NewLocation(s.cfg.Flavor), s.cfg.EnableGTID) <= 0 && len(tablePoint) == 0, nil
}

func (s *Syncer) reset() {
	if s.streamerController != nil {
		s.streamerController.Close(s.tctx)
	}
	// create new job chans
	s.newJobChans(s.cfg.WorkerCount + 1)

	s.execError.Set(nil)
	s.resetExecErrors()
	s.setErrLocation(nil, nil)
	s.isReplacingErr = false

	switch s.cfg.ShardMode {
	case config.ShardPessimistic:
		// every time start to re-sync from resume, we reset status to make it like a fresh syncing
		s.sgk.ResetGroups()
		s.pessimist.Reset()
	case config.ShardOptimistic:
		s.optimist.Reset()
	}
}

func (s *Syncer) resetDBs(tctx *tcontext.Context) error {
	var err error

	for i := 0; i < len(s.toDBConns); i++ {
		err = s.toDBConns[i].resetConn(tctx)
		if err != nil {
			return terror.WithScope(err, terror.ScopeDownstream)
		}
	}

	if s.onlineDDL != nil {
		err = s.onlineDDL.ResetConn(tctx)
		if err != nil {
			return terror.WithScope(err, terror.ScopeDownstream)
		}
	}

	if s.sgk != nil {
		err = s.sgk.dbConn.resetConn(tctx)
		if err != nil {
			return terror.WithScope(err, terror.ScopeDownstream)
		}
	}

	err = s.ddlDBConn.resetConn(tctx)
	if err != nil {
		return terror.WithScope(err, terror.ScopeDownstream)
	}

	err = s.checkpoint.ResetConn(tctx)
	if err != nil {
		return terror.WithScope(err, terror.ScopeDownstream)
	}

	return nil
}

// Process implements the dm.Unit interface.
func (s *Syncer) Process(ctx context.Context, pr chan pb.ProcessResult) {
	syncerExitWithErrorCounter.WithLabelValues(s.cfg.Name, s.cfg.SourceID).Add(0)

	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// create new done chan
	s.done = make(chan struct{})

	runFatalChan := make(chan *pb.ProcessError, s.cfg.WorkerCount+1)
	s.runFatalChan = runFatalChan
	var (
		errs   = make([]*pb.ProcessError, 0, 2)
		errsMu sync.Mutex
	)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			err, ok := <-runFatalChan
			if !ok {
				return
			}
			cancel() // cancel s.Run
			syncerExitWithErrorCounter.WithLabelValues(s.cfg.Name, s.cfg.SourceID).Inc()
			errsMu.Lock()
			errs = append(errs, err)
			errsMu.Unlock()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-newCtx.Done() // ctx or newCtx
	}()

	err := s.Run(newCtx)
	if err != nil {
		// returned error rather than sent to runFatalChan
		// cancel goroutines created in s.Run
		cancel()
	}
	s.closeJobChans()   // Run returned, all jobs sent, we can close s.jobs
	s.wg.Wait()         // wait for sync goroutine to return
	close(runFatalChan) // Run returned, all potential fatal sent to s.runFatalChan
	wg.Wait()           // wait for receive all fatal from s.runFatalChan

	if err != nil {
		if utils.IsContextCanceledError(err) {
			s.tctx.L().Info("filter out error caused by user cancel")
		} else {
			syncerExitWithErrorCounter.WithLabelValues(s.cfg.Name, s.cfg.SourceID).Inc()
			errsMu.Lock()
			errs = append(errs, unit.NewProcessError(err))
			errsMu.Unlock()
		}
	}

	isCanceled := false
	select {
	case <-ctx.Done():
		isCanceled = true
	default:
	}

	if len(errs) != 0 {
		// pause because of error occurred
		s.Pause()
	}

	// try to rollback checkpoints, if they already flushed, no effect
	prePos := s.checkpoint.GlobalPoint()
	s.checkpoint.Rollback(s.schemaTracker)
	currPos := s.checkpoint.GlobalPoint()
	if binlog.CompareLocation(prePos, currPos, s.cfg.EnableGTID) != 0 {
		s.tctx.L().Warn("something wrong with rollback global checkpoint", zap.Stringer("previous position", prePos), zap.Stringer("current position", currPos))
	}

	pr <- pb.ProcessResult{
		IsCanceled: isCanceled,
		Errors:     errs,
	}
}

func (s *Syncer) getMasterStatus() (mysql.Position, gtid.Set, error) {
	return s.fromDB.getMasterStatus(s.cfg.Flavor)
}

func (s *Syncer) getTable(origSchema, origTable, renamedSchema, renamedTable string) (*model.TableInfo, error) {
	ti, err := s.schemaTracker.GetTable(origSchema, origTable)
	if err == nil {
		return ti, nil
	}
	if !schema.IsTableNotExists(err) {
		return nil, terror.ErrSchemaTrackerCannotGetTable.Delegate(err, origSchema, origTable)
	}

	if err = s.schemaTracker.CreateSchemaIfNotExists(origSchema); err != nil {
		return nil, terror.ErrSchemaTrackerCannotCreateSchema.Delegate(err, origSchema)
	}

	// in optimistic shard mode, we should try to get the init schema (the one before modified by other tables) first.
	if s.cfg.ShardMode == config.ShardOptimistic {
		ti, err = s.trackInitTableInfoOptimistic(origSchema, origTable, renamedSchema, renamedTable)
		if err != nil {
			return nil, err
		}
	}

	// if the table does not exist (IsTableNotExists(err)), continue to fetch the table from downstream and create it.
	if ti == nil {
		err = s.trackTableInfoFromDownstream(origSchema, origTable, renamedSchema, renamedTable)
		if err != nil {
			return nil, err
		}
	}

	ti, err = s.schemaTracker.GetTable(origSchema, origTable)
	if err != nil {
		return nil, terror.ErrSchemaTrackerCannotGetTable.Delegate(err, origSchema, origTable)
	}
	return ti, nil
}

// trackTableInfoFromDownstream tries to track the table info from the downstream.
func (s *Syncer) trackTableInfoFromDownstream(origSchema, origTable, renamedSchema, renamedTable string) error {
	// TODO: Switch to use the HTTP interface to retrieve the TableInfo directly
	// (and get rid of ddlDBConn).
	// use parser for downstream.
	parser2, err := utils.GetParserForConn(s.ddlDBConn.baseConn.DBConn)
	if err != nil {
		return terror.ErrSchemaTrackerCannotFetchDownstreamTable.Delegate(err, renamedSchema, renamedTable, origSchema, origTable)
	}

	rows, err := s.ddlDBConn.querySQL(s.tctx, "SHOW CREATE TABLE "+dbutil.TableName(renamedSchema, renamedTable))
	if err != nil {
		return terror.ErrSchemaTrackerCannotFetchDownstreamTable.Delegate(err, renamedSchema, renamedTable, origSchema, origTable)
	}
	defer rows.Close()

	ctx := context.Background()
	for rows.Next() {
		var tableName, createSQL string
		if err = rows.Scan(&tableName, &createSQL); err != nil {
			return terror.WithScope(terror.DBErrorAdapt(err, terror.ErrDBDriverError), terror.ScopeDownstream)
		}

		// rename the table back to original.
		var createNode ast.StmtNode
		createNode, err = parser2.ParseOneStmt(createSQL, "", "")
		if err != nil {
			return terror.ErrSchemaTrackerCannotParseDownstreamTable.Delegate(err, renamedSchema, renamedTable, origSchema, origTable)
		}
		createStmt := createNode.(*ast.CreateTableStmt)
		createStmt.IfNotExists = true
		createStmt.Table.Schema = model.NewCIStr(origSchema)
		createStmt.Table.Name = model.NewCIStr(origTable)

		var newCreateSQLBuilder strings.Builder
		restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &newCreateSQLBuilder)
		if err = createStmt.Restore(restoreCtx); err != nil {
			return terror.ErrSchemaTrackerCannotParseDownstreamTable.Delegate(err, renamedSchema, renamedTable, origSchema, origTable)
		}
		newCreateSQL := newCreateSQLBuilder.String()
		s.tctx.L().Debug("reverse-synchronized table schema",
			zap.String("origSchema", origSchema),
			zap.String("origTable", origTable),
			zap.String("renamedSchema", renamedSchema),
			zap.String("renamedTable", renamedTable),
			zap.String("sql", newCreateSQL),
		)
		if err = s.schemaTracker.Exec(ctx, origSchema, newCreateSQL); err != nil {
			return terror.ErrSchemaTrackerCannotCreateTable.Delegate(err, origSchema, origTable)
		}
	}

	if err = rows.Err(); err != nil {
		return terror.WithScope(terror.DBErrorAdapt(err, terror.ErrDBDriverError), terror.ScopeDownstream)
	}
	return nil
}

func (s *Syncer) addCount(isFinished bool, queueBucket string, tp opType, n int64) {
	m := addedJobsTotal
	if isFinished {
		s.count.Add(n)
		m = finishedJobsTotal
	}

	switch tp {
	case insert:
		m.WithLabelValues("insert", s.cfg.Name, queueBucket, s.cfg.SourceID).Add(float64(n))
	case update:
		m.WithLabelValues("update", s.cfg.Name, queueBucket, s.cfg.SourceID).Add(float64(n))
	case del:
		m.WithLabelValues("del", s.cfg.Name, queueBucket, s.cfg.SourceID).Add(float64(n))
	case ddl:
		m.WithLabelValues("ddl", s.cfg.Name, queueBucket, s.cfg.SourceID).Add(float64(n))
	case xid:
		// ignore xid jobs
	case flush:
		m.WithLabelValues("flush", s.cfg.Name, queueBucket, s.cfg.SourceID).Add(float64(n))
	case skip:
		// ignore skip jobs
	default:
		s.tctx.L().Warn("unknown job operation type", zap.Stringer("type", tp))
	}
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

func (s *Syncer) saveTablePoint(db, table string, location binlog.Location) {
	ti, err := s.schemaTracker.GetTable(db, table)
	if err != nil {
		s.tctx.L().DPanic("table info missing from schema tracker",
			zap.String("schema", db),
			zap.String("table", table),
			zap.Stringer("location", location),
			zap.Error(err))
	}
	s.checkpoint.SaveTablePoint(db, table, location.Clone(), ti)
}

func (s *Syncer) addJob(job *job) error {
	var (
		queueBucket int
	)
	switch job.tp {
	case xid:
		s.saveGlobalPoint(job.location)
		return nil
	case flush:
		addedJobsTotal.WithLabelValues("flush", s.cfg.Name, adminQueueName, s.cfg.SourceID).Inc()
		// ugly code addJob and sync, refine it later
		s.jobWg.Add(s.cfg.WorkerCount)
		for i := 0; i < s.cfg.WorkerCount; i++ {
			startTime := time.Now()
			s.jobs[i] <- job
			// flush for every DML queue
			addJobDurationHistogram.WithLabelValues("flush", s.cfg.Name, s.queueBucketMapping[i], s.cfg.SourceID).Observe(time.Since(startTime).Seconds())
		}
		s.jobWg.Wait()
		finishedJobsTotal.WithLabelValues("flush", s.cfg.Name, adminQueueName, s.cfg.SourceID).Inc()
		return s.flushCheckPoints()
	case ddl:
		s.jobWg.Wait()
		addedJobsTotal.WithLabelValues("ddl", s.cfg.Name, adminQueueName, s.cfg.SourceID).Inc()
		s.jobWg.Add(1)
		queueBucket = s.cfg.WorkerCount
		startTime := time.Now()
		s.jobs[queueBucket] <- job
		addJobDurationHistogram.WithLabelValues("ddl", s.cfg.Name, adminQueueName, s.cfg.SourceID).Observe(time.Since(startTime).Seconds())
	case insert, update, del:
		s.jobWg.Add(1)
		queueBucket = int(utils.GenHashKey(job.key)) % s.cfg.WorkerCount
		s.addCount(false, s.queueBucketMapping[queueBucket], job.tp, 1)
		startTime := time.Now()
		s.tctx.L().Debug("queue for key", zap.Int("queue", queueBucket), zap.String("key", job.key))
		s.jobs[queueBucket] <- job
		addJobDurationHistogram.WithLabelValues(job.tp.String(), s.cfg.Name, s.queueBucketMapping[queueBucket], s.cfg.SourceID).Observe(time.Since(startTime).Seconds())
	}

	wait := s.checkWait(job)
	if wait {
		s.jobWg.Wait()
		s.c.reset()
	}

	switch job.tp {
	case ddl:
		failpoint.Inject("ExitAfterDDLBeforeFlush", func() {
			s.tctx.L().Warn("exit triggered", zap.String("failpoint", "ExitAfterDDLBeforeFlush"))
			utils.OsExit(1)
		})
		// interrupted after executed DDL and before save checkpoint.
		failpoint.Inject("FlushCheckpointStage", func(val failpoint.Value) {
			err := handleFlushCheckpointStage(3, val.(int), "before save checkpoint")
			if err != nil {
				failpoint.Return(err)
			}
		})
		// only save checkpoint for DDL and XID (see above)
		s.saveGlobalPoint(job.location)
		for sourceSchema, tbs := range job.sourceTbl {
			if len(sourceSchema) == 0 {
				continue
			}
			for _, sourceTable := range tbs {
				s.saveTablePoint(sourceSchema, sourceTable, job.location)
			}
		}
		// reset sharding group after checkpoint saved
		s.resetShardingGroup(job.targetSchema, job.targetTable)
	case insert, update, del:
		// save job's current pos for DML events
		for sourceSchema, tbs := range job.sourceTbl {
			if len(sourceSchema) == 0 {
				continue
			}
			for _, sourceTable := range tbs {
				s.saveTablePoint(sourceSchema, sourceTable, job.currentLocation)
			}
		}
	}

	if wait {
		// interrupted after save checkpoint and before flush checkpoint.
		failpoint.Inject("FlushCheckpointStage", func(val failpoint.Value) {
			err := handleFlushCheckpointStage(4, val.(int), "before flush checkpoint")
			if err != nil {
				failpoint.Return(err)
			}
		})
		return s.flushCheckPoints()
	}

	return nil
}

func (s *Syncer) saveGlobalPoint(globalLocation binlog.Location) {
	if s.cfg.ShardMode == config.ShardPessimistic {
		// NOTE: for the optimistic mode, because we don't handle conflicts automatically (or no re-direct supported),
		// so it is not need to adjust global checkpoint now, and after re-direct supported this should be updated.
		globalLocation = s.sgk.AdjustGlobalLocation(globalLocation)
	}
	s.checkpoint.SaveGlobalPoint(globalLocation.Clone())
}

func (s *Syncer) resetShardingGroup(schema, table string) {
	if s.cfg.ShardMode == config.ShardPessimistic {
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
	if s.execError.Get() != nil {
		s.tctx.L().Warn("error detected when executing SQL job, skip flush checkpoint", zap.Stringer("checkpoint", s.checkpoint))
		return nil
	}

	var (
		exceptTableIDs map[string]bool
		exceptTables   [][]string
		shardMetaSQLs  []string
		shardMetaArgs  [][]interface{}
	)
	if s.cfg.ShardMode == config.ShardPessimistic {
		// flush all checkpoints except tables which are unresolved for sharding DDL for the pessimistic mode.
		// NOTE: for the optimistic mode, because we don't handle conflicts automatically (or no re-direct supported),
		// so we can simply flush checkpoint for all tables now, and after re-direct supported this should be updated.
		exceptTableIDs, exceptTables = s.sgk.UnresolvedTables()
		s.tctx.L().Info("flush checkpoints except for these tables", zap.Reflect("tables", exceptTables))

		shardMetaSQLs, shardMetaArgs = s.sgk.PrepareFlushSQLs(exceptTableIDs)
		s.tctx.L().Info("prepare flush sqls", zap.Strings("shard meta sqls", shardMetaSQLs), zap.Reflect("shard meta arguments", shardMetaArgs))
	}

	tctx, cancel := s.tctx.WithContext(context.Background()).WithTimeout(maxDMLConnectionDuration)
	defer cancel()
	err := s.checkpoint.FlushPointsExcept(tctx, exceptTables, shardMetaSQLs, shardMetaArgs)
	if err != nil {
		return terror.Annotatef(err, "flush checkpoint %s", s.checkpoint)
	}
	s.tctx.L().Info("flushed checkpoint", zap.Stringer("checkpoint", s.checkpoint))

	// update current active relay log after checkpoint flushed
	err = s.updateActiveRelayLog(s.checkpoint.GlobalPoint().Position)
	if err != nil {
		return err
	}
	return nil
}

// DDL synced one by one, so we only need to process one DDL at a time
func (s *Syncer) syncDDL(tctx *tcontext.Context, queueBucket string, db *DBConn, ddlJobChan chan *job) {
	defer s.wg.Done()

	var err error
	for {
		sqlJob, ok := <-ddlJobChan
		if !ok {
			return
		}

		var (
			ignore           = false
			shardPessimistOp *pessimism.Operation
		)
		switch s.cfg.ShardMode {
		case config.ShardPessimistic:
			shardPessimistOp = s.pessimist.PendingOperation()
			if shardPessimistOp != nil && !shardPessimistOp.Exec {
				ignore = true
				tctx.L().Info("ignore shard DDLs in pessimistic shard mode", zap.Strings("ddls", sqlJob.ddls))
			}
		case config.ShardOptimistic:
			if len(sqlJob.ddls) == 0 {
				ignore = true
				tctx.L().Info("ignore shard DDLs in optimistic mode", log.WrapStringerField("info", s.optimist.PendingInfo()))
			}
		}
		if !ignore {
			var affected int
			affected, err = db.executeSQLWithIgnore(tctx, ignoreDDLError, sqlJob.ddls)
			if err != nil {
				err = s.handleSpecialDDLError(tctx, err, sqlJob.ddls, affected, db)
				err = terror.WithScope(err, terror.ScopeDownstream)
			}
		}
		if err != nil {
			s.appendExecErrors(&ExecErrorContext{
				err:      err,
				location: sqlJob.currentLocation.Clone(),
				jobs:     fmt.Sprintf("%v", sqlJob.ddls),
			})
		}

		switch s.cfg.ShardMode {
		case config.ShardPessimistic:
			// for sharding DDL syncing, send result back
			shardInfo := s.pessimist.PendingInfo()
			if shardInfo == nil {
				// no need to do the shard DDL handle for `CREATE DATABASE/TABLE` now.
				s.tctx.L().Warn("skip shard DDL handle in pessimistic shard mode", zap.Strings("ddl", sqlJob.ddls))
			} else if shardPessimistOp == nil {
				err = terror.ErrWorkerDDLLockOpNotFound.Generate(shardInfo)
			} else {
				err = s.pessimist.DoneOperationDeleteInfo(*shardPessimistOp, *shardInfo)
			}
		case config.ShardOptimistic:
			shardInfo := s.optimist.PendingInfo()
			if shardInfo == nil {
				// no need to do the shard DDL handle for `DROP DATABASE/TABLE` now.
				// but for `CREATE DATABASE` and `ALTER DATABASE` we execute it to the downstream directly without `shardInfo`.
				if ignore { // actually ignored.
					s.tctx.L().Warn("skip shard DDL handle in optimistic shard mode", zap.Strings("ddl", sqlJob.ddls))
				}
			} else if s.optimist.PendingOperation() == nil {
				err = terror.ErrWorkerDDLLockOpNotFound.Generate(shardInfo)
			} else {
				err = s.optimist.DoneOperation(*(s.optimist.PendingOperation()))
			}
		}
		s.jobWg.Done()
		if err != nil {
			s.execError.Set(err)
			if !utils.IsContextCanceledError(err) {
				err = s.handleEventError(err, &sqlJob.startLocation, &sqlJob.currentLocation)
				s.runFatalChan <- unit.NewProcessError(err)
			}
			continue
		}
		s.addCount(true, queueBucket, sqlJob.tp, int64(len(sqlJob.ddls)))
	}
}

func (s *Syncer) sync(tctx *tcontext.Context, queueBucket string, db *DBConn, jobChan chan *job) {
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

	fatalF := func(affected int, err error) {
		s.execError.Set(err)
		if !utils.IsContextCanceledError(err) {
			err = s.handleEventError(err, &jobs[affected].startLocation, &jobs[affected].currentLocation)
			s.runFatalChan <- unit.NewProcessError(err)
		}
		clearF()
	}

	executeSQLs := func() (int, error) {
		if len(jobs) == 0 {
			return 0, nil
		}
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
		affected, err := db.executeSQL(tctx, queries, args...)
		if err != nil {
			errCtx := &ExecErrorContext{err, jobs[affected].currentLocation.Clone(), fmt.Sprintf("%v", jobs)}
			s.appendExecErrors(errCtx)
		}
		return affected, err
	}

	var err error
	var affect int
	for {
		select {
		case sqlJob, ok := <-jobChan:
			queueSizeGauge.WithLabelValues(s.cfg.Name, queueBucket, s.cfg.SourceID).Set(float64(len(jobChan)))
			if !ok {
				return
			}
			idx++

			if sqlJob.tp != flush && len(sqlJob.sql) > 0 {
				jobs = append(jobs, sqlJob)
				tpCnt[sqlJob.tp]++
			}

			if idx >= count || sqlJob.tp == flush {
				affect, err = executeSQLs()
				if err != nil {
					fatalF(affect, err)
					continue
				}
				clearF()
			}

		case <-time.After(waitTime):
			if len(jobs) > 0 {
				affect, err = executeSQLs()
				if err != nil {
					fatalF(affect, err)
					continue
				}
				clearF()
			}
		}
	}
}

// Run starts running for sync, we should guarantee it can rerun when paused.
func (s *Syncer) Run(ctx context.Context) (err error) {
	tctx := s.tctx.WithContext(ctx)

	defer func() {
		if s.done != nil {
			close(s.done)
		}
	}()

	fresh, err := s.IsFreshTask(ctx)
	if err != nil {
		return err
	} else if fresh {
		// for fresh task, we try to load checkpoints from meta (file or config item)
		err = s.checkpoint.LoadMeta()
		if err != nil {
			return err
		}

		// for fresh and all-mode task, flush checkpoint so we could delete metadata file
		if s.cfg.Mode == config.ModeAll {
			if err = s.flushCheckPoints(); err != nil {
				s.tctx.L().Warn("fail to flush checkpoints when starting task", zap.Error(err))
			} else if s.cfg.CleanDumpFile {
				s.tctx.L().Info("try to remove loaded files")
				metadataFile := path.Join(s.cfg.Dir, "metadata")
				if err = os.Remove(metadataFile); err != nil {
					s.tctx.L().Warn("error when remove loaded dump file", zap.String("data file", metadataFile), zap.Error(err))
				}
				if err = os.Remove(s.cfg.Dir); err != nil {
					s.tctx.L().Warn("error when remove loaded dump folder", zap.String("data folder", s.cfg.Dir), zap.Error(err))
				}
			}
		}
	}

	// startLocation is the start location for current received event
	// currentLocation is the end location for current received event (End_log_pos in `show binlog events` for mysql)
	// lastLocation is the end location for last received (ROTATE / QUERY / XID) event
	// we use startLocation to replace and skip binlog event of specified position
	// we use currentLocation and update table checkpoint in sharding ddl
	// we use lastLocation to update global checkpoint and table checkpoint
	var (
		currentLocation = s.checkpoint.GlobalPoint() // also init to global checkpoint
		startLocation   = s.checkpoint.GlobalPoint()
		lastLocation    = s.checkpoint.GlobalPoint()
	)
	s.tctx.L().Info("replicate binlog from checkpoint", zap.Stringer("checkpoint", lastLocation))

	if s.streamerController.IsClosed() {
		err = s.streamerController.Start(tctx, lastLocation.Clone())
		if err != nil {
			return terror.Annotate(err, "fail to restart streamer controller")
		}
	}

	s.queueBucketMapping = make([]string, 0, s.cfg.WorkerCount+1)
	for i := 0; i < s.cfg.WorkerCount; i++ {
		s.wg.Add(1)
		name := queueBucketName(i)
		s.queueBucketMapping = append(s.queueBucketMapping, name)
		go func(i int, n string) {
			ctx2, cancel := context.WithCancel(ctx)
			ctctx := s.tctx.WithContext(ctx2)
			s.sync(ctctx, n, s.toDBConns[i], s.jobs[i])
			cancel()
		}(i, name)
	}

	s.queueBucketMapping = append(s.queueBucketMapping, adminQueueName)
	s.wg.Add(1)
	go func() {
		ctx2, cancel := context.WithCancel(ctx)
		ctctx := s.tctx.WithContext(ctx2)
		s.syncDDL(ctctx, adminQueueName, s.ddlDBConn, s.jobs[s.cfg.WorkerCount])
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
			s.tctx.L().Error("panic log", zap.Reflect("error message", err1), zap.Stack("statck"))
			err = terror.ErrSyncerUnitPanic.Generate(err1)
		}

		s.jobWg.Wait()
		if err2 := s.flushCheckPoints(); err2 != nil {
			s.tctx.L().Warn("fail to flush check points when exit task", zap.Error(err2))
		}
	}()

	s.start = time.Now()
	s.lastTime.Lock()
	s.lastTime.t = s.start
	s.lastTime.Unlock()

	tryReSync := true

	// safeMode makes syncer reentrant.
	// we make each operator reentrant to make syncer reentrant.
	// `replace` and `delete` are naturally reentrant.
	// use `delete`+`replace` to represent `update` can make `update`  reentrant.
	// but there are no ways to make `update` idempotent,
	// if we start syncer at an early position, database must bear a period of inconsistent state,
	// it's eventual consistency.
	safeMode := sm.NewSafeMode()
	s.enableSafeModeInitializationPhase(tctx, safeMode)

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
		shardingReSyncCh        = make(chan *ShardingReSync, 10)
		shardingReSync          *ShardingReSync
		savedGlobalLastLocation binlog.Location
		latestOp                opType // latest job operation tp
		traceSource             = fmt.Sprintf("%s.syncer.%s", s.cfg.SourceID, s.cfg.Name)
		traceID                 string
	)

	closeShardingResync := func() error {
		if shardingReSync == nil {
			return nil
		}

		// if remaining DDLs in sequence, redirect global stream to the next sharding DDL position
		if !shardingReSync.allResolved {
			nextLocation, err2 := s.sgk.ActiveDDLFirstLocation(shardingReSync.targetSchema, shardingReSync.targetTable)
			if err2 != nil {
				return err2
			}

			currentLocation = nextLocation.Clone()
			lastLocation = nextLocation.Clone()
		} else {
			currentLocation = savedGlobalLastLocation.Clone()
			lastLocation = savedGlobalLastLocation.Clone() // restore global last pos
		}
		// if suffix>0, we are replacing error
		s.isReplacingErr = (currentLocation.Suffix != 0)

		err3 := s.streamerController.RedirectStreamer(s.tctx, currentLocation.Clone())
		if err3 != nil {
			return err3
		}

		shardingReSync = nil
		return nil
	}

	for {
		s.currentLocationMu.Lock()
		s.currentLocationMu.currentLocation = currentLocation.Clone()
		s.currentLocationMu.Unlock()

		// fetch from sharding resync channel if needed, and redirect global
		// stream to current binlog position recorded by ShardingReSync
		if shardingReSync == nil && len(shardingReSyncCh) > 0 {
			// some sharding groups need to re-syncing
			shardingReSync = <-shardingReSyncCh
			savedGlobalLastLocation = lastLocation.Clone() // save global last location
			lastLocation = shardingReSync.currLocation.Clone()

			currentLocation = shardingReSync.currLocation.Clone()
			// if suffix>0, we are replacing error
			s.isReplacingErr = (currentLocation.Suffix != 0)
			err = s.streamerController.RedirectStreamer(s.tctx, shardingReSync.currLocation.Clone())
			if err != nil {
				return err
			}

			failpoint.Inject("ReSyncExit", func() {
				s.tctx.L().Warn("exit triggered", zap.String("failpoint", "ReSyncExit"))
				utils.OsExit(1)
			})
		}

		var e *replication.BinlogEvent

		if shardingReSync == nil {
			latestOp = null
		}

		startTime := time.Now()
		e, err = s.getEvent(tctx, &currentLocation)

		if err == context.Canceled {
			s.tctx.L().Info("binlog replication main routine quit(context canceled)!", zap.Stringer("last location", lastLocation))
			return nil
		} else if err == context.DeadlineExceeded {
			s.tctx.L().Info("deadline exceeded when fetching binlog event")
			continue
		} else if isDuplicateServerIDError(err) {
			// if the server id is already used, need to use a new server id
			tctx.L().Info("server id is already used by another slave, will change to a new server id and get event again")
			err1 := s.streamerController.UpdateServerIDAndResetReplication(tctx, lastLocation.Clone())
			if err1 != nil {
				return err1
			}
			continue
		}

		if err != nil {
			s.tctx.L().Error("fail to fetch binlog", log.ShortError(err))

			if isConnectionRefusedError(err) {
				return err
			}

			if s.streamerController.CanRetry() {
				err = s.streamerController.ResetReplicationSyncer(s.tctx, lastLocation.Clone())
				if err != nil {
					return err
				}
				continue
			}

			// try to re-sync in gtid mode
			if tryReSync && s.cfg.EnableGTID && isBinlogPurgedError(err) && s.cfg.AutoFixGTID {
				time.Sleep(retryTimeout)
				err = s.reSyncBinlog(*tctx, lastLocation.Clone())
				if err != nil {
					return err
				}
				tryReSync = false
				continue
			}

			return err
		}

		// time duration for reading an event from relay log or upstream master.
		binlogReadDurationHistogram.WithLabelValues(s.cfg.Name, s.cfg.SourceID).Observe(time.Since(startTime).Seconds())
		startTime = time.Now() // reset start time for the next metric.

		// get binlog event, reset tryReSync, so we can re-sync binlog while syncer meets errors next time
		tryReSync = true
		binlogPosGauge.WithLabelValues("syncer", s.cfg.Name, s.cfg.SourceID).Set(float64(e.Header.LogPos))
		index, err := binlog.GetFilenameIndex(lastLocation.Position.Name)
		if err != nil {
			s.tctx.L().Error("fail to get index number of binlog file", log.ShortError(err))
		} else {
			binlogFileGauge.WithLabelValues("syncer", s.cfg.Name, s.cfg.SourceID).Set(float64(index))
		}
		s.binlogSizeCount.Add(int64(e.Header.EventSize))
		binlogEventSizeHistogram.WithLabelValues(s.cfg.Name, s.cfg.SourceID).Observe(float64(e.Header.EventSize))

		failpoint.Inject("ProcessBinlogSlowDown", nil)

		s.tctx.L().Debug("receive binlog event", zap.Reflect("header", e.Header))

		// TODO: support all event
		// we calculate startLocation and endLocation(currentLocation) for Rows/Query event here
		// set startLocation empty for other events to avoid misuse
		startLocation = binlog.Location{}
		switch e.Event.(type) {
		case *replication.RowsEvent, *replication.QueryEvent:
			startLocation = binlog.Location{
				Position: mysql.Position{
					Name: lastLocation.Position.Name,
					Pos:  e.Header.LogPos - e.Header.EventSize,
				},
				GTIDSet: lastLocation.GTIDSet.Clone(),
				Suffix:  currentLocation.Suffix,
			}

			endSuffix := startLocation.Suffix
			if s.isReplacingErr {
				endSuffix++
			}
			currentLocation = binlog.Location{
				Position: mysql.Position{
					Name: lastLocation.Position.Name,
					Pos:  e.Header.LogPos,
				},
				GTIDSet: lastLocation.GTIDSet.Clone(),
				Suffix:  endSuffix,
			}
			if ev, ok := e.Event.(*replication.QueryEvent); ok {
				currentLocation.GTIDSet.Set(ev.GSet)
			}

			if !s.isReplacingErr {
				apply, op := s.errOperatorHolder.MatchAndApply(&startLocation, &currentLocation)
				if apply {
					if op == pb.ErrorOp_Replace {
						s.isReplacingErr = true
						// revert currentLocation to startLocation
						currentLocation = startLocation.Clone()
					}
					// skip the event
					continue
				}
			}
			// set endLocation.Suffix=0 of last replace event
			// also redirect stream to next event
			if currentLocation.Suffix > 0 && e.Header.EventSize > 0 {
				currentLocation.Suffix = 0
				s.isReplacingErr = false
				err = s.streamerController.RedirectStreamer(s.tctx, currentLocation.Clone())
				if err != nil {
					return err
				}
			}
		default:
		}

		// check pass SafeModeExitLoc and try disable safe mode, but not in sharding or replacing error
		if s.cfg.SafeModeExitLoc != nil && !s.isReplacingErr && shardingReSync == nil {
			if binlog.CompareLocation(currentLocation, *s.cfg.SafeModeExitLoc, s.cfg.EnableGTID) >= 0 {
				s.cfg.SafeModeExitLoc = nil
				safeMode.Add(tctx, -1)
			}
		}

		ec := eventContext{
			tctx:                tctx,
			header:              e.Header,
			startLocation:       &startLocation,
			currentLocation:     &currentLocation,
			lastLocation:        &lastLocation,
			shardingReSync:      shardingReSync,
			latestOp:            &latestOp,
			closeShardingResync: closeShardingResync,
			traceSource:         traceSource,
			safeMode:            safeMode,
			tryReSync:           tryReSync,
			startTime:           startTime,
			traceID:             &traceID,
			shardingReSyncCh:    &shardingReSyncCh,
		}

		switch ev := e.Event.(type) {
		case *replication.RotateEvent:
			err = s.handleRotateEvent(ev, ec)
			if err = s.handleEventError(err, &startLocation, &currentLocation); err != nil {
				return err
			}
		case *replication.RowsEvent:
			err = s.handleRowsEvent(ev, ec)
			if err = s.handleEventError(err, &startLocation, &currentLocation); err != nil {
				return err
			}
		case *replication.QueryEvent:
			err = s.handleQueryEvent(ev, ec)
			if err = s.handleEventError(err, &startLocation, &currentLocation); err != nil {
				return err
			}
		case *replication.XIDEvent:
			if shardingReSync != nil {
				shardingReSync.currLocation.Position.Pos = e.Header.LogPos
				shardingReSync.currLocation.Suffix = currentLocation.Suffix
				shardingReSync.currLocation.GTIDSet.Set(ev.GSet)

				// only need compare binlog position?
				lastLocation = shardingReSync.currLocation.Clone()
				if binlog.CompareLocation(shardingReSync.currLocation, shardingReSync.latestLocation, s.cfg.EnableGTID) >= 0 {
					s.tctx.L().Info("re-replicate shard group was completed", zap.String("event", "XID"), zap.Stringer("re-shard", shardingReSync))
					err = closeShardingResync()
					if err != nil {
						return terror.Annotatef(err, "shard group current location %s", shardingReSync.currLocation)
					}
					continue
				}
			}

			latestOp = xid
			currentLocation.Position.Pos = e.Header.LogPos
			currentLocation.GTIDSet.Set(ev.GSet)
			s.tctx.L().Debug("", zap.String("event", "XID"), zap.Stringer("last location", lastLocation), log.WrapStringerField("location", currentLocation))
			lastLocation.Position.Pos = e.Header.LogPos // update lastPos
			lastLocation.GTIDSet.Set(ev.GSet)

			job := newXIDJob(currentLocation, startLocation, currentLocation, traceID)
			err = s.addJobFunc(job)
			if err = s.handleEventError(err, &startLocation, &currentLocation); err != nil {
				return err
			}
		case *replication.GenericEvent:
			switch e.Header.EventType {
			case replication.HEARTBEAT_EVENT:
				// flush checkpoint even if there are no real binlog events
				if s.checkpoint.CheckGlobalPoint() {
					s.tctx.L().Info("meet heartbeat event and then flush jobs")
					err = s.flushJobs()
					if err = s.handleEventError(err, &startLocation, &currentLocation); err != nil {
						return err
					}
				}
			}
		}
	}
}

type eventContext struct {
	tctx                *tcontext.Context
	header              *replication.EventHeader
	startLocation       *binlog.Location
	currentLocation     *binlog.Location
	lastLocation        *binlog.Location
	shardingReSync      *ShardingReSync
	latestOp            *opType
	closeShardingResync func() error
	traceSource         string
	safeMode            *sm.SafeMode
	tryReSync           bool
	startTime           time.Time
	traceID             *string
	parser2             *parser.Parser
	shardingReSyncCh    *chan *ShardingReSync
}

// TODO: Further split into smaller functions and group common arguments into
// a context struct.
func (s *Syncer) handleRotateEvent(ev *replication.RotateEvent, ec eventContext) error {
	if ec.header.Timestamp == 0 || ec.header.LogPos == 0 {
		// it is fake rotate event, ignore it
		return nil
	}

	*ec.currentLocation = binlog.Location{
		Position: mysql.Position{
			Name: string(ev.NextLogName),
			Pos:  uint32(ev.Position),
		},
		GTIDSet: ec.currentLocation.GTIDSet,
	}

	if binlog.CompareLocation(*ec.currentLocation, *ec.lastLocation, s.cfg.EnableGTID) >= 0 {
		*ec.lastLocation = ec.currentLocation.Clone()
	}

	if ec.shardingReSync != nil {
		if binlog.CompareLocation(*ec.currentLocation, ec.shardingReSync.currLocation, s.cfg.EnableGTID) > 0 {
			ec.shardingReSync.currLocation = ec.currentLocation.Clone()
		}

		if binlog.CompareLocation(ec.shardingReSync.currLocation, ec.shardingReSync.latestLocation, s.cfg.EnableGTID) >= 0 {
			s.tctx.L().Info("re-replicate shard group was completed", zap.String("event", "rotate"), zap.Stringer("re-shard", ec.shardingReSync))
			err := ec.closeShardingResync()
			if err != nil {
				return err
			}
		} else {
			s.tctx.L().Debug("re-replicate shard group", zap.String("event", "rotate"), log.WrapStringerField("location", ec.currentLocation), zap.Reflect("re-shard", ec.shardingReSync))
		}
		return nil
	}
	*ec.latestOp = rotate

	s.tctx.L().Info("", zap.String("event", "rotate"), log.WrapStringerField("location", ec.currentLocation))
	return nil
}

func (s *Syncer) handleRowsEvent(ev *replication.RowsEvent, ec eventContext) error {
	originSchema, originTable := string(ev.Table.Schema), string(ev.Table.Table)
	schemaName, tableName := s.renameShardingSchema(originSchema, originTable)

	if ec.shardingReSync != nil {
		ec.shardingReSync.currLocation = ec.currentLocation.Clone()
		if binlog.CompareLocation(ec.shardingReSync.currLocation, ec.shardingReSync.latestLocation, s.cfg.EnableGTID) >= 0 {
			s.tctx.L().Info("re-replicate shard group was completed", zap.String("event", "row"), zap.Stringer("re-shard", ec.shardingReSync))
			return ec.closeShardingResync()
		}
		if ec.shardingReSync.targetSchema != schemaName || ec.shardingReSync.targetTable != tableName {
			// in re-syncing, ignore non current sharding group's events
			s.tctx.L().Debug("skip event in re-replicating shard group", zap.String("event", "row"), zap.Reflect("re-shard", ec.shardingReSync))
			return nil
		}
	}

	// DML position before table checkpoint, ignore it
	if !s.checkpoint.IsNewerTablePoint(originSchema, originTable, *ec.currentLocation, s.cfg.EnableGTID) {
		s.tctx.L().Debug("ignore obsolete event that is old than table checkpoint", zap.String("event", "row"), log.WrapStringerField("location", ec.currentLocation), zap.String("origin schema", originSchema), zap.String("origin table", originTable))
		return nil
	}

	s.tctx.L().Debug("", zap.String("event", "row"), zap.String("origin schema", originSchema), zap.String("origin table", originTable), zap.String("target schema", schemaName), zap.String("target table", tableName), log.WrapStringerField("location", ec.currentLocation), zap.Reflect("raw event data", ev.Rows))

	if s.cfg.EnableHeartbeat {
		s.heartbeat.TryUpdateTaskTs(s.cfg.Name, originSchema, originTable, ev.Rows)
	}

	ignore, err := s.skipDMLEvent(originSchema, originTable, ec.header.EventType)
	if err != nil {
		return err
	}
	if ignore {
		skipBinlogDurationHistogram.WithLabelValues("rows", s.cfg.Name, s.cfg.SourceID).Observe(time.Since(ec.startTime).Seconds())
		// for RowsEvent, we should record lastLocation rather than currentLocation
		return s.recordSkipSQLsLocation(*ec.lastLocation)
	}

	if s.cfg.ShardMode == config.ShardPessimistic {
		source, _ := GenTableID(originSchema, originTable)
		if s.sgk.InSyncing(schemaName, tableName, source, *ec.currentLocation) {
			// if in unsync stage and not before active DDL, ignore it
			// if in sharding re-sync stage and not before active DDL (the next DDL to be synced), ignore it
			s.tctx.L().Debug("replicate sharding DDL, ignore Rows event", zap.String("event", "row"), zap.String("source", source), log.WrapStringerField("location", ec.currentLocation))
			return nil
		}
	}

	// TODO(csuzhangxc): check performance of `getTabel` from schema tracker.
	ti, err := s.getTable(originSchema, originTable, schemaName, tableName)
	if err != nil {
		return terror.WithScope(err, terror.ScopeDownstream)
	}
	rows, err := s.mappingDML(originSchema, originTable, ti, ev.Rows)
	if err != nil {
		return err
	}
	prunedColumns, prunedRows, err := pruneGeneratedColumnDML(ti, rows)
	if err != nil {
		return err
	}

	var (
		applied bool
		sqls    []string
		keys    [][]string
		args    [][]interface{}
	)

	param := &genDMLParam{
		schema:            schemaName,
		table:             tableName,
		data:              prunedRows,
		originalData:      rows,
		columns:           prunedColumns,
		originalTableInfo: ti,
	}

	switch ec.header.EventType {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		if !applied {
			param.safeMode = ec.safeMode.Enable()
			sqls, keys, args, err = genInsertSQLs(param)
			if err != nil {
				return terror.Annotatef(err, "gen insert sqls failed, schema: %s, table: %s", schemaName, tableName)
			}
		}
		binlogEvent.WithLabelValues("write_rows", s.cfg.Name, s.cfg.SourceID).Observe(time.Since(ec.startTime).Seconds())
		*ec.latestOp = insert

	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		if !applied {
			param.safeMode = ec.safeMode.Enable()
			sqls, keys, args, err = genUpdateSQLs(param)
			if err != nil {
				return terror.Annotatef(err, "gen update sqls failed, schema: %s, table: %s", schemaName, tableName)
			}
		}
		binlogEvent.WithLabelValues("update_rows", s.cfg.Name, s.cfg.SourceID).Observe(time.Since(ec.startTime).Seconds())
		*ec.latestOp = update

	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		if !applied {
			sqls, keys, args, err = genDeleteSQLs(param)
			if err != nil {
				return terror.Annotatef(err, "gen delete sqls failed, schema: %s, table: %s", schemaName, tableName)
			}
		}
		binlogEvent.WithLabelValues("delete_rows", s.cfg.Name, s.cfg.SourceID).Observe(time.Since(ec.startTime).Seconds())
		*ec.latestOp = del

	default:
		s.tctx.L().Debug("ignoring unrecognized event", zap.String("event", "row"), zap.Stringer("type", ec.header.EventType))
		return nil
	}

	startTime := time.Now()
	for i := range sqls {
		var arg []interface{}
		var key []string
		if args != nil {
			arg = args[i]
		}
		if keys != nil {
			key = keys[i]
		}
		err = s.commitJob(*ec.latestOp, originSchema, originTable, schemaName, tableName, sqls[i], arg, key, true, *ec.lastLocation, *ec.startLocation, *ec.currentLocation, *ec.traceID)
		if err != nil {
			return err
		}
	}
	dispatchBinlogDurationHistogram.WithLabelValues(ec.latestOp.String(), s.cfg.Name, s.cfg.SourceID).Observe(time.Since(startTime).Seconds())
	return nil
}

func (s *Syncer) handleQueryEvent(ev *replication.QueryEvent, ec eventContext) error {
	sql := strings.TrimSpace(string(ev.Query))
	usedSchema := string(ev.Schema)
	parser2, err := event.GetParserForStatusVars(ev.StatusVars)
	if err != nil {
		log.L().Warn("can't determine sql_mode from binlog status_vars, use default parser instead", zap.Error(err))
		parser2 = parser.New()
	}

	parseResult, err := s.parseDDLSQL(sql, parser2, usedSchema)
	if err != nil {
		s.tctx.L().Error("fail to parse statement", zap.String("event", "query"), zap.String("statement", sql), zap.String("schema", usedSchema), zap.Stringer("last location", ec.lastLocation), log.WrapStringerField("location", ec.currentLocation), log.ShortError(err))
		return err
	}

	if parseResult.ignore {
		skipBinlogDurationHistogram.WithLabelValues("query", s.cfg.Name, s.cfg.SourceID).Observe(time.Since(ec.startTime).Seconds())
		s.tctx.L().Warn("skip event", zap.String("event", "query"), zap.String("statement", sql), zap.String("schema", usedSchema))
		*ec.lastLocation = ec.currentLocation.Clone() // before record skip location, update lastLocation
		return s.recordSkipSQLsLocation(*ec.lastLocation)
	}
	if !parseResult.isDDL {
		// skipped sql maybe not a DDL (like `BEGIN`)
		return nil
	}

	if ec.shardingReSync != nil {
		ec.shardingReSync.currLocation = ec.currentLocation.Clone()
		if binlog.CompareLocation(ec.shardingReSync.currLocation, ec.shardingReSync.latestLocation, s.cfg.EnableGTID) >= 0 {
			s.tctx.L().Info("re-replicate shard group was completed", zap.String("event", "query"), zap.String("statement", sql), zap.Stringer("re-shard", ec.shardingReSync))
			err2 := ec.closeShardingResync()
			if err2 != nil {
				return err2
			}
		} else {
			// in re-syncing, we can simply skip all DDLs,
			// as they have been added to sharding DDL sequence
			// only update lastPos when the query is a real DDL
			*ec.lastLocation = ec.shardingReSync.currLocation.Clone()
			s.tctx.L().Debug("skip event in re-replicating sharding group", zap.String("event", "query"), zap.String("statement", sql), zap.Reflect("re-shard", ec.shardingReSync))
		}
		return nil
	}

	s.tctx.L().Info("", zap.String("event", "query"), zap.String("statement", sql), zap.String("schema", usedSchema), zap.Stringer("last location", ec.lastLocation), log.WrapStringerField("location", ec.currentLocation))
	*ec.lastLocation = ec.currentLocation.Clone() // update lastLocation, because we have checked `isDDL`
	*ec.latestOp = ddl

	var (
		sqls                []string
		onlineDDLTableNames map[string]*filter.Table
	)

	// for DDL, we don't apply operator until we try to execute it. so can handle sharding cases
	// We use default parser because inside function where need parser, sqls are came from parserpkg.SplitDDL, which is StringSingleQuotes, KeyWordUppercase and NameBackQuotes
	sqls, onlineDDLTableNames, err = s.resolveDDLSQL(ec.tctx, parser.New(), parseResult.stmt, usedSchema)
	if err != nil {
		s.tctx.L().Error("fail to resolve statement", zap.String("event", "query"), zap.String("statement", sql), zap.String("schema", usedSchema), zap.Stringer("last location", ec.lastLocation), log.WrapStringerField("location", ec.currentLocation), log.ShortError(err))
		return err
	}
	s.tctx.L().Info("resolve sql", zap.String("event", "query"), zap.String("raw statement", sql), zap.Strings("statements", sqls), zap.String("schema", usedSchema), zap.Stringer("last location", ec.lastLocation), zap.Stringer("location", ec.currentLocation))

	if len(onlineDDLTableNames) > 1 {
		return terror.ErrSyncerUnitOnlineDDLOnMultipleTable.Generate(string(ev.Query))
	}

	binlogEvent.WithLabelValues("query", s.cfg.Name, s.cfg.SourceID).Observe(time.Since(ec.startTime).Seconds())

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
		needTrackDDLs  []trackedDDL
		sourceTbls     = make(map[string]map[string]struct{}) // db name -> tb name
	)
	for _, sql := range sqls {
		// We use default parser because sqls are came from above *Syncer.resolveDDLSQL, which is StringSingleQuotes, KeyWordUppercase and NameBackQuotes
		sqlDDL, tableNames, stmt, handleErr := s.handleDDL(parser.New(), usedSchema, sql)
		if handleErr != nil {
			return handleErr
		}
		if len(sqlDDL) == 0 {
			skipBinlogDurationHistogram.WithLabelValues("query", s.cfg.Name, s.cfg.SourceID).Observe(time.Since(ec.startTime).Seconds())
			s.tctx.L().Warn("skip event", zap.String("event", "query"), zap.String("statement", sql), zap.String("schema", usedSchema))
			continue
		}

		// for DDL, we wait it to be executed, so we can check if event is newer in this syncer's main process goroutine
		// ignore obsolete DDL here can avoid to try-sync again for already synced DDLs
		if !s.checkpoint.IsNewerTablePoint(tableNames[0][0].Schema, tableNames[0][0].Name, *ec.currentLocation, false) {
			s.tctx.L().Info("ignore obsolete DDL", zap.String("event", "query"), zap.String("statement", sql), log.WrapStringerField("location", ec.currentLocation))
			continue
		}

		// pre-filter of sharding
		if s.cfg.ShardMode == config.ShardPessimistic {
			switch stmt.(type) {
			case *ast.DropDatabaseStmt:
				err = s.dropSchemaInSharding(ec.tctx, tableNames[0][0].Schema)
				if err != nil {
					return err
				}
				continue
			case *ast.DropTableStmt:
				sourceID, _ := GenTableID(tableNames[0][0].Schema, tableNames[0][0].Name)
				err = s.sgk.LeaveGroup(tableNames[1][0].Schema, tableNames[1][0].Name, []string{sourceID})
				if err != nil {
					return err
				}
				err = s.checkpoint.DeleteTablePoint(ec.tctx, tableNames[0][0].Schema, tableNames[0][0].Name)
				if err != nil {
					return err
				}
				continue
			case *ast.TruncateTableStmt:
				s.tctx.L().Info("ignore truncate table statement in shard group", zap.String("event", "query"), zap.String("statement", sqlDDL))
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
					return terror.ErrSyncerUnitDDLOnMultipleTable.Generate(string(ev.Query))
				}
			}
		} else if s.cfg.ShardMode == config.ShardOptimistic {
			switch stmt.(type) {
			case *ast.TruncateTableStmt:
				s.tctx.L().Info("ignore truncate table statement in shard group", zap.String("event", "query"), zap.String("statement", sqlDDL))
				continue
			}
		}

		needHandleDDLs = append(needHandleDDLs, sqlDDL)
		needTrackDDLs = append(needTrackDDLs, trackedDDL{rawSQL: sql, stmt: stmt, tableNames: tableNames})
		// TODO: current table checkpoints will be deleted in track ddls, but created and updated in flush checkpoints,
		//       we should use a better mechanism to combine these operations
		recordSourceTbls(sourceTbls, stmt, tableNames[0][0])
	}

	s.tctx.L().Info("prepare to handle ddls", zap.String("event", "query"), zap.Strings("ddls", needHandleDDLs), zap.ByteString("raw statement", ev.Query), log.WrapStringerField("location", ec.currentLocation))
	if len(needHandleDDLs) == 0 {
		s.tctx.L().Info("skip event, need handled ddls is empty", zap.String("event", "query"), zap.ByteString("raw statement", ev.Query), log.WrapStringerField("location", ec.currentLocation))
		return s.recordSkipSQLsLocation(*ec.lastLocation)
	}

	// interrupted before flush old checkpoint.
	failpoint.Inject("FlushCheckpointStage", func(val failpoint.Value) {
		err = handleFlushCheckpointStage(0, val.(int), "before flush old checkpoint")
		if err != nil {
			failpoint.Return(err)
		}
	})

	// flush previous DMLs and checkpoint if needing to handle the DDL.
	// NOTE: do this flush before operations on shard groups which may lead to skip a table caused by `UnresolvedTables`.
	if err = s.flushJobs(); err != nil {
		return err
	}

	if s.cfg.ShardMode == "" {
		s.tctx.L().Info("start to handle ddls in normal mode", zap.String("event", "query"), zap.Strings("ddls", needHandleDDLs), zap.ByteString("raw statement", ev.Query), log.WrapStringerField("location", ec.currentLocation))

		// interrupted after flush old checkpoint and before track DDL.
		failpoint.Inject("FlushCheckpointStage", func(val failpoint.Value) {
			err = handleFlushCheckpointStage(1, val.(int), "before track DDL")
			if err != nil {
				failpoint.Return(err)
			}
		})

		// run trackDDL before add ddl job to make sure checkpoint can be flushed
		for _, td := range needTrackDDLs {
			if err = s.trackDDL(usedSchema, td.rawSQL, td.tableNames, td.stmt, &ec); err != nil {
				return err
			}
		}

		// interrupted after track DDL and before execute DDL.
		failpoint.Inject("FlushCheckpointStage", func(val failpoint.Value) {
			err = handleFlushCheckpointStage(2, val.(int), "before execute DDL")
			if err != nil {
				failpoint.Return(err)
			}
		})

		job := newDDLJob(nil, needHandleDDLs, *ec.lastLocation, *ec.startLocation, *ec.currentLocation, *ec.traceID, sourceTbls)
		err = s.addJobFunc(job)
		if err != nil {
			return err
		}

		// when add ddl job, will execute ddl and then flush checkpoint.
		// if execute ddl failed, the execErrorDetected will be true.
		err = s.execError.Get()
		if err != nil {
			return terror.ErrSyncerUnitHandleDDLFailed.Delegate(err, ev.Query)
		}

		s.tctx.L().Info("finish to handle ddls in normal mode", zap.String("event", "query"), zap.Strings("ddls", needHandleDDLs), zap.ByteString("raw statement", ev.Query), log.WrapStringerField("location", ec.currentLocation))

		for _, table := range onlineDDLTableNames {
			s.tctx.L().Info("finish online ddl and clear online ddl metadata in normal mode", zap.String("event", "query"), zap.Strings("ddls", needHandleDDLs), zap.ByteString("raw statement", ev.Query), zap.String("schema", table.Schema), zap.String("table", table.Name))
			err = s.onlineDDL.Finish(ec.tctx, table.Schema, table.Name)
			if err != nil {
				return terror.Annotatef(err, "finish online ddl on %s.%s", table.Schema, table.Name)
			}
		}

		return nil
	}

	// handle shard DDL in optimistic mode.
	if s.cfg.ShardMode == config.ShardOptimistic {
		return s.handleQueryEventOptimistic(ev, ec, needHandleDDLs, needTrackDDLs, onlineDDLTableNames)
	}

	// handle sharding ddl
	var (
		needShardingHandle bool
		group              *ShardingGroup
		synced             bool
		active             bool
		remain             int
		source             string
	)
	// for sharding DDL, the firstPos should be the `Pos` of the binlog, not the `End_log_pos`
	// so when restarting before sharding DDLs synced, this binlog can be re-sync again to trigger the TrySync
	startLocation := ec.startLocation.Clone()

	source, _ = GenTableID(ddlInfo.tableNames[0][0].Schema, ddlInfo.tableNames[0][0].Name)

	var annotate string
	switch ddlInfo.stmt.(type) {
	case *ast.CreateDatabaseStmt:
		// for CREATE DATABASE, we do nothing. when CREATE TABLE under this DATABASE, sharding groups will be added
	case *ast.CreateTableStmt:
		// for CREATE TABLE, we add it to group
		needShardingHandle, group, synced, remain, err = s.sgk.AddGroup(ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name, []string{source}, nil, true)
		if err != nil {
			return err
		}
		annotate = "add table to shard group"
	default:
		needShardingHandle, group, synced, active, remain, err = s.sgk.TrySync(ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name, source, startLocation.Clone(), ec.currentLocation.Clone(), needHandleDDLs)
		if err != nil {
			return err
		}
		annotate = "try to sync table in shard group"
		// meets DDL that will not be processed in sequence sharding
		if !active {
			s.tctx.L().Info("skip in-activeDDL", zap.String("event", "query"), zap.String("source", source), zap.Strings("ddls", needHandleDDLs), zap.ByteString("raw statement", ev.Query), zap.Bool("in-sharding", needShardingHandle), zap.Stringer("start location", startLocation), zap.Bool("is-synced", synced), zap.Int("unsynced", remain))
			return nil
		}
	}

	s.tctx.L().Info(annotate, zap.String("event", "query"), zap.String("source", source), zap.Strings("ddls", needHandleDDLs), zap.ByteString("raw statement", ev.Query), zap.Bool("in-sharding", needShardingHandle), zap.Stringer("start location", startLocation), zap.Bool("is-synced", synced), zap.Int("unsynced", remain))

	// interrupted after flush old checkpoint and before track DDL.
	failpoint.Inject("FlushCheckpointStage", func(val failpoint.Value) {
		err = handleFlushCheckpointStage(1, val.(int), "before track DDL")
		if err != nil {
			failpoint.Return(err)
		}
	})

	for _, td := range needTrackDDLs {
		if err = s.trackDDL(usedSchema, td.rawSQL, td.tableNames, td.stmt, &ec); err != nil {
			return err
		}
	}

	if needShardingHandle {
		target, _ := GenTableID(ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name)
		unsyncedTableGauge.WithLabelValues(s.cfg.Name, target, s.cfg.SourceID).Set(float64(remain))
		err = ec.safeMode.IncrForTable(s.tctx, ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name) // try enable safe-mode when starting syncing for sharding group
		if err != nil {
			return err
		}

		// save checkpoint in memory, don't worry, if error occurred, we can rollback it
		// for non-last sharding DDL's table, this checkpoint will be used to skip binlog event when re-syncing
		// NOTE: when last sharding DDL executed, all this checkpoints will be flushed in the same txn
		s.tctx.L().Info("save table checkpoint for source", zap.String("event", "query"), zap.String("source", source), zap.Stringer("start location", startLocation), log.WrapStringerField("end location", ec.currentLocation))
		s.saveTablePoint(ddlInfo.tableNames[0][0].Schema, ddlInfo.tableNames[0][0].Name, ec.currentLocation.Clone())
		if !synced {
			s.tctx.L().Info("source shard group is not synced", zap.String("event", "query"), zap.String("source", source), zap.Stringer("start location", startLocation), log.WrapStringerField("end location", ec.currentLocation))
			return nil
		}

		s.tctx.L().Info("source shard group is synced", zap.String("event", "query"), zap.String("source", source), zap.Stringer("start location", startLocation), log.WrapStringerField("end location", ec.currentLocation))
		err = ec.safeMode.DescForTable(s.tctx, ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name) // try disable safe-mode after sharding group synced
		if err != nil {
			return err
		}
		// maybe multi-groups' sharding DDL synced in this for-loop (one query-event, multi tables)
		if cap(*ec.shardingReSyncCh) < len(sqls) {
			*ec.shardingReSyncCh = make(chan *ShardingReSync, len(sqls))
		}
		firstEndLocation := group.FirstEndPosUnresolved()
		if firstEndLocation == nil {
			return terror.ErrSyncerUnitFirstEndPosNotFound.Generate(source)
		}

		allResolved, err2 := s.sgk.ResolveShardingDDL(ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name)
		if err2 != nil {
			return err2
		}
		*ec.shardingReSyncCh <- &ShardingReSync{
			currLocation:   firstEndLocation.Clone(),
			latestLocation: ec.currentLocation.Clone(),
			targetSchema:   ddlInfo.tableNames[1][0].Schema,
			targetTable:    ddlInfo.tableNames[1][0].Name,
			allResolved:    allResolved,
		}

		// Don't send new DDLInfo to dm-master until all local sql jobs finished
		// since jobWg is flushed by flushJobs before, we don't wait here any more

		// NOTE: if we need singleton Syncer (without dm-master) to support sharding DDL sync
		// we should add another config item to differ, and do not save DDLInfo, and not wait for ddlExecInfo

		// construct & send shard DDL info into etcd, DM-master will handle it.
		shardInfo := s.pessimist.ConstructInfo(ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name, needHandleDDLs)
		rev, err2 := s.pessimist.PutInfo(ec.tctx.Ctx, shardInfo)
		if err2 != nil {
			return err2
		}
		shardLockResolving.WithLabelValues(s.cfg.Name, s.cfg.SourceID).Set(1) // block and wait DDL lock to be synced
		s.tctx.L().Info("putted shard DDL info", zap.Stringer("info", shardInfo), zap.Int64("revision", rev))

		shardOp, err2 := s.pessimist.GetOperation(ec.tctx.Ctx, shardInfo, rev+1)
		shardLockResolving.WithLabelValues(s.cfg.Name, s.cfg.SourceID).Set(0)
		if err2 != nil {
			return err2
		}

		if shardOp.Exec {
			failpoint.Inject("ShardSyncedExecutionExit", func() {
				s.tctx.L().Warn("exit triggered", zap.String("failpoint", "ShardSyncedExecutionExit"))
				s.flushCheckPoints()
				utils.OsExit(1)
			})
			failpoint.Inject("SequenceShardSyncedExecutionExit", func() {
				group := s.sgk.Group(ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name)
				if group != nil {
					// exit in the first round sequence sharding DDL only
					if group.meta.ActiveIdx() == 1 {
						s.tctx.L().Warn("exit triggered", zap.String("failpoint", "SequenceShardSyncedExecutionExit"))
						s.flushCheckPoints()
						utils.OsExit(1)
					}
				}
			})

			s.tctx.L().Info("execute DDL job", zap.String("event", "query"), zap.String("source", source), zap.ByteString("raw statement", ev.Query), zap.Stringer("start location", startLocation), log.WrapStringerField("end location", ec.currentLocation), zap.Stringer("operation", shardOp))
		} else {
			s.tctx.L().Info("ignore DDL job", zap.String("event", "query"), zap.String("source", source), zap.ByteString("raw statement", ev.Query), zap.Stringer("start location", startLocation), log.WrapStringerField("end location", ec.currentLocation), zap.Stringer("operation", shardOp))
		}
	}

	s.tctx.L().Info("start to handle ddls in shard mode", zap.String("event", "query"), zap.Strings("ddls", needHandleDDLs), zap.ByteString("raw statement", ev.Query), zap.Stringer("start location", startLocation), log.WrapStringerField("end location", ec.currentLocation))

	// interrupted after track DDL and before execute DDL.
	failpoint.Inject("FlushCheckpointStage", func(val failpoint.Value) {
		err = handleFlushCheckpointStage(2, val.(int), "before execute DDL")
		if err != nil {
			failpoint.Return(err)
		}
	})

	job := newDDLJob(ddlInfo, needHandleDDLs, *ec.lastLocation, *ec.startLocation, *ec.currentLocation, *ec.traceID, nil)
	err = s.addJobFunc(job)
	if err != nil {
		return err
	}

	err = s.execError.Get()
	if err != nil {
		return terror.ErrSyncerUnitHandleDDLFailed.Delegate(err, ev.Query)
	}

	if len(onlineDDLTableNames) > 0 {
		err = s.clearOnlineDDL(ec.tctx, ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name)
		if err != nil {
			return err
		}
	}

	s.tctx.L().Info("finish to handle ddls in shard mode", zap.String("event", "query"), zap.Strings("ddls", needHandleDDLs), zap.ByteString("raw statement", ev.Query), zap.Stringer("start location", startLocation), log.WrapStringerField("end location", ec.currentLocation))
	return nil
}

func (s *Syncer) trackDDL(usedSchema string, sql string, tableNames [][]*filter.Table, stmt ast.StmtNode, ec *eventContext) error {
	srcTable := tableNames[0][0]

	// Make sure the tables are all loaded into the schema tracker.
	var shouldExecDDLOnSchemaTracker, shouldSchemaExist, shouldTableExist bool
	switch stmt.(type) {
	case *ast.CreateDatabaseStmt:
		shouldExecDDLOnSchemaTracker = true
	case *ast.AlterDatabaseStmt:
		shouldExecDDLOnSchemaTracker = true
		shouldSchemaExist = true
	case *ast.DropDatabaseStmt:
		shouldExecDDLOnSchemaTracker = true
		if s.cfg.ShardMode == "" {
			if err := s.checkpoint.DeleteSchemaPoint(ec.tctx, srcTable.Schema); err != nil {
				return err
			}
		}
	case *ast.CreateTableStmt, *ast.CreateViewStmt, *ast.RecoverTableStmt:
		shouldExecDDLOnSchemaTracker = true
		shouldSchemaExist = true
	case *ast.DropTableStmt:
		shouldExecDDLOnSchemaTracker = true
		if err := s.checkpoint.DeleteTablePoint(ec.tctx, srcTable.Schema, srcTable.Name); err != nil {
			return err
		}
	case *ast.RenameTableStmt, *ast.CreateIndexStmt, *ast.DropIndexStmt, *ast.RepairTableStmt, *ast.AlterTableStmt:
		// TODO: RENAME TABLE / ALTER TABLE RENAME should require special treatment.
		shouldExecDDLOnSchemaTracker = true
		shouldSchemaExist = true
		shouldTableExist = true
	case *ast.LockTablesStmt, *ast.UnlockTablesStmt, *ast.CleanupTableLockStmt, *ast.TruncateTableStmt:
		break
	default:
		s.tctx.L().DPanic("unhandled DDL type cannot be tracked", zap.Stringer("type", reflect.TypeOf(stmt)))
	}

	if shouldSchemaExist {
		if err := s.schemaTracker.CreateSchemaIfNotExists(srcTable.Schema); err != nil {
			return terror.ErrSchemaTrackerCannotCreateSchema.Delegate(err, srcTable.Schema)
		}
	}
	if shouldTableExist {
		targetTable := tableNames[1][0]
		if _, err := s.getTable(srcTable.Schema, srcTable.Name, targetTable.Schema, targetTable.Name); err != nil {
			return err
		}
	}

	if shouldExecDDLOnSchemaTracker {
		if err := s.schemaTracker.Exec(s.tctx.Ctx, usedSchema, sql); err != nil {
			s.tctx.L().Error("cannot track DDL", zap.String("schema", usedSchema), zap.String("statement", sql), log.WrapStringerField("location", ec.currentLocation), log.ShortError(err))
			return terror.ErrSchemaTrackerCannotExecDDL.Delegate(err, sql)
		}
	}

	return nil
}

func (s *Syncer) commitJob(tp opType, sourceSchema, sourceTable, targetSchema, targetTable, sql string, args []interface{}, keys []string, retry bool, location, startLocation, cmdLocation binlog.Location, traceID string) error {
	startTime := time.Now()
	key, err := s.resolveCasuality(keys)
	if err != nil {
		return terror.ErrSyncerUnitResolveCasualityFail.Generate(err)
	}
	s.tctx.L().Debug("key for keys", zap.String("key", key), zap.Strings("keys", keys))
	conflictDetectDurationHistogram.WithLabelValues(s.cfg.Name, s.cfg.SourceID).Observe(time.Since(startTime).Seconds())

	job := newJob(tp, sourceSchema, sourceTable, targetSchema, targetTable, sql, args, key, location, startLocation, cmdLocation, traceID)
	return s.addJobFunc(job)
}

func (s *Syncer) resolveCasuality(keys []string) (string, error) {
	if s.cfg.DisableCausality {
		if len(keys) > 0 {
			return keys[0], nil
		}
		return "", nil
	}

	if s.c.detectConflict(keys) {
		s.tctx.L().Debug("meet causality key, will generate a flush job and wait all sqls executed", zap.Strings("keys", keys))
		if err := s.flushJobs(); err != nil {
			return "", err
		}
		s.c.reset()
	}
	if err := s.c.add(keys); err != nil {
		return "", err
	}
	var key string
	if len(keys) > 0 {
		key = keys[0]
	}
	return s.c.get(key), nil
}

func (s *Syncer) genRouter() error {
	s.tableRouter, _ = router.NewTableRouter(s.cfg.CaseSensitive, []*router.TableRule{})
	for _, rule := range s.cfg.RouteRules {
		err := s.tableRouter.AddRule(rule)
		if err != nil {
			return terror.ErrSyncerUnitGenTableRouter.Delegate(err)
		}
	}
	return nil
}

func (s *Syncer) printStatus(ctx context.Context) {
	defer s.wg.Done()

	failpoint.Inject("PrintStatusCheckSeconds", func(val failpoint.Value) {
		if seconds, ok := val.(int); ok {
			statusTime = time.Duration(seconds) * time.Second
			s.tctx.L().Info("set printStatusInterval", zap.Int("value", seconds), zap.String("failpoint", "PrintStatusCheckSeconds"))
		}
	})

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
			s.tctx.L().Info("print status routine exits", log.ShortError(ctx.Err()))
			return
		case <-timer.C:
			now := time.Now()
			s.lastTime.RLock()
			seconds := now.Unix() - s.lastTime.t.Unix()
			s.lastTime.RUnlock()
			totalSeconds := now.Unix() - s.start.Unix()
			last := s.lastCount.Get()
			total := s.count.Get()

			totalBinlogSize := s.binlogSizeCount.Get()
			lastBinlogSize := s.lastBinlogSizeCount.Get()

			tps, totalTps := int64(0), int64(0)
			if seconds > 0 {
				tps = (total - last) / seconds
				totalTps = total / totalSeconds

				s.currentLocationMu.RLock()
				currentLocation := s.currentLocationMu.currentLocation.Clone()
				s.currentLocationMu.RUnlock()

				remainingSize, err2 := s.fromDB.countBinaryLogsSize(currentLocation.Position)
				if err2 != nil {
					// log the error, but still handle the rest operation
					s.tctx.L().Error("fail to estimate unreplicated binlog size", zap.Error(err2))
				} else {
					bytesPerSec := (totalBinlogSize - lastBinlogSize) / seconds
					if bytesPerSec > 0 {
						remainingSeconds := remainingSize / bytesPerSec
						s.tctx.L().Info("binlog replication progress",
							zap.Int64("total binlog size", totalBinlogSize),
							zap.Int64("last binlog size", lastBinlogSize),
							zap.Int64("cost time", seconds),
							zap.Int64("bytes/Second", bytesPerSec),
							zap.Int64("unsynced binlog size", remainingSize),
							zap.Int64("estimate time to catch up", remainingSeconds))
						remainingTimeGauge.WithLabelValues(s.cfg.Name, s.cfg.SourceID).Set(float64(remainingSeconds))
					}
				}
			}

			latestMasterPos, latestmasterGTIDSet, err = s.getMasterStatus()
			if err != nil {
				s.tctx.L().Error("fail to get master status", log.ShortError(err))
			} else {
				binlogPosGauge.WithLabelValues("master", s.cfg.Name, s.cfg.SourceID).Set(float64(latestMasterPos.Pos))
				index, err := binlog.GetFilenameIndex(latestMasterPos.Name)
				if err != nil {
					s.tctx.L().Error("fail to parse binlog file", log.ShortError(err))
				} else {
					binlogFileGauge.WithLabelValues("master", s.cfg.Name, s.cfg.SourceID).Set(float64(index))
				}
			}

			s.tctx.L().Info("binlog replication status",
				zap.Int64("total_events", total),
				zap.Int64("total_tps", totalTps),
				zap.Int64("tps", tps),
				zap.Stringer("master_position", latestMasterPos),
				log.WrapStringerField("master_gtid", latestmasterGTIDSet),
				zap.Stringer("checkpoint", s.checkpoint))

			s.lastCount.Set(total)
			s.lastBinlogSizeCount.Set(totalBinlogSize)
			s.lastTime.Lock()
			s.lastTime.t = time.Now()
			s.lastTime.Unlock()
			s.totalTps.Set(totalTps)
			s.tps.Set(tps)
		}
	}
}

func (s *Syncer) createDBs() error {
	var err error
	dbCfg := s.cfg.From
	dbCfg.RawDBCfg = config.DefaultRawDBConfig().SetReadTimeout(maxDMLConnectionTimeout)
	s.fromDB, err = createUpStreamConn(dbCfg)
	if err != nil {
		return err
	}

	dbCfg = s.cfg.To
	dbCfg.RawDBCfg = config.DefaultRawDBConfig().
		SetReadTimeout(maxDMLConnectionTimeout).
		SetMaxIdleConns(s.cfg.WorkerCount)

	s.toDB, s.toDBConns, err = createConns(s.tctx, s.cfg, dbCfg, s.cfg.WorkerCount)
	if err != nil {
		closeUpstreamConn(s.tctx, s.fromDB) // release resources acquired before return with error
		return err
	}
	// baseConn for ddl
	dbCfg = s.cfg.To
	dbCfg.RawDBCfg = config.DefaultRawDBConfig().SetReadTimeout(maxDDLConnectionTimeout)

	var ddlDBConns []*DBConn
	s.ddlDB, ddlDBConns, err = createConns(s.tctx, s.cfg, dbCfg, 1)
	if err != nil {
		closeUpstreamConn(s.tctx, s.fromDB)
		closeBaseDB(s.tctx, s.toDB)
		return err
	}
	s.ddlDBConn = ddlDBConns[0]

	return nil
}

// closeBaseDB closes all opened DBs, rollback for createConns
func (s *Syncer) closeDBs() {
	closeUpstreamConn(s.tctx, s.fromDB)
	closeBaseDB(s.tctx, s.toDB)
	closeBaseDB(s.tctx, s.ddlDB)
}

// record skip ddl/dml sqls' position
// make newJob's sql argument empty to distinguish normal sql and skips sql
func (s *Syncer) recordSkipSQLsLocation(location binlog.Location) error {
	job := newSkipJob(location)
	return s.addJobFunc(job)
}

func (s *Syncer) flushJobs() error {
	s.tctx.L().Info("flush all jobs", zap.Stringer("global checkpoint", s.checkpoint))
	job := newFlushJob()
	return s.addJobFunc(job)
}

func (s *Syncer) reSyncBinlog(tctx tcontext.Context, location binlog.Location) error {
	err := s.retrySyncGTIDs()
	if err != nil {
		return err
	}
	// close still running sync
	return s.streamerController.ReopenWithRetry(&tctx, location)
}

func (s *Syncer) renameShardingSchema(schema, table string) (string, string) {
	if schema == "" {
		return schema, table
	}
	targetSchema, targetTable, err := s.tableRouter.Route(schema, table)
	if err != nil {
		s.tctx.L().Error("fail to route table", zap.String("schema", schema), zap.String("table", table), zap.Error(err)) // log the error, but still continue
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

	s.removeHeartbeat()

	s.stopSync()

	s.closeDBs()

	s.checkpoint.Close()

	if err := s.schemaTracker.Close(); err != nil {
		s.tctx.L().Error("fail to close schema tracker", log.ShortError(err))
	}

	if s.sgk != nil {
		s.sgk.Close()
	}

	s.closeOnlineDDL()

	// when closing syncer by `stop-task`, remove active relay log from hub
	s.removeActiveRelayLog()

	s.removeLabelValuesWithTaskInMetrics(s.cfg.Name)

	s.closed.Set(true)
}

// stopSync stops syncing, now it used by Close and Pause
// maybe we can refine the workflow more clear
func (s *Syncer) stopSync() {
	if s.done != nil {
		<-s.done // wait Run to return
	}
	s.closeJobChans()
	s.wg.Wait() // wait job workers to return

	// before re-write workflow for s.syncer, simply close it
	// when resuming, re-create s.syncer

	if s.streamerController != nil {
		s.streamerController.Close(s.tctx)
	}
}

func (s *Syncer) closeOnlineDDL() {
	if s.onlineDDL != nil {
		s.onlineDDL.Close()
		s.onlineDDL = nil
	}
}

func (s *Syncer) removeHeartbeat() {
	if s.cfg.EnableHeartbeat {
		s.heartbeat.RemoveTask(s.cfg.Name)
	}
}

// Pause pauses the process, and it can be resumed later
// should cancel context from external
// TODO: it is not a true-meaning Pause because you can't stop it by calling Pause only.
func (s *Syncer) Pause() {
	if s.isClosed() {
		s.tctx.L().Warn("try to pause, but already closed")
		return
	}

	s.stopSync()
}

// Resume resumes the paused process
func (s *Syncer) Resume(ctx context.Context, pr chan pb.ProcessResult) {
	if s.isClosed() {
		s.tctx.L().Warn("try to resume, but already closed")
		return
	}

	// continue the processing
	s.reset()
	// reset database conns
	err := s.resetDBs(s.tctx.WithContext(ctx))
	if err != nil {
		pr <- pb.ProcessResult{
			IsCanceled: false,
			Errors: []*pb.ProcessError{
				unit.NewProcessError(err),
			},
		}
		return
	}
	s.Process(ctx, pr)
}

// Update implements Unit.Update
// now, only support to update config for routes, filters, column-mappings, block-allow-list
// now no config diff implemented, so simply re-init use new config
func (s *Syncer) Update(cfg *config.SubTaskConfig) error {
	if s.cfg.ShardMode == config.ShardPessimistic {
		_, tables := s.sgk.UnresolvedTables()
		if len(tables) > 0 {
			return terror.ErrSyncerUnitUpdateConfigInSharding.Generate(tables)
		}
	}

	var (
		err              error
		oldBaList        *filter.Filter
		oldTableRouter   *router.Table
		oldBinlogFilter  *bf.BinlogEvent
		oldColumnMapping *cm.Mapping
	)

	defer func() {
		if err == nil {
			return
		}
		if oldBaList != nil {
			s.baList = oldBaList
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

	// update block-allow-list
	oldBaList = s.baList
	s.baList, err = filter.New(cfg.CaseSensitive, cfg.BAList)
	if err != nil {
		return terror.ErrSyncerUnitGenBAList.Delegate(err)
	}

	// update route
	oldTableRouter = s.tableRouter
	s.tableRouter, err = router.NewTableRouter(cfg.CaseSensitive, cfg.RouteRules)
	if err != nil {
		return terror.ErrSyncerUnitGenTableRouter.Delegate(err)
	}

	// update binlog filter
	oldBinlogFilter = s.binlogFilter
	s.binlogFilter, err = bf.NewBinlogEvent(cfg.CaseSensitive, cfg.FilterRules)
	if err != nil {
		return terror.ErrSyncerUnitGenBinlogEventFilter.Delegate(err)
	}

	// update column-mappings
	oldColumnMapping = s.columnMapping
	s.columnMapping, err = cm.NewMapping(cfg.CaseSensitive, cfg.ColumnMappingRules)
	if err != nil {
		return terror.ErrSyncerUnitGenColumnMapping.Delegate(err)
	}

	switch s.cfg.ShardMode {
	case config.ShardPessimistic:
		// re-init sharding group
		err = s.sgk.Init()
		if err != nil {
			return err
		}

		err = s.initShardingGroups()
		if err != nil {
			return err
		}
	case config.ShardOptimistic:
		err = s.initOptimisticShardDDL()
		if err != nil {
			return err
		}
	}

	// update l.cfg
	s.cfg.BAList = cfg.BAList
	s.cfg.RouteRules = cfg.RouteRules
	s.cfg.FilterRules = cfg.FilterRules
	s.cfg.ColumnMappingRules = cfg.ColumnMappingRules
	s.cfg.Timezone = cfg.Timezone

	// update timezone
	s.setTimezone()

	return nil
}

// assume that reset master before switching to new master, and only the new master would write
// it's a weak function to try best to fix gtid set while switching master/slave
func (s *Syncer) retrySyncGTIDs() error {
	// NOTE: our (per-table based) checkpoint does not support GTID yet, implement it if needed
	// TODO: support GTID
	s.tctx.L().Warn("our (per-table based) checkpoint does not support GTID yet")
	return nil
}

// checkpointID returns ID which used for checkpoint table
func (s *Syncer) checkpointID() string {
	if len(s.cfg.SourceID) > 0 {
		return s.cfg.SourceID
	}
	return strconv.FormatUint(uint64(s.cfg.ServerID), 10)
}

// UpdateFromConfig updates config for `From`
func (s *Syncer) UpdateFromConfig(cfg *config.SubTaskConfig) error {
	s.Lock()
	defer s.Unlock()
	s.fromDB.BaseDB.Close()

	s.cfg.From = cfg.From

	var err error
	s.cfg.From.RawDBCfg = config.DefaultRawDBConfig().SetReadTimeout(maxDMLConnectionTimeout)
	s.fromDB, err = createUpStreamConn(s.cfg.From)
	if err != nil {
		s.tctx.L().Error("fail to create baseConn connection", log.ShortError(err))
		return err
	}

	err = s.setSyncCfg()
	if err != nil {
		return err
	}

	if s.streamerController != nil {
		s.streamerController.UpdateSyncCfg(s.syncCfg, s.fromDB)
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
		s.tctx.L().Warn("use system default time location")
	}
	s.tctx.L().Info("use timezone", log.WrapStringerField("location", loc))
	s.timezone = loc
}

func (s *Syncer) setSyncCfg() error {
	var tlsConfig *tls.Config
	var err error
	if s.cfg.From.Security != nil {
		tlsConfig, err = toolutils.ToTLSConfig(s.cfg.From.Security.SSLCA, s.cfg.From.Security.SSLCert, s.cfg.From.Security.SSLKey)
		if err != nil {
			return terror.ErrConnInvalidTLSConfig.Delegate(err)
		}
		if tlsConfig != nil {
			tlsConfig.InsecureSkipVerify = true
		}
	}

	syncCfg := replication.BinlogSyncerConfig{
		ServerID:                uint32(s.cfg.ServerID),
		Flavor:                  s.cfg.Flavor,
		Host:                    s.cfg.From.Host,
		Port:                    uint16(s.cfg.From.Port),
		User:                    s.cfg.From.User,
		Password:                s.cfg.From.Password,
		TimestampStringLocation: s.timezone,
		TLSConfig:               tlsConfig,
	}
	// when retry count > 1, go-mysql will retry sync from the previous GTID set in GTID mode,
	// which may get duplicate binlog event after retry success. so just set retry count = 1, and task
	// will exit when meet error, and then auto resume by DM itself.
	common.SetDefaultReplicationCfg(&syncCfg, 1)
	s.syncCfg = syncCfg
	return nil
}

// ShardDDLInfo returns the current pending to handle shard DDL info.
func (s *Syncer) ShardDDLInfo() *pessimism.Info {
	return s.pessimist.PendingInfo()
}

// ShardDDLOperation returns the current pending to handle shard DDL lock operation.
func (s *Syncer) ShardDDLOperation() *pessimism.Operation {
	return s.pessimist.PendingOperation()
}

func (s *Syncer) setErrLocation(startLocation, endLocation *binlog.Location) {
	s.errLocation.Lock()
	defer s.errLocation.Unlock()

	cloneStartLocation := startLocation.ClonePtr()
	cloneEndLocation := endLocation.ClonePtr()

	if s.errLocation.startLocation == nil || cloneStartLocation == nil {
		s.errLocation.startLocation = cloneStartLocation
	} else if binlog.CompareLocation(*cloneStartLocation, *s.errLocation.startLocation, s.cfg.EnableGTID) < 0 {
		s.errLocation.startLocation = cloneStartLocation
	}

	if s.errLocation.endLocation == nil || cloneEndLocation == nil {
		s.errLocation.endLocation = cloneEndLocation
	} else if binlog.CompareLocation(*cloneEndLocation, *s.errLocation.endLocation, s.cfg.EnableGTID) < 0 {
		s.errLocation.endLocation = cloneEndLocation
	}
}

func (s *Syncer) getErrLocation() *binlog.Location {
	s.errLocation.Lock()
	defer s.errLocation.Unlock()
	return s.errLocation.startLocation
}

func (s *Syncer) handleEventError(err error, startLocation, endLocation *binlog.Location) error {
	if err == nil {
		return nil
	}

	s.setErrLocation(startLocation, endLocation)
	return terror.Annotatef(err, "startLocation: [%s], endLocation: [%s]", startLocation, endLocation)
}

// getEvent gets an event from streamerController or errOperatorHolder
func (s *Syncer) getEvent(tctx *tcontext.Context, startLocation *binlog.Location) (*replication.BinlogEvent, error) {
	// next event is a replace event
	if s.isReplacingErr {
		s.tctx.L().Info(fmt.Sprintf("try to get replace event"), zap.Stringer("location", startLocation))
		return s.errOperatorHolder.GetEvent(startLocation)
	}

	return s.streamerController.GetEvent(tctx)
}
