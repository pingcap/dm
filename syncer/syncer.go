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
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	cm "github.com/pingcap/tidb-tools/pkg/column-mapping"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/sync2"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/unit"
	"github.com/pingcap/dm/pkg/binlog"
	"github.com/pingcap/dm/pkg/conn"
	tcontext "github.com/pingcap/dm/pkg/context"
	fr "github.com/pingcap/dm/pkg/func-rollback"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/streamer"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/tracing"
	"github.com/pingcap/dm/pkg/utils"
	sm "github.com/pingcap/dm/syncer/safe-mode"
	operator "github.com/pingcap/dm/syncer/sql-operator"
)

var (
	maxRetryCount = 100

	retryTimeout    = 3 * time.Second
	waitTime        = 10 * time.Millisecond
	eventTimeout    = 1 * time.Minute
	maxEventTimeout = 1 * time.Hour
	statusTime      = 30 * time.Second

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

// StreamerProducer provides the ability to generate binlog streamer by StartSync()
// but go-mysql StartSync() returns (struct, err) rather than (interface, err)
// And we can't simplely use StartSync() method in SteamerProducer
// so use generateStreamer to wrap StartSync() method to make *BinlogSyncer and *BinlogReader in same interface
// For other implementations who implement StreamerProducer and Streamer can easily take place of Syncer.streamProducer
// For test is easy to mock
type StreamerProducer interface {
	generateStreamer(pos mysql.Position) (streamer.Streamer, error)
}

// Read local relay log
type localBinlogReader struct {
	reader *streamer.BinlogReader
}

func (l *localBinlogReader) generateStreamer(pos mysql.Position) (streamer.Streamer, error) {
	return l.reader.StartSync(pos)
}

// Read remote binlog
type remoteBinlogReader struct {
	reader     *replication.BinlogSyncer
	tctx       *tcontext.Context
	EnableGTID bool
}

func (r *remoteBinlogReader) generateStreamer(pos mysql.Position) (streamer.Streamer, error) {
	defer func() {
		lastSlaveConnectionID := r.reader.LastConnectionID()
		r.tctx.L().Info("last slave connection", zap.Uint32("connection ID", lastSlaveConnectionID))
	}()
	if r.EnableGTID {
		// NOTE: our (per-table based) checkpoint does not support GTID yet
		return nil, terror.ErrSyncerUnitRemoteSteamerWithGTID.Generate()
	}

	streamer, err := r.reader.StartSync(pos)
	return streamer, terror.ErrSyncerUnitRemoteSteamerStartSync.Delegate(err)
}

// Syncer can sync your MySQL data to another MySQL database.
type Syncer struct {
	sync.RWMutex

	tctx *tcontext.Context

	cfg     *config.SubTaskConfig
	syncCfg replication.BinlogSyncerConfig

	shardingSyncCfg replication.BinlogSyncerConfig // used by sharding group to re-consume DMLs
	sgk             *ShardingGroupKeeper           // keeper to keep all sharding (sub) group in this syncer
	ddlInfoCh       chan *pb.DDLInfo               // DDL info pending to sync, only support sync one DDL lock one time, refine if needed
	ddlExecInfo     *DDLExecInfo                   // DDL execute (ignore) info
	injectEventCh   chan *replication.BinlogEvent  // extra binlog event chan, used to inject binlog event into the main for loop

	streamerProducer StreamerProducer
	binlogType       BinlogType
	streamer         streamer.Streamer

	wg    sync.WaitGroup
	jobWg sync.WaitGroup

	tables       map[string]*table   // table cache: `target-schema`.`target-table` -> table
	cacheColumns map[string][]string // table columns cache: `target-schema`.`target-table` -> column names list
	genColsCache *GenColCache

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
	bwList        *filter.Filter

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

	addJobFunc func(*job) error
}

// NewSyncer creates a new Syncer.
func NewSyncer(cfg *config.SubTaskConfig) *Syncer {
	syncer := new(Syncer)
	syncer.cfg = cfg
	syncer.tctx = tcontext.Background().WithLogger(log.With(zap.String("task", cfg.Name), zap.String("unit", "binlog replication")))
	syncer.jobsClosed.Set(true) // not open yet
	syncer.closed.Set(false)
	syncer.lastBinlogSizeCount.Set(0)
	syncer.binlogSizeCount.Set(0)
	syncer.lastCount.Set(0)
	syncer.count.Set(0)
	syncer.tables = make(map[string]*table)
	syncer.cacheColumns = make(map[string][]string)
	syncer.genColsCache = NewGenColCache()
	syncer.c = newCausality()
	syncer.done = nil
	syncer.injectEventCh = make(chan *replication.BinlogEvent)
	syncer.tracer = tracing.GetTracer()
	syncer.setTimezone()
	syncer.addJobFunc = syncer.addJob

	syncer.checkpoint = NewRemoteCheckPoint(syncer.tctx, cfg, syncer.checkpointID())

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
		syncer.sgk = NewShardingGroupKeeper(syncer.tctx, cfg)
		syncer.ddlInfoCh = make(chan *pb.DDLInfo, 1)
		syncer.ddlExecInfo = NewDDLExecInfo()
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

	s.bwList, err = filter.New(s.cfg.CaseSensitive, s.cfg.BWList)
	if err != nil {
		return terror.ErrSyncerUnitGenBWList.Delegate(err)
	}

	s.binlogFilter, err = bf.NewBinlogEvent(s.cfg.CaseSensitive, s.cfg.FilterRules)
	if err != nil {
		return terror.ErrSyncerUnitNewBinlogEventFilter.Delegate(err)
	}

	if len(s.cfg.ColumnMappingRules) > 0 {
		s.columnMapping, err = cm.NewMapping(s.cfg.CaseSensitive, s.cfg.ColumnMappingRules)
		if err != nil {
			return terror.ErrSyncerUnitNewColumnMapping.Delegate(err)
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

	if s.cfg.IsSharding {
		err = s.sgk.Init()
		if err != nil {
			return err
		}

		err = s.initShardingGroups()
		if err != nil {
			return err
		}
		rollbackHolder.Add(fr.FuncRollback{Name: "close-sharding-group-keeper", Fn: s.sgk.Close})
	}

	err = s.checkpoint.Init(tctx)
	if err != nil {
		return err
	}
	rollbackHolder.Add(fr.FuncRollback{Name: "close-checkpoint", Fn: s.checkpoint.Close})

	if s.cfg.RemoveMeta {
		err = s.checkpoint.Clear(tctx)
		if err != nil {
			return terror.Annotate(err, "clear checkpoint in syncer")
		}

		if s.onlineDDL != nil {
			err = s.onlineDDL.Clear(tctx)
			if err != nil {
				return terror.Annotate(err, "clear online ddl in syncer")
			}
		}
		s.tctx.L().Info("all previous meta cleared")
	}

	err = s.checkpoint.Load(tctx)
	if err != nil {
		return err
	}
	if s.cfg.EnableHeartbeat {
		s.heartbeat, err = GetHeartbeat(&HeartbeatConfig{
			serverID:       s.cfg.ServerID,
			masterCfg:      s.cfg.From,
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
	sourceTables, err := s.fromDB.fetchAllDoTables(s.bwList)
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

	loadMeta, err2 := s.sgk.LoadShardMeta()
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
	return binlog.ComparePosition(globalPoint, minCheckpoint) <= 0 && len(tablePoint) == 0, nil
}

func (s *Syncer) resetReplicationSyncer() {
	// close old streamerProducer
	if s.streamerProducer != nil {
		switch t := s.streamerProducer.(type) {
		case *remoteBinlogReader:
			s.closeBinlogSyncer(t.reader)
		case *localBinlogReader:
			// TODO: close old local reader before creating a new one
		default:
			// some other producers such as mockStreamerProducer, should not re-create
			return
		}
	}
	// re-create new streamerProducer
	switch s.binlogType {
	case RemoteBinlog:
		s.streamerProducer = &remoteBinlogReader{replication.NewBinlogSyncer(s.syncCfg), s.tctx, s.cfg.EnableGTID}
	case LocalBinlog:
		s.streamerProducer = &localBinlogReader{streamer.NewBinlogReader(s.tctx, &streamer.BinlogReaderConfig{RelayDir: s.cfg.RelayDir, Timezone: s.timezone})}
	default:
		s.tctx.L().Error("init streamerProducer with un-recognized binlogType")
	}
}

func (s *Syncer) reset() {
	s.resetReplicationSyncer()
	// create new job chans
	s.newJobChans(s.cfg.WorkerCount + 1)
	// clear tables info
	s.clearAllTables()

	s.execErrorDetected.Set(false)
	s.resetExecErrors()

	if s.cfg.IsSharding {
		// every time start to re-sync from resume, we reset status to make it like a fresh syncing
		s.sgk.ResetGroups()
		s.ddlExecInfo.Renew()
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
	syncerExitWithErrorCounter.WithLabelValues(s.cfg.Name).Add(0)

	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// create new done chan
	s.done = make(chan struct{})

	runFatalChan := make(chan *pb.ProcessError, s.cfg.WorkerCount+1)
	s.runFatalChan = runFatalChan
	errs := make([]*pb.ProcessError, 0, 2)

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
	s.closeJobChans()   // Run returned, all jobs sent, we can close s.jobs
	s.wg.Wait()         // wait for sync goroutine to return
	close(runFatalChan) // Run returned, all potential fatal sent to s.runFatalChan
	wg.Wait()           // wait for receive all fatal from s.runFatalChan

	if err != nil {
		syncerExitWithErrorCounter.WithLabelValues(s.cfg.Name).Inc()
		errs = append(errs, unit.NewProcessError(pb.ErrorType_UnknownError, err))
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

// clearTables is used for clear table cache of given table. this function must
// be called when DDL is applied to this table.
func (s *Syncer) clearTables(schema, table string) {
	key := dbutil.TableName(schema, table)
	delete(s.tables, key)
	delete(s.cacheColumns, key)
	s.genColsCache.clearTable(schema, table)
}

func (s *Syncer) clearAllTables() {
	s.tables = make(map[string]*table)
	s.cacheColumns = make(map[string][]string)
	s.genColsCache.reset()
}

func (s *Syncer) getTableFromDB(db *DBConn, schema string, name string) (*table, error) {
	table := &table{}
	table.schema = schema
	table.name = name
	table.indexColumns = make(map[string][]*column)

	err := getTableColumns(s.tctx, db, table)
	if err != nil {
		return nil, err
	}

	err = getTableIndex(s.tctx, db, table)
	if err != nil {
		return nil, err
	}

	if len(table.columns) == 0 {
		return nil, terror.ErrSyncerUnitGetTableFromDB.Generate(schema, name)
	}

	return table, nil
}

func (s *Syncer) getTable(schema string, table string) (*table, []string, error) {
	key := dbutil.TableName(schema, table)

	value, ok := s.tables[key]
	if ok {
		return value, s.cacheColumns[key], nil
	}

	t, err := s.getTableFromDB(s.ddlDBConn, schema, table)
	if err != nil {
		return nil, nil, err
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
		s.count.Add(n)
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

func (s *Syncer) addJob(job *job) error {
	var (
		queueBucket int
		execDDLReq  *pb.ExecDDLRequest
	)
	switch job.tp {
	case xid:
		s.saveGlobalPoint(job.pos)
		return nil
	case flush:
		addedJobsTotal.WithLabelValues("flush", s.cfg.Name, adminQueueName).Inc()
		// ugly code addJob and sync, refine it later
		s.jobWg.Add(s.cfg.WorkerCount)
		for i := 0; i < s.cfg.WorkerCount; i++ {
			startTime := time.Now()
			s.jobs[i] <- job
			// flush for every DML queue
			addJobDurationHistogram.WithLabelValues("flush", s.cfg.Name, s.queueBucketMapping[i]).Observe(time.Since(startTime).Seconds())
		}
		s.jobWg.Wait()
		finishedJobsTotal.WithLabelValues("flush", s.cfg.Name, adminQueueName).Inc()
		return s.flushCheckPoints()
	case ddl:
		s.jobWg.Wait()
		addedJobsTotal.WithLabelValues("ddl", s.cfg.Name, adminQueueName).Inc()
		s.jobWg.Add(1)
		queueBucket = s.cfg.WorkerCount
		startTime := time.Now()
		s.jobs[queueBucket] <- job
		addJobDurationHistogram.WithLabelValues("ddl", s.cfg.Name, adminQueueName).Observe(time.Since(startTime).Seconds())
		if job.ddlExecItem != nil {
			execDDLReq = job.ddlExecItem.req
		}
	case insert, update, del:
		s.jobWg.Add(1)
		queueBucket = int(utils.GenHashKey(job.key)) % s.cfg.WorkerCount
		s.addCount(false, s.queueBucketMapping[queueBucket], job.tp, 1)
		startTime := time.Now()
		s.jobs[queueBucket] <- job
		addJobDurationHistogram.WithLabelValues(job.tp.String(), s.cfg.Name, s.queueBucketMapping[queueBucket]).Observe(time.Since(startTime).Seconds())
	}

	if s.tracer.Enable() {
		_, err := s.tracer.CollectSyncerJobEvent(job.traceID, job.traceGID, int32(job.tp), job.pos, job.currentPos, s.queueBucketMapping[queueBucket], job.sql, job.ddls, job.args, execDDLReq, pb.SyncerJobState_queued)
		if err != nil {
			s.tctx.L().Error("fail to collect binlog replication job event", log.ShortError(err))
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
		return s.flushCheckPoints()
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
		s.tctx.L().Warn("error detected when executing SQL job, skip flush checkpoint", zap.Stringer("checkpoint", s.checkpoint))
		return nil
	}

	var (
		exceptTableIDs map[string]bool
		exceptTables   [][]string
		shardMetaSQLs  []string
		shardMetaArgs  [][]interface{}
	)
	if s.cfg.IsSharding {
		// flush all checkpoints except tables which are unresolved for sharding DDL
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
	err = s.updateActiveRelayLog(s.checkpoint.GlobalPoint())
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

		if sqlJob.ddlExecItem != nil && sqlJob.ddlExecItem.req != nil && !sqlJob.ddlExecItem.req.Exec {
			tctx.L().Info("ignore sharding DDLs", zap.Strings("ddls", sqlJob.ddls))
		} else {
			var affected int
			affected, err = db.executeSQLWithIgnore(tctx, ignoreDDLError, sqlJob.ddls)
			if err != nil {
				err = s.handleSpecialDDLError(tctx, err, sqlJob.ddls, affected, db)
				err = terror.WithScope(err, terror.ScopeDownstream)
			}

			if s.tracer.Enable() {
				syncerJobState := s.tracer.FinishedSyncerJobState(err)
				var execDDLReq *pb.ExecDDLRequest
				if sqlJob.ddlExecItem != nil {
					execDDLReq = sqlJob.ddlExecItem.req
				}
				_, traceErr := s.tracer.CollectSyncerJobEvent(sqlJob.traceID, sqlJob.traceGID, int32(sqlJob.tp), sqlJob.pos, sqlJob.currentPos, queueBucket, sqlJob.sql, sqlJob.ddls, nil, execDDLReq, syncerJobState)
				if traceErr != nil {
					tctx.L().Error("fail to collect binlog replication job event", log.ShortError(traceErr))
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

		if s.cfg.IsSharding {
			// for sharding DDL syncing, send result back
			if sqlJob.ddlExecItem != nil {
				sqlJob.ddlExecItem.resp <- err
			}
			s.ddlExecInfo.ClearBlockingDDL()
		}
		s.jobWg.Done()
		if err != nil {
			s.execErrorDetected.Set(true)
			if !utils.IsContextCanceledError(err) {
				s.runFatalChan <- unit.NewProcessError(pb.ErrorType_ExecSQL, err)
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

	fatalF := func(err error, errType pb.ErrorType) {
		s.execErrorDetected.Set(true)
		if !utils.IsContextCanceledError(err) {
			s.runFatalChan <- unit.NewProcessError(errType, err)
		}
		clearF()
	}

	executeSQLs := func() error {
		if len(jobs) == 0 {
			return nil
		}
		queries := make([]string, 0, len(jobs))
		args := make([][]interface{}, 0, len(jobs))
		for _, j := range jobs {
			queries = append(queries, j.sql)
			args = append(args, j.args)
		}
		affected, err := db.executeSQL(tctx, queries, args...)
		if err != nil {
			errCtx := &ExecErrorContext{err, jobs[affected].currentPos, fmt.Sprintf("%v", jobs)}
			s.appendExecErrors(errCtx)
		}
		if s.tracer.Enable() {
			syncerJobState := s.tracer.FinishedSyncerJobState(err)
			for _, job := range jobs {
				_, err2 := s.tracer.CollectSyncerJobEvent(job.traceID, job.traceGID, int32(job.tp), job.pos, job.currentPos, queueBucket, job.sql, job.ddls, nil, nil, syncerJobState)
				if err2 != nil {
					tctx.L().Error("fail to collect binlog replication job event", log.ShortError(err2))
				}
			}
		}
		return err
	}

	var err error
	for {
		select {
		case sqlJob, ok := <-jobChan:
			queueSizeGauge.WithLabelValues(s.cfg.Name, queueBucket).Set(float64(len(jobChan)))
			if !ok {
				return
			}
			idx++

			if sqlJob.tp != flush && len(sqlJob.sql) > 0 {
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

		case <-time.After(waitTime):
			if len(jobs) > 0 {
				err = executeSQLs()
				if err != nil {
					fatalF(err, pb.ErrorType_ExecSQL)
					continue
				}
				clearF()
			}
		}
	}
}

// redirectStreamer redirects binlog stream to given position
func (s *Syncer) redirectStreamer(pos mysql.Position) error {
	var err error
	s.tctx.L().Info("reset global streamer", zap.Stringer("position", pos))
	s.resetReplicationSyncer()
	s.streamer, err = s.streamerProducer.generateStreamer(pos)
	return err
}

// Run starts running for sync, we should guarantee it can rerun when paused.
func (s *Syncer) Run(ctx context.Context) (err error) {
	tctx := s.tctx.WithContext(ctx)

	defer func() {
		if s.done != nil {
			close(s.done)
		}
	}()

	parser2, err := s.fromDB.getParser(s.cfg.EnableANSIQuotes)
	if err != nil {
		return err
	}

	fresh, err := s.IsFreshTask(ctx)
	if err != nil {
		return err
	} else if fresh {
		// for fresh task, we try to load checkpoints from meta (file or config item)
		err = s.checkpoint.LoadMeta()
		if err != nil {
			return err
		}
	}

	// currentPos is the pos for current received event (End_log_pos in `show binlog events` for mysql)
	// lastPos is the pos for last received (ROTATE / QUERY / XID) event (End_log_pos in `show binlog events` for mysql)
	// we use currentPos to replace and skip binlog event of specified position and update table checkpoint in sharding ddl
	// we use lastPos to update global checkpoint and table checkpoint
	var (
		currentPos = s.checkpoint.GlobalPoint() // also init to global checkpoint
		lastPos    = s.checkpoint.GlobalPoint()
	)
	s.tctx.L().Info("replicate binlog from checkpoint", zap.Stringer("checkpoint", lastPos))

	s.streamer, err = s.streamerProducer.generateStreamer(lastPos)
	if err != nil {
		return err
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
		shardingReSyncCh    = make(chan *ShardingReSync, 10)
		shardingReSync      *ShardingReSync
		savedGlobalLastPos  mysql.Position
		latestOp            opType // latest job operation tp
		eventTimeoutCounter time.Duration
		traceSource         = fmt.Sprintf("%s.syncer.%s", s.cfg.SourceID, s.cfg.Name)
		traceID             string
	)

	closeShardingResync := func() error {
		if shardingReSync == nil {
			return nil
		}

		// if remaining DDLs in sequence, redirect global stream to the next sharding DDL position
		if !shardingReSync.allResolved {
			nextPos, err2 := s.sgk.ActiveDDLFirstPos(shardingReSync.targetSchema, shardingReSync.targetTable)
			if err2 != nil {
				return err2
			}

			err2 = s.redirectStreamer(nextPos)
			if err2 != nil {
				return err2
			}
		}
		shardingReSync = nil
		lastPos = savedGlobalLastPos // restore global last pos
		return nil
	}

	for {
		s.currentPosMu.Lock()
		s.currentPosMu.currentPos = currentPos
		s.currentPosMu.Unlock()

		// fetch from sharding resync channel if needed, and redirect global
		// stream to current binlog position recorded by ShardingReSync
		if shardingReSync == nil && len(shardingReSyncCh) > 0 {
			// some sharding groups need to re-syncing
			shardingReSync = <-shardingReSyncCh
			savedGlobalLastPos = lastPos // save global last pos
			lastPos = shardingReSync.currPos

			err = s.redirectStreamer(shardingReSync.currPos)
			if err != nil {
				return err
			}

			failpoint.Inject("ReSyncExit", func() {
				s.tctx.L().Warn("exit triggered", zap.String("failpoint", "ReSyncExit"))
				utils.OsExit(1)
			})
		}

		var e *replication.BinlogEvent

		// we only inject sqls  in global streaming to avoid DDL position confusion
		if shardingReSync == nil {
			e = s.tryInject(latestOp, currentPos)
			latestOp = null
		}

		startTime := time.Now()
		if e == nil {
			failpoint.Inject("SyncerEventTimeout", func(val failpoint.Value) {
				if seconds, ok := val.(int); ok {
					eventTimeout = time.Duration(seconds) * time.Second
					s.tctx.L().Info("set fetch binlog event timeout", zap.String("failpoint", "SyncerEventTimeout"), zap.Int("value", seconds))
				}
			})
			ctx2, cancel := context.WithTimeout(ctx, eventTimeout)
			e, err = s.streamer.GetEvent(ctx2)
			cancel()
		}

		if err == context.Canceled {
			s.tctx.L().Info("binlog replication main routine quit(context canceled)!", zap.Stringer("last position", lastPos))
			return nil
		} else if err == context.DeadlineExceeded {
			s.tctx.L().Info("deadline exceeded when fetching binlog event")
			eventTimeoutCounter += eventTimeout
			if eventTimeoutCounter < maxEventTimeout {
				err = s.flushJobs()
				if err != nil {
					return err
				}
				continue
			}

			eventTimeoutCounter = 0
			if s.needResync() {
				s.tctx.L().Info("timeout when fetching binlog event, there must be some problems with replica connection, try to re-connect")
				err = s.reopenWithRetry(s.syncCfg)
				if err != nil {
					return err
				}
			}
			continue
		}

		if err != nil {
			s.tctx.L().Error("fail to fetch binlog", log.ShortError(err))
			// try to re-sync in gtid mode
			if tryReSync && s.cfg.EnableGTID && isBinlogPurgedError(err) && s.cfg.AutoFixGTID {
				time.Sleep(retryTimeout)
				err = s.reSyncBinlog(s.syncCfg)
				if err != nil {
					return err
				}
				tryReSync = false
				continue
			}

			return err
		}

		// time duration for reading an event from relay log or upstream master.
		binlogReadDurationHistogram.WithLabelValues(s.cfg.Name).Observe(time.Since(startTime).Seconds())
		startTime = time.Now() // reset start time for the next metric.

		// get binlog event, reset tryReSync, so we can re-sync binlog while syncer meets errors next time
		tryReSync = true
		binlogPosGauge.WithLabelValues("syncer", s.cfg.Name).Set(float64(e.Header.LogPos))
		index, err := binlog.GetFilenameIndex(lastPos.Name)
		if err != nil {
			s.tctx.L().Error("fail to get index number of binlog file", log.ShortError(err))
		} else {
			binlogFileGauge.WithLabelValues("syncer", s.cfg.Name).Set(float64(index))
		}
		s.binlogSizeCount.Add(int64(e.Header.EventSize))
		binlogEventSizeHistogram.WithLabelValues(s.cfg.Name).Observe(float64(e.Header.EventSize))

		failpoint.Inject("ProcessBinlogSlowDown", nil)

		s.tctx.L().Debug("receive binlog event", zap.Reflect("header", e.Header))
		ec := eventContext{
			tctx:                tctx,
			header:              e.Header,
			currentPos:          &currentPos,
			lastPos:             &lastPos,
			shardingReSync:      shardingReSync,
			latestOp:            &latestOp,
			closeShardingResync: closeShardingResync,
			traceSource:         traceSource,
			safeMode:            safeMode,
			tryReSync:           tryReSync,
			startTime:           startTime,
			traceID:             &traceID,
			parser2:             parser2,
			shardingReSyncCh:    &shardingReSyncCh,
		}
		switch ev := e.Event.(type) {
		case *replication.RotateEvent:
			err = s.handleRotateEvent(ev, ec)
			if err != nil {
				return terror.Annotatef(err, "current pos %s", currentPos)
			}
		case *replication.RowsEvent:
			err = s.handleRowsEvent(ev, ec)
			if err != nil {
				return terror.Annotatef(err, "current pos %s", currentPos)
			}

		case *replication.QueryEvent:
			err = s.handleQueryEvent(ev, ec)
			if err != nil {
				return terror.Annotatef(err, "current pos %s", currentPos)
			}

		case *replication.XIDEvent:
			if shardingReSync != nil {
				shardingReSync.currPos.Pos = e.Header.LogPos
				lastPos = shardingReSync.currPos
				if shardingReSync.currPos.Compare(shardingReSync.latestPos) >= 0 {
					s.tctx.L().Info("re-replicate shard group was completed", zap.String("event", "XID"), zap.Reflect("re-shard", shardingReSync))
					err = closeShardingResync()
					if err != nil {
						return terror.Annotatef(err, "shard group current pos %s", shardingReSync.currPos)
					}
					continue
				}
			}

			latestOp = xid
			currentPos.Pos = e.Header.LogPos
			s.tctx.L().Debug("", zap.String("event", "XID"), zap.Stringer("last position", lastPos), log.WrapStringerField("position", currentPos), log.WrapStringerField("gtid set", ev.GSet))
			lastPos.Pos = e.Header.LogPos // update lastPos

			job := newXIDJob(currentPos, currentPos, nil, traceID)
			err = s.addJobFunc(job)
			if err != nil {
				return terror.Annotatef(err, "current pos %s", currentPos)
			}
		}
	}
}

type eventContext struct {
	tctx                *tcontext.Context
	header              *replication.EventHeader
	currentPos          *mysql.Position
	lastPos             *mysql.Position
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
	*ec.currentPos = mysql.Position{
		Name: string(ev.NextLogName),
		Pos:  uint32(ev.Position),
	}
	if ec.currentPos.Name > ec.lastPos.Name {
		*ec.lastPos = *ec.currentPos
	}

	if ec.shardingReSync != nil {
		if ec.currentPos.Compare(ec.shardingReSync.currPos) == 1 {
			ec.shardingReSync.currPos = *ec.currentPos
		}

		if ec.shardingReSync.currPos.Compare(ec.shardingReSync.latestPos) >= 0 {
			s.tctx.L().Info("re-replicate shard group was completed", zap.String("event", "rotate"), zap.Reflect("re-shard", ec.shardingReSync))
			err := ec.closeShardingResync()
			if err != nil {
				return err
			}
		} else {
			s.tctx.L().Debug("re-replicate shard group", zap.String("event", "rotate"), log.WrapStringerField("position", ec.currentPos), zap.Reflect("re-shard", ec.shardingReSync))
		}
		return nil
	}
	*ec.latestOp = rotate

	if s.tracer.Enable() {
		// Cannot convert this into a common method like `ec.CollectSyncerBinlogEvent()`
		// since CollectSyncerBinlogEvent relies on a fixed stack trace level
		// (must track 3 callers up).
		_, err := s.tracer.CollectSyncerBinlogEvent(ec.traceSource, ec.safeMode.Enable(), ec.tryReSync, *ec.lastPos, *ec.currentPos, int32(ec.header.EventType), int32(*ec.latestOp))
		if err != nil {
			s.tctx.L().Error("fail to collect binlog replication job event", zap.String("event", "rotate"), log.ShortError(err))
		}
	}

	s.tctx.L().Info("", zap.String("event", "rotate"), log.WrapStringerField("position", ec.currentPos))
	return nil
}

func (s *Syncer) handleRowsEvent(ev *replication.RowsEvent, ec eventContext) error {
	originSchema, originTable := string(ev.Table.Schema), string(ev.Table.Table)
	schemaName, tableName := s.renameShardingSchema(originSchema, originTable)
	*ec.currentPos = mysql.Position{
		Name: ec.lastPos.Name,
		Pos:  ec.header.LogPos,
	}

	if ec.shardingReSync != nil {
		ec.shardingReSync.currPos.Pos = ec.header.LogPos
		if ec.shardingReSync.currPos.Compare(ec.shardingReSync.latestPos) >= 0 {
			s.tctx.L().Info("re-replicate shard group was completed", zap.String("event", "row"), zap.Reflect("re-shard", ec.shardingReSync))
			return ec.closeShardingResync()
		}
		if ec.shardingReSync.targetSchema != schemaName || ec.shardingReSync.targetTable != tableName {
			// in re-syncing, ignore non current sharding group's events
			s.tctx.L().Debug("skip event in re-replicating shard group", zap.String("event", "row"), zap.Reflect("re-shard", ec.shardingReSync))
			return nil
		}
	}

	// DML position before table checkpoint, ignore it
	if !s.checkpoint.IsNewerTablePoint(originSchema, originTable, *ec.currentPos) {
		s.tctx.L().Debug("ignore obsolete event that is old than table checkpoint", zap.String("event", "row"), log.WrapStringerField("position", ec.currentPos), zap.String("origin schema", originSchema), zap.String("origin table", originTable))
		return nil
	}

	s.tctx.L().Debug("", zap.String("event", "row"), zap.String("origin schema", originSchema), zap.String("origin table", originTable), zap.String("target schema", schemaName), zap.String("target table", tableName), log.WrapStringerField("position", ec.currentPos), zap.Reflect("raw event data", ev.Rows))

	if s.cfg.EnableHeartbeat {
		s.heartbeat.TryUpdateTaskTs(s.cfg.Name, originSchema, originTable, ev.Rows)
	}

	ignore, err := s.skipDMLEvent(originSchema, originTable, ec.header.EventType)
	if err != nil {
		return err
	}
	if ignore {
		binlogSkippedEventsTotal.WithLabelValues("rows", s.cfg.Name).Inc()
		// for RowsEvent, we should record lastPos rather than currentPos
		return s.recordSkipSQLsPos(*ec.lastPos, nil)
	}

	if s.cfg.IsSharding {
		source, _ := GenTableID(originSchema, originTable)
		if s.sgk.InSyncing(schemaName, tableName, source, *ec.currentPos) {
			// if in unsync stage and not before active DDL, ignore it
			// if in sharding re-sync stage and not before active DDL (the next DDL to be synced), ignore it
			s.tctx.L().Debug("replicate sharding DDL, ignore Rows event", zap.String("event", "row"), zap.String("source", source), log.WrapStringerField("position", ec.currentPos))
			return nil
		}
	}

	table, columns, err := s.getTable(schemaName, tableName)
	if err != nil {
		return terror.WithScope(err, terror.ScopeDownstream)
	}
	rows, err := s.mappingDML(originSchema, originTable, columns, ev.Rows)
	if err != nil {
		return err
	}
	prunedColumns, prunedRows, err := pruneGeneratedColumnDML(table.columns, rows, schemaName, tableName, s.genColsCache)
	if err != nil {
		return err
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
	applied, sqls, err = s.tryApplySQLOperator(*ec.currentPos, nil) // forbidden sql-pattern for DMLs
	if err != nil {
		return err
	}
	param := &genDMLParam{
		schema:               table.schema,
		table:                table.name,
		data:                 prunedRows,
		originalData:         rows,
		columns:              prunedColumns,
		originalColumns:      table.columns,
		originalIndexColumns: table.indexColumns,
	}

	switch ec.header.EventType {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		if !applied {
			param.safeMode = ec.safeMode.Enable()
			sqls, keys, args, err = genInsertSQLs(param)
			if err != nil {
				return terror.Annotatef(err, "gen insert sqls failed, schema: %s, table: %s", table.schema, table.name)
			}
		}
		binlogEvent.WithLabelValues("write_rows", s.cfg.Name).Observe(time.Since(ec.startTime).Seconds())
		*ec.latestOp = insert

	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		if !applied {
			param.safeMode = ec.safeMode.Enable()
			sqls, keys, args, err = genUpdateSQLs(param)
			if err != nil {
				return terror.Annotatef(err, "gen update sqls failed, schema: %s, table: %s", table.schema, table.name)
			}
		}
		binlogEvent.WithLabelValues("update_rows", s.cfg.Name).Observe(time.Since(ec.startTime).Seconds())
		*ec.latestOp = update

	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		if !applied {
			sqls, keys, args, err = genDeleteSQLs(param)
			if err != nil {
				return terror.Annotatef(err, "gen delete sqls failed, schema: %s, table: %s", table.schema, table.name)
			}
		}
		binlogEvent.WithLabelValues("delete_rows", s.cfg.Name).Observe(time.Since(ec.startTime).Seconds())
		*ec.latestOp = del

	default:
		s.tctx.L().Debug("ignoring unrecognized event", zap.String("event", "row"), zap.Stringer("type", ec.header.EventType))
		return nil
	}

	if s.tracer.Enable() {
		traceEvent, traceErr := s.tracer.CollectSyncerBinlogEvent(ec.traceSource, ec.safeMode.Enable(), ec.tryReSync, *ec.lastPos, *ec.currentPos, int32(ec.header.EventType), int32(*ec.latestOp))
		if traceErr != nil {
			s.tctx.L().Error("fail to collect binlog replication job event", zap.String("event", "row"), log.ShortError(traceErr))
		}
		*ec.traceID = traceEvent.Base.TraceID
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
		err = s.commitJob(*ec.latestOp, originSchema, originTable, table.schema, table.name, sqls[i], arg, key, true, *ec.lastPos, *ec.currentPos, nil, *ec.traceID)
		if err != nil {
			return err
		}
	}
	dispatchBinlogDurationHistogram.WithLabelValues(ec.latestOp.String(), s.cfg.Name).Observe(time.Since(startTime).Seconds())
	return nil
}

func (s *Syncer) handleQueryEvent(ev *replication.QueryEvent, ec eventContext) error {
	*ec.currentPos = mysql.Position{
		Name: ec.lastPos.Name,
		Pos:  ec.header.LogPos,
	}
	sql := strings.TrimSpace(string(ev.Query))
	usedSchema := string(ev.Schema)
	parseResult, err := s.parseDDLSQL(sql, ec.parser2, usedSchema)
	if err != nil {
		s.tctx.L().Error("fail to parse statement", zap.String("event", "query"), zap.String("statement", sql), zap.String("schema", usedSchema), zap.Stringer("last position", ec.lastPos), log.WrapStringerField("position", ec.currentPos), log.WrapStringerField("gtid set", ev.GSet), log.ShortError(err))
		return err
	}

	if parseResult.ignore {
		binlogSkippedEventsTotal.WithLabelValues("query", s.cfg.Name).Inc()
		s.tctx.L().Warn("skip event", zap.String("event", "query"), zap.String("statement", sql), zap.String("schema", usedSchema))
		*ec.lastPos = *ec.currentPos // before record skip pos, update lastPos
		return s.recordSkipSQLsPos(*ec.lastPos, nil)
	}
	if !parseResult.isDDL {
		// skipped sql maybe not a DDL (like `BEGIN`)
		return nil
	}

	if ec.shardingReSync != nil {
		ec.shardingReSync.currPos.Pos = ec.header.LogPos
		if ec.shardingReSync.currPos.Compare(ec.shardingReSync.latestPos) >= 0 {
			s.tctx.L().Info("re-replicate shard group was completed", zap.String("event", "query"), zap.String("statement", sql), zap.Reflect("re-shard", ec.shardingReSync))
			err2 := ec.closeShardingResync()
			if err2 != nil {
				return err2
			}
		} else {
			// in re-syncing, we can simply skip all DDLs,
			// as they have been added to sharding DDL sequence
			// only update lastPos when the query is a real DDL
			*ec.lastPos = ec.shardingReSync.currPos
			s.tctx.L().Debug("skip event in re-replicating sharding group", zap.String("event", "query"), zap.String("statement", sql), zap.Reflect("re-shard", ec.shardingReSync))
		}
		return nil
	}

	s.tctx.L().Info("", zap.String("event", "query"), zap.String("statement", sql), zap.String("schema", usedSchema), zap.Stringer("last position", ec.lastPos), log.WrapStringerField("position", ec.currentPos), log.WrapStringerField("gtid set", ev.GSet))
	*ec.lastPos = *ec.currentPos // update lastPos, because we have checked `isDDL`
	*ec.latestOp = ddl

	var (
		sqls                []string
		onlineDDLTableNames map[string]*filter.Table
	)

	// for DDL, we don't apply operator until we try to execute it.
	// so can handle sharding cases
	sqls, onlineDDLTableNames, err = s.resolveDDLSQL(ec.tctx, ec.parser2, parseResult.stmt, usedSchema)
	if err != nil {
		s.tctx.L().Error("fail to resolve statement", zap.String("event", "query"), zap.String("statement", sql), zap.String("schema", usedSchema), zap.Stringer("last position", ec.lastPos), log.WrapStringerField("position", ec.currentPos), log.WrapStringerField("gtid set", ev.GSet), log.ShortError(err))
		return err
	}
	s.tctx.L().Info("resolve sql", zap.String("event", "query"), zap.String("raw statement", sql), zap.Strings("statements", sqls), zap.String("schema", usedSchema), zap.Stringer("last position", ec.lastPos), zap.Stringer("position", ec.currentPos), log.WrapStringerField("gtid set", ev.GSet))

	if len(onlineDDLTableNames) > 1 {
		return terror.ErrSyncerUnitOnlineDDLOnMultipleTable.Generate(string(ev.Query))
	}

	binlogEvent.WithLabelValues("query", s.cfg.Name).Observe(time.Since(ec.startTime).Seconds())

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
		sqlDDL, tableNames, stmt, handleErr := s.handleDDL(ec.parser2, usedSchema, sql)
		if handleErr != nil {
			return handleErr
		}
		if len(sqlDDL) == 0 {
			binlogSkippedEventsTotal.WithLabelValues("query", s.cfg.Name).Inc()
			s.tctx.L().Warn("skip event", zap.String("event", "query"), zap.String("statement", sql), zap.String("schema", usedSchema))
			continue
		}

		// for DDL, we wait it to be executed, so we can check if event is newer in this syncer's main process goroutine
		// ignore obsolete DDL here can avoid to try-sync again for already synced DDLs
		if !s.checkpoint.IsNewerTablePoint(tableNames[0][0].Schema, tableNames[0][0].Name, *ec.currentPos) {
			s.tctx.L().Info("ignore obsolete DDL", zap.String("event", "query"), zap.String("statement", sql), log.WrapStringerField("position", ec.currentPos))
			continue
		}

		if s.cfg.IsSharding {
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
		}

		needHandleDDLs = append(needHandleDDLs, sqlDDL)
		targetTbls[tableNames[1][0].String()] = tableNames[1][0]
	}

	s.tctx.L().Info("prepare to handle ddls", zap.String("event", "query"), zap.Strings("ddls", needHandleDDLs), zap.ByteString("raw statement", ev.Query), log.WrapStringerField("position", ec.currentPos))
	if len(needHandleDDLs) == 0 {
		s.tctx.L().Info("skip event, need handled ddls is empty", zap.String("event", "query"), zap.ByteString("raw statement", ev.Query), log.WrapStringerField("position", ec.currentPos))
		return s.recordSkipSQLsPos(*ec.lastPos, nil)
	}

	if s.tracer.Enable() {
		traceEvent, traceErr := s.tracer.CollectSyncerBinlogEvent(ec.traceSource, ec.safeMode.Enable(), ec.tryReSync, *ec.lastPos, *ec.currentPos, int32(ec.header.EventType), int32(*ec.latestOp))
		if traceErr != nil {
			s.tctx.L().Error("fail to collect binlog replication job event", zap.String("event", "query"), log.ShortError(traceErr))
		}
		*ec.traceID = traceEvent.Base.TraceID
	}

	if !s.cfg.IsSharding {
		s.tctx.L().Info("start to handle ddls in normal mode", zap.String("event", "query"), zap.Strings("ddls", needHandleDDLs), zap.ByteString("raw statement", ev.Query), log.WrapStringerField("position", ec.currentPos))
		// try apply SQL operator before addJob. now, one query event only has one DDL job, if updating to multi DDL jobs, refine this.
		applied, appliedSQLs, applyErr := s.tryApplySQLOperator(*ec.currentPos, needHandleDDLs)
		if applyErr != nil {
			return terror.Annotatef(applyErr, "try apply SQL operator on binlog-pos %s with DDLs %v", ec.currentPos, needHandleDDLs)
		}
		if applied {
			s.tctx.L().Info("replace ddls to preset ddls by sql operator in normal mode", zap.String("event", "query"), zap.Strings("preset ddls", appliedSQLs), zap.Strings("ddls", needHandleDDLs), zap.ByteString("raw statement", ev.Query), log.WrapStringerField("position", ec.currentPos))
			needHandleDDLs = appliedSQLs // maybe nil
		}

		job := newDDLJob(nil, needHandleDDLs, *ec.lastPos, *ec.currentPos, nil, nil, *ec.traceID)
		err = s.addJobFunc(job)
		if err != nil {
			return err
		}

		// when add ddl job, will execute ddl and then flush checkpoint.
		// if execute ddl failed, the execErrorDetected will be true.
		if s.execErrorDetected.Get() {
			return terror.ErrSyncerUnitHandleDDLFailed.Generate(ev.Query)
		}

		s.tctx.L().Info("finish to handle ddls in normal mode", zap.String("event", "query"), zap.Strings("ddls", needHandleDDLs), zap.ByteString("raw statement", ev.Query), log.WrapStringerField("position", ec.currentPos))

		for _, tbl := range targetTbls {
			s.clearTables(tbl.Schema, tbl.Name)
			// save checkpoint of each table
			s.checkpoint.SaveTablePoint(tbl.Schema, tbl.Name, *ec.currentPos)
		}

		for _, table := range onlineDDLTableNames {
			s.tctx.L().Info("finish online ddl and clear online ddl metadata in normal mode", zap.String("event", "query"), zap.Strings("ddls", needHandleDDLs), zap.ByteString("raw statement", ev.Query), zap.String("schema", table.Schema), zap.String("table", table.Name))
			err = s.onlineDDL.Finish(ec.tctx, table.Schema, table.Name)
			if err != nil {
				return terror.Annotatef(err, "finish online ddl on %s.%s", table.Schema, table.Name)
			}
		}

		return nil
	}

	// handle sharding ddl
	var (
		needShardingHandle bool
		group              *ShardingGroup
		synced             bool
		active             bool
		remain             int
		source             string
		ddlExecItem        *DDLExecItem
	)
	// for sharding DDL, the firstPos should be the `Pos` of the binlog, not the `End_log_pos`
	// so when restarting before sharding DDLs synced, this binlog can be re-sync again to trigger the TrySync
	startPos := mysql.Position{
		Name: ec.currentPos.Name,
		Pos:  ec.currentPos.Pos - ec.header.EventSize,
	}
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
		needShardingHandle, group, synced, active, remain, err = s.sgk.TrySync(ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name, source, startPos, *ec.currentPos, needHandleDDLs)
		if err != nil {
			return err
		}
		annotate = "try to sync table in shard group"
		// meets DDL that will not be processed in sequence sharding
		if !active {
			s.tctx.L().Info("skip in-activeDDL", zap.String("event", "query"), zap.String("source", source), zap.Strings("ddls", needHandleDDLs), zap.ByteString("raw statement", ev.Query), zap.Bool("in-sharding", needShardingHandle), zap.Stringer("start position", startPos), zap.Bool("is-synced", synced), zap.Int("unsynced", remain))
			return nil
		}
	}

	s.tctx.L().Info(annotate, zap.String("event", "query"), zap.String("source", source), zap.Strings("ddls", needHandleDDLs), zap.ByteString("raw statement", ev.Query), zap.Bool("in-sharding", needShardingHandle), zap.Stringer("start position", startPos), zap.Bool("is-synced", synced), zap.Int("unsynced", remain))

	if needShardingHandle {
		target, _ := GenTableID(ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name)
		unsyncedTableGauge.WithLabelValues(s.cfg.Name, target).Set(float64(remain))
		err = ec.safeMode.IncrForTable(s.tctx, ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name) // try enable safe-mode when starting syncing for sharding group
		if err != nil {
			return err
		}

		// save checkpoint in memory, don't worry, if error occurred, we can rollback it
		// for non-last sharding DDL's table, this checkpoint will be used to skip binlog event when re-syncing
		// NOTE: when last sharding DDL executed, all this checkpoints will be flushed in the same txn
		s.tctx.L().Info("save table checkpoint for source", zap.String("event", "query"), zap.String("source", source), zap.Stringer("start position", startPos), log.WrapStringerField("end position", ec.currentPos))
		s.checkpoint.SaveTablePoint(ddlInfo.tableNames[0][0].Schema, ddlInfo.tableNames[0][0].Name, *ec.currentPos)
		if !synced {
			s.tctx.L().Info("source shard group is not synced", zap.String("event", "query"), zap.String("source", source), zap.Stringer("start position", startPos), log.WrapStringerField("end position", ec.currentPos))
			return nil
		}

		s.tctx.L().Info("source shard group is synced", zap.String("event", "query"), zap.String("source", source), zap.Stringer("start position", startPos), log.WrapStringerField("end position", ec.currentPos))
		err = ec.safeMode.DescForTable(s.tctx, ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name) // try disable safe-mode after sharding group synced
		if err != nil {
			return err
		}
		// maybe multi-groups' sharding DDL synced in this for-loop (one query-event, multi tables)
		if cap(*ec.shardingReSyncCh) < len(sqls) {
			*ec.shardingReSyncCh = make(chan *ShardingReSync, len(sqls))
		}
		firstEndPos := group.FirstEndPosUnresolved()
		if firstEndPos == nil {
			return terror.ErrSyncerUnitFirstEndPosNotFound.Generate(source)
		}

		allResolved, err2 := s.sgk.ResolveShardingDDL(ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name)
		if err2 != nil {
			return err2
		}
		*ec.shardingReSyncCh <- &ShardingReSync{
			currPos:      *firstEndPos,
			latestPos:    *ec.currentPos,
			targetSchema: ddlInfo.tableNames[1][0].Schema,
			targetTable:  ddlInfo.tableNames[1][0].Name,
			allResolved:  allResolved,
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
		shardLockResolving.WithLabelValues(s.cfg.Name).Set(1)
		for {
			var ok bool
			ddlExecItem, ok = <-s.ddlExecInfo.Chan(needHandleDDLs)
			if !ok {
				// chan closed
				shardLockResolving.WithLabelValues(s.cfg.Name).Set(0)
				s.tctx.L().Warn("canceled from external", zap.String("event", "query"), zap.String("source", source), zap.Strings("ddls", needHandleDDLs), zap.ByteString("raw statement", ev.Query), zap.Stringer("start position", startPos), log.WrapStringerField("end position", ec.currentPos))
				return nil
			} else if len(ddlExecItem.req.DDLs) != 0 && !reflect.DeepEqual(ddlExecItem.req.DDLs, needHandleDDLs) {
				// ignore un-cleared cached/duplicate DDL execute request
				// check `len(ddlExecItem.req.DDLs) != 0` to support old DM-master and `break-ddl-lock`
				s.tctx.L().Warn("ignore mismatched DDL execute request", zap.String("source", source), zap.Strings("expect", needHandleDDLs), zap.Strings("request", ddlExecItem.req.DDLs))
				continue
			}
			shardLockResolving.WithLabelValues(s.cfg.Name).Set(0)
			break
		}

		if ddlExecItem.req.Exec {
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

			s.tctx.L().Info("execute DDL job", zap.String("event", "query"), zap.String("source", source), zap.Strings("ddls", ddlInfo1.DDLs), zap.ByteString("raw statement", ev.Query), zap.Stringer("start position", startPos), log.WrapStringerField("end position", ec.currentPos), zap.Reflect("request", ddlExecItem.req))
		} else {
			s.tctx.L().Info("ignore DDL job", zap.String("event", "query"), zap.String("source", source), zap.Strings("ddls", ddlInfo1.DDLs), zap.ByteString("raw statement", ev.Query), zap.Stringer("start position", startPos), log.WrapStringerField("end position", ec.currentPos), zap.Reflect("request", ddlExecItem.req))
		}
	}

	s.tctx.L().Info("start to handle ddls in shard mode", zap.String("event", "query"), zap.Strings("ddls", needHandleDDLs), zap.ByteString("raw statement", ev.Query), zap.Stringer("start position", startPos), log.WrapStringerField("end position", ec.currentPos))

	// try apply SQL operator before addJob. now, one query event only has one DDL job, if updating to multi DDL jobs, refine this.
	applied, appliedSQLs, err := s.tryApplySQLOperator(*ec.currentPos, needHandleDDLs)
	if err != nil {
		return terror.Annotatef(err, "try apply SQL operator on binlog-pos %s with DDLs %v", ec.currentPos, needHandleDDLs)
	}
	if applied {
		s.tctx.L().Info("replace ddls to preset ddls by sql operator in shard mode", zap.String("event", "query"), zap.Strings("preset ddls", appliedSQLs), zap.Strings("ddls", needHandleDDLs), zap.ByteString("raw statement", ev.Query), zap.Stringer("start position", startPos), log.WrapStringerField("end position", ec.currentPos))
		needHandleDDLs = appliedSQLs // maybe nil
	}
	job := newDDLJob(ddlInfo, needHandleDDLs, *ec.lastPos, *ec.currentPos, nil, ddlExecItem, *ec.traceID)
	err = s.addJobFunc(job)
	if err != nil {
		return err
	}

	if s.execErrorDetected.Get() {
		return terror.ErrSyncerUnitHandleDDLFailed.Generate(ev.Query)
	}

	if len(onlineDDLTableNames) > 0 {
		err = s.clearOnlineDDL(ec.tctx, ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name)
		if err != nil {
			return err
		}
	}

	s.tctx.L().Info("finish to handle ddls in shard mode", zap.String("event", "query"), zap.Strings("ddls", needHandleDDLs), zap.ByteString("raw statement", ev.Query), zap.Stringer("start position", startPos), log.WrapStringerField("end position", ec.currentPos))

	s.clearTables(ddlInfo.tableNames[1][0].Schema, ddlInfo.tableNames[1][0].Name)
	return nil
}

func (s *Syncer) commitJob(tp opType, sourceSchema, sourceTable, targetSchema, targetTable, sql string, args []interface{}, keys []string, retry bool, pos, cmdPos mysql.Position, gs gtid.Set, traceID string) error {
	startTime := time.Now()
	key, err := s.resolveCasuality(keys)
	if err != nil {
		return terror.ErrSyncerUnitResolveCasualityFail.Generate(err)
	}
	conflictDetectDurationHistogram.WithLabelValues(s.cfg.Name).Observe(time.Since(startTime).Seconds())

	job := newJob(tp, sourceSchema, sourceTable, targetSchema, targetTable, sql, args, key, pos, cmdPos, gs, traceID)
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

				s.currentPosMu.RLock()
				currentPos := s.currentPosMu.currentPos
				s.currentPosMu.RUnlock()

				remainingSize, err2 := s.fromDB.countBinaryLogsSize(currentPos)
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
						remainingTimeGauge.WithLabelValues(s.cfg.Name).Set(float64(remainingSeconds))
					}
				}
			}

			latestMasterPos, latestmasterGTIDSet, err = s.getMasterStatus()
			if err != nil {
				s.tctx.L().Error("fail to get master status", log.ShortError(err))
			} else {
				binlogPosGauge.WithLabelValues("master", s.cfg.Name).Set(float64(latestMasterPos.Pos))
				index, err := binlog.GetFilenameIndex(latestMasterPos.Name)
				if err != nil {
					s.tctx.L().Error("fail to parse binlog file", log.ShortError(err))
				} else {
					binlogFileGauge.WithLabelValues("master", s.cfg.Name).Set(float64(index))
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
func (s *Syncer) recordSkipSQLsPos(pos mysql.Position, gtidSet gtid.Set) error {
	job := newSkipJob(pos, gtidSet)
	return s.addJobFunc(job)
}

func (s *Syncer) flushJobs() error {
	s.tctx.L().Info("flush all jobs", zap.Stringer("global checkpoint", s.checkpoint))
	job := newFlushJob()
	return s.addJobFunc(job)
}

func (s *Syncer) reSyncBinlog(cfg replication.BinlogSyncerConfig) error {
	err := s.retrySyncGTIDs()
	if err != nil {
		return err
	}
	// close still running sync
	return s.reopenWithRetry(cfg)
}

func (s *Syncer) reopenWithRetry(cfg replication.BinlogSyncerConfig) error {
	var err error
	for i := 0; i < maxRetryCount; i++ {
		s.streamer, err = s.reopen(cfg)
		if err == nil {
			return nil
		}
		if needRetryReplicate(err) {
			s.tctx.L().Info("fail to retry open binlog streamer", log.ShortError(err))
			time.Sleep(retryTimeout)
			continue
		}
		break
	}
	return err
}

func (s *Syncer) reopen(cfg replication.BinlogSyncerConfig) (streamer.Streamer, error) {
	if s.streamerProducer != nil {
		switch r := s.streamerProducer.(type) {
		case *remoteBinlogReader:
			err := s.closeBinlogSyncer(r.reader)
			if err != nil {
				return nil, err
			}
		default:
			return nil, terror.ErrSyncerUnitReopenStreamNotSupport.Generate(r)
		}
	}
	// TODO: refactor to support relay
	s.streamerProducer = &remoteBinlogReader{replication.NewBinlogSyncer(cfg), s.tctx, s.cfg.EnableGTID}
	return s.streamerProducer.generateStreamer(s.checkpoint.GlobalPoint())
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

	if s.ddlInfoCh != nil {
		close(s.ddlInfoCh)
		s.ddlInfoCh = nil
	}

	s.closeDBs()

	s.checkpoint.Close()

	s.closeOnlineDDL()

	// when closing syncer by `stop-task`, remove active relay log from hub
	s.removeActiveRelayLog()

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

	if s.streamerProducer != nil {
		switch r := s.streamerProducer.(type) {
		case *remoteBinlogReader:
			// process remote binlog reader
			s.closeBinlogSyncer(r.reader)
			s.streamerProducer = nil
		case *localBinlogReader:
			// process local binlog reader
			r.reader.Close()
		}
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

func (s *Syncer) closeBinlogSyncer(syncer *replication.BinlogSyncer) error {
	if syncer == nil {
		return nil
	}

	lastSlaveConnectionID := syncer.LastConnectionID()
	defer syncer.Close()
	if lastSlaveConnectionID > 0 {
		err := s.fromDB.killConn(lastSlaveConnectionID)
		if err != nil {
			s.tctx.L().Error("fail to kill last connection", zap.Uint32("connection ID", lastSlaveConnectionID), log.ShortError(err))
			if !utils.IsNoSuchThreadError(err) {
				return err
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
				unit.NewProcessError(pb.ErrorType_UnknownError, err),
			},
		}
		return
	}
	s.Process(ctx, pr)
}

// Update implements Unit.Update
// now, only support to update config for routes, filters, column-mappings, black-white-list
// now no config diff implemented, so simply re-init use new config
func (s *Syncer) Update(cfg *config.SubTaskConfig) error {
	if s.cfg.IsSharding {
		_, tables := s.sgk.UnresolvedTables()
		if len(tables) > 0 {
			return terror.ErrSyncerUnitUpdateConfigInSharding.Generate(tables)
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
	s.bwList, err = filter.New(cfg.CaseSensitive, cfg.BWList)
	if err != nil {
		return terror.ErrSyncerUnitGenBWList.Delegate(err)
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
		return terror.ErrSyncerUnitNewBinlogEventFilter.Delegate(err)
	}

	// update column-mappings
	oldColumnMapping = s.columnMapping
	s.columnMapping, err = cm.NewMapping(cfg.CaseSensitive, cfg.ColumnMappingRules)
	if err != nil {
		return terror.ErrSyncerUnitNewColumnMapping.Delegate(err)
	}

	if s.cfg.IsSharding {
		// re-init sharding group
		err = s.sgk.Init()
		if err != nil {
			return err
		}

		err = s.initShardingGroups()
		if err != nil {
			return err
		}
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
		s.tctx.L().Error("fail to get master status", log.ShortError(err))
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

// DDLInfo returns a chan from which can receive DDLInfo
func (s *Syncer) DDLInfo() <-chan *pb.DDLInfo {
	s.RLock()
	defer s.RUnlock()
	return s.ddlInfoCh
}

// ExecuteDDL executes or skips a hanging-up DDL when in sharding
func (s *Syncer) ExecuteDDL(ctx context.Context, execReq *pb.ExecDDLRequest) (<-chan error, error) {
	if len(s.ddlExecInfo.BlockingDDLs()) == 0 {
		return nil, terror.ErrSyncerUnitExecWithNoBlockingDDL.Generate()
	}
	item := newDDLExecItem(execReq)
	err := s.ddlExecInfo.Send(ctx, item)
	if err != nil {
		return nil, err
	}
	return item.resp, nil
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
