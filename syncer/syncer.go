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
	"bytes"
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

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/errors"
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
	toolutils "github.com/pingcap/tidb-tools/pkg/utils"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/config"
	common2 "github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/unit"
	"github.com/pingcap/dm/pkg/binlog"
	"github.com/pingcap/dm/pkg/binlog/common"
	"github.com/pingcap/dm/pkg/binlog/event"
	"github.com/pingcap/dm/pkg/binlog/reader"
	"github.com/pingcap/dm/pkg/conn"
	tcontext "github.com/pingcap/dm/pkg/context"
	fr "github.com/pingcap/dm/pkg/func-rollback"
	"github.com/pingcap/dm/pkg/ha"
	"github.com/pingcap/dm/pkg/log"
	parserpkg "github.com/pingcap/dm/pkg/parser"
	"github.com/pingcap/dm/pkg/schema"
	"github.com/pingcap/dm/pkg/shardddl/pessimism"
	"github.com/pingcap/dm/pkg/streamer"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/pingcap/dm/syncer/dbconn"
	operator "github.com/pingcap/dm/syncer/err-operator"
	"github.com/pingcap/dm/syncer/metrics"
	onlineddl "github.com/pingcap/dm/syncer/online-ddl-tools"
	sm "github.com/pingcap/dm/syncer/safe-mode"
	"github.com/pingcap/dm/syncer/shardddl"
)

var (
	maxRetryCount = 100

	retryTimeout = 3 * time.Second
	waitTime     = 10 * time.Millisecond

	// MaxDDLConnectionTimeoutMinute also used by SubTask.ExecuteDDL.
	MaxDDLConnectionTimeoutMinute = 5

	maxDMLConnectionTimeout = "5m"
	maxDDLConnectionTimeout = fmt.Sprintf("%dm", MaxDDLConnectionTimeoutMinute)

	maxDMLConnectionDuration, _ = time.ParseDuration(maxDMLConnectionTimeout)
	maxDMLExecutionDuration     = 30 * time.Second

	maxPauseOrStopWaitTime = 10 * time.Second

	adminQueueName     = "admin queue"
	defaultBucketCount = 8
)

// BinlogType represents binlog sync type.
type BinlogType uint8

// binlog sync type.
const (
	RemoteBinlog BinlogType = iota + 1
	LocalBinlog

	skipJobIdx = iota
	ddlJobIdx
	workerJobTSArrayInitSize // size = skip + ddl
)

// waitXIDStatus represents the status for waiting XID event when pause/stop task.
type waitXIDStatus int64

const (
	noWait waitXIDStatus = iota
	waiting
	waitComplete
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
	cli       *clientv3.Client

	binlogType         BinlogType
	streamerController *StreamerController
	enableRelay        bool

	wg    sync.WaitGroup
	jobWg sync.WaitGroup

	schemaTracker *schema.Tracker

	fromDB *dbconn.UpStreamConn

	toDB      *conn.BaseDB
	toDBConns []*dbconn.DBConn
	ddlDB     *conn.BaseDB
	ddlDBConn *dbconn.DBConn

	jobs                []chan *job
	jobsClosed          atomic.Bool
	jobsChanLock        sync.Mutex
	queueBucketMapping  []string
	waitXIDJob          atomic.Int64
	isTransactionEnd    bool
	waitTransactionLock sync.Mutex

	c *causality

	tableRouter     *router.Table
	binlogFilter    *bf.BinlogEvent
	columnMapping   *cm.Mapping
	baList          *filter.Filter
	exprFilterGroup *ExprFilterGroup

	closed atomic.Bool

	start    atomic.Time
	lastTime atomic.Time

	// safeMode is used to track if we need to generate dml with safe-mode
	// For each binlog event, we will set the current value into eventContext because
	// the status of this track may change over time.
	safeMode *sm.SafeMode

	timezone *time.Location

	binlogSizeCount     atomic.Int64
	lastBinlogSizeCount atomic.Int64

	lastCount atomic.Int64
	count     atomic.Int64
	totalTps  atomic.Int64
	tps       atomic.Int64

	filteredInsert atomic.Int64
	filteredUpdate atomic.Int64
	filteredDelete atomic.Int64

	done chan struct{}

	checkpoint CheckPoint
	onlineDDL  onlineddl.OnlinePlugin

	// record process error rather than log.Fatal
	runFatalChan chan *pb.ProcessError
	// record whether error occurred when execute SQLs
	execError atomic.Error

	heartbeat *Heartbeat

	readerHub              *streamer.ReaderHub
	recordedActiveRelayLog bool

	errOperatorHolder *operator.Holder

	isReplacingErr bool // true if we are in replace events by handle-error

	currentLocationMu struct {
		sync.RWMutex
		currentLocation binlog.Location // use to calc remain binlog size
	}

	errLocation struct {
		sync.RWMutex
		startLocation *binlog.Location
		endLocation   *binlog.Location
		isQueryEvent  bool
	}

	addJobFunc func(*job) error

	// `lower_case_table_names` setting of upstream db
	SourceTableNamesFlavor utils.LowerCaseTableNamesFlavor

	tsOffset                  atomic.Int64    // time offset between upstream and syncer, DM's timestamp - MySQL's timestamp
	secondsBehindMaster       atomic.Int64    // current task delay second behind upstream
	workerJobTSArray          []*atomic.Int64 // worker's sync job TS array, note that idx=0 is skip idx and idx=1 is ddl idx,sql worker job idx=(queue id + 2)
	lastCheckpointFlushedTime time.Time
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
	syncer.jobsClosed.Store(true) // not open yet
	syncer.waitXIDJob.Store(int64(noWait))
	syncer.isTransactionEnd = true
	syncer.closed.Store(false)
	syncer.lastBinlogSizeCount.Store(0)
	syncer.binlogSizeCount.Store(0)
	syncer.lastCount.Store(0)
	syncer.count.Store(0)
	syncer.c = newCausality()
	syncer.done = nil
	syncer.setTimezone()
	syncer.addJobFunc = syncer.addJob
	syncer.enableRelay = cfg.UseRelay
	syncer.cli = etcdClient

	syncer.checkpoint = NewRemoteCheckPoint(syncer.tctx, cfg, syncer.checkpointID())

	syncer.binlogType = toBinlogType(cfg.UseRelay)
	syncer.errOperatorHolder = operator.NewHolder(&logger)
	syncer.readerHub = streamer.GetReaderHub()

	if cfg.ShardMode == config.ShardPessimistic {
		// only need to sync DDL in sharding mode
		syncer.sgk = NewShardingGroupKeeper(syncer.tctx, cfg)
	}
	syncer.recordedActiveRelayLog = false
	syncer.workerJobTSArray = make([]*atomic.Int64, cfg.WorkerCount+workerJobTSArrayInitSize)
	for i := range syncer.workerJobTSArray {
		syncer.workerJobTSArray[i] = atomic.NewInt64(0)
	}
	syncer.lastCheckpointFlushedTime = time.Time{}
	return syncer
}

func (s *Syncer) newJobChans(count int) {
	s.closeJobChans()
	s.jobs = make([]chan *job, 0, count)
	for i := 0; i < count; i++ {
		s.jobs = append(s.jobs, make(chan *job, s.cfg.QueueSize))
	}
	s.jobsClosed.Store(false)
}

func (s *Syncer) closeJobChans() {
	s.jobsChanLock.Lock()
	defer s.jobsChanLock.Unlock()
	if s.jobsClosed.Load() {
		return
	}
	for _, ch := range s.jobs {
		close(ch)
	}
	s.jobsClosed.Store(true)
}

// Type implements Unit.Type.
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

	err = s.setSyncCfg()
	if err != nil {
		return err
	}

	err = s.createDBs(ctx)
	if err != nil {
		return err
	}
	rollbackHolder.Add(fr.FuncRollback{Name: "close-DBs", Fn: s.closeDBs})

	s.schemaTracker, err = schema.NewTracker(ctx, s.cfg.Name, s.cfg.To.Session, s.ddlDBConn.BaseConn)
	if err != nil {
		return terror.ErrSchemaTrackerInit.Delegate(err)
	}

	s.streamerController = NewStreamerController(s.syncCfg, s.cfg.EnableGTID, s.fromDB, s.binlogType, s.cfg.RelayDir, s.timezone)

	s.baList, err = filter.New(s.cfg.CaseSensitive, s.cfg.BAList)
	if err != nil {
		return terror.ErrSyncerUnitGenBAList.Delegate(err)
	}

	s.binlogFilter, err = bf.NewBinlogEvent(s.cfg.CaseSensitive, s.cfg.FilterRules)
	if err != nil {
		return terror.ErrSyncerUnitGenBinlogEventFilter.Delegate(err)
	}

	s.exprFilterGroup = NewExprFilterGroup(s.cfg.ExprFilter)

	if len(s.cfg.ColumnMappingRules) > 0 {
		s.columnMapping, err = cm.NewMapping(s.cfg.CaseSensitive, s.cfg.ColumnMappingRules)
		if err != nil {
			return terror.ErrSyncerUnitGenColumnMapping.Delegate(err)
		}
	}

	if s.cfg.OnlineDDL {
		s.onlineDDL, err = onlineddl.NewRealOnlinePlugin(tctx, s.cfg)
		if err != nil {
			return err
		}
		rollbackHolder.Add(fr.FuncRollback{Name: "close-onlineDDL", Fn: s.closeOnlineDDL})
	}

	err = s.genRouter()
	if err != nil {
		return err
	}

	var schemaMap map[string]string
	var tableMap map[string]map[string]string
	if s.SourceTableNamesFlavor == utils.LCTableNamesSensitive {
		// TODO: we should avoid call this function multi times
		allTables, err1 := utils.FetchAllDoTables(ctx, s.fromDB.BaseDB.DB, s.baList)
		if err1 != nil {
			return err1
		}
		schemaMap, tableMap = buildLowerCaseTableNamesMap(allTables)
	}

	switch s.cfg.ShardMode {
	case config.ShardPessimistic:
		err = s.sgk.Init()
		if err != nil {
			return err
		}
		err = s.initShardingGroups(ctx, true)
		if err != nil {
			return err
		}
		rollbackHolder.Add(fr.FuncRollback{Name: "close-sharding-group-keeper", Fn: s.sgk.Close})
	case config.ShardOptimistic:
		if err = s.initOptimisticShardDDL(ctx); err != nil {
			return err
		}
	}

	err = s.checkpoint.Init(tctx)
	if err != nil {
		return err
	}
	rollbackHolder.Add(fr.FuncRollback{Name: "close-checkpoint", Fn: s.checkpoint.Close})

	err = s.checkpoint.Load(tctx)
	if err != nil {
		return err
	}
	if s.SourceTableNamesFlavor == utils.LCTableNamesSensitive {
		if err = s.checkpoint.CheckAndUpdate(ctx, schemaMap, tableMap); err != nil {
			return err
		}

		if s.onlineDDL != nil {
			if err = s.onlineDDL.CheckAndUpdate(s.tctx, schemaMap, tableMap); err != nil {
				return err
			}
		}
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
	err = s.setInitActiveRelayLog(ctx)
	if err != nil {
		return err
	}
	rollbackHolder.Add(fr.FuncRollback{Name: "remove-active-realylog", Fn: s.removeActiveRelayLog})

	s.reset()
	return nil
}

// buildLowerCaseTableNamesMap build a lower case schema map and lower case table map for all tables
// Input: map of schema --> list of tables
// Output: schema names map: lower_case_schema_name --> schema_name
//         tables names map: lower_case_schema_name --> lower_case_table_name --> table_name
// Note: the result will skip the schemas and tables that their lower_case_name are the same.
func buildLowerCaseTableNamesMap(tables map[string][]string) (map[string]string, map[string]map[string]string) {
	schemaMap := make(map[string]string)
	tablesMap := make(map[string]map[string]string)
	lowerCaseSchemaSet := make(map[string]string)
	for schema, tableNames := range tables {
		lcSchema := strings.ToLower(schema)
		// track if there are multiple schema names with the same lower case name.
		// just skip this kind of schemas.
		if rawSchema, ok := lowerCaseSchemaSet[lcSchema]; ok {
			delete(schemaMap, lcSchema)
			delete(tablesMap, lcSchema)
			log.L().Warn("skip check schema with same lower case value",
				zap.Strings("schemas", []string{schema, rawSchema}))
			continue
		}
		lowerCaseSchemaSet[lcSchema] = schema

		if lcSchema != schema {
			schemaMap[lcSchema] = schema
		}
		tblsMap := make(map[string]string)
		lowerCaseTableSet := make(map[string]string)
		for _, tb := range tableNames {
			lcTbl := strings.ToLower(tb)
			if rawTbl, ok := lowerCaseTableSet[lcTbl]; ok {
				delete(tblsMap, lcTbl)
				log.L().Warn("skip check tables with same lower case value", zap.String("schema", schema),
					zap.Strings("table", []string{tb, rawTbl}))
				continue
			}
			if lcTbl != tb {
				tblsMap[lcTbl] = tb
			}
		}
		if len(tblsMap) > 0 {
			tablesMap[lcSchema] = tblsMap
		}
	}
	return schemaMap, tablesMap
}

// initShardingGroups initializes sharding groups according to source MySQL, filter rules and router rules
// NOTE: now we don't support modify router rules after task has started.
func (s *Syncer) initShardingGroups(ctx context.Context, needCheck bool) error {
	// fetch tables from source and filter them
	sourceTables, err := s.fromDB.FetchAllDoTables(ctx, s.baList)
	if err != nil {
		return err
	}

	// convert according to router rules
	// target-ID -> source-IDs
	mapper := make(map[string][]string, len(sourceTables))
	for schema, tables := range sourceTables {
		for _, table := range tables {
			sourceTable := &filter.Table{Schema: schema, Name: table}
			targetTable := s.route(sourceTable)
			targetID := utils.GenTableID(targetTable)
			sourceID := utils.GenTableID(sourceTable)
			_, ok := mapper[targetID]
			if !ok {
				mapper[targetID] = make([]string, 0, len(tables))
			}
			mapper[targetID] = append(mapper[targetID], sourceID)
		}
	}

	loadMeta, err2 := s.sgk.LoadShardMeta(s.cfg.Flavor, s.cfg.EnableGTID)
	if err2 != nil {
		return err2
	}
	if needCheck && s.SourceTableNamesFlavor == utils.LCTableNamesSensitive {
		// try fix persistent data before init
		schemaMap, tableMap := buildLowerCaseTableNamesMap(sourceTables)
		if err2 = s.sgk.CheckAndFix(loadMeta, schemaMap, tableMap); err2 != nil {
			return err2
		}
	}

	// add sharding group
	for targetID, sourceIDs := range mapper {
		targetTable := utils.UnpackTableID(targetID)
		_, _, _, _, err := s.sgk.AddGroup(targetTable, sourceIDs, loadMeta[targetID], false)
		if err != nil {
			return err
		}
	}

	shardGroup := s.sgk.Groups()
	s.tctx.L().Debug("initial sharding groups", zap.Int("shard group length", len(shardGroup)), zap.Reflect("shard group", shardGroup))

	return nil
}

// IsFreshTask implements Unit.IsFreshTask.
func (s *Syncer) IsFreshTask(ctx context.Context) (bool, error) {
	globalPoint := s.checkpoint.GlobalPoint()
	tablePoint := s.checkpoint.TablePoint()
	// doesn't have neither GTID nor binlog pos
	return binlog.IsFreshPosition(globalPoint, s.cfg.Flavor, s.cfg.EnableGTID) && len(tablePoint) == 0, nil
}

func (s *Syncer) reset() {
	if s.streamerController != nil {
		s.streamerController.Close(s.tctx)
	}
	// create new job chans
	s.newJobChans(s.cfg.WorkerCount + 1)

	s.execError.Store(nil)
	s.setErrLocation(nil, nil, false)
	s.isReplacingErr = false
	s.waitXIDJob.Store(int64(noWait))
	s.isTransactionEnd = true

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
		err = s.toDBConns[i].ResetConn(tctx)
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
		err = s.sgk.dbConn.ResetConn(tctx)
		if err != nil {
			return terror.WithScope(err, terror.ScopeDownstream)
		}
	}

	err = s.ddlDBConn.ResetConn(tctx)
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
	metrics.SyncerExitWithErrorCounter.WithLabelValues(s.cfg.Name, s.cfg.SourceID).Add(0)

	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// create new done chan
	// use lock of Syncer to avoid Close while Process
	s.Lock()
	if s.isClosed() {
		s.Unlock()
		return
	}
	s.done = make(chan struct{})
	s.Unlock()

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
			metrics.SyncerExitWithErrorCounter.WithLabelValues(s.cfg.Name, s.cfg.SourceID).Inc()
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
			s.tctx.L().Info("filter out error caused by user cancel", log.ShortError(err))
		} else {
			metrics.SyncerExitWithErrorCounter.WithLabelValues(s.cfg.Name, s.cfg.SourceID).Inc()
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

func (s *Syncer) getTableInfo(tctx *tcontext.Context, origTable, targetTable *filter.Table) (*model.TableInfo, error) {
	ti, err := s.schemaTracker.GetTableInfo(origTable)
	if err == nil {
		return ti, nil
	}
	if !schema.IsTableNotExists(err) {
		return nil, terror.ErrSchemaTrackerCannotGetTable.Delegate(err, origTable)
	}

	if err = s.schemaTracker.CreateSchemaIfNotExists(origTable.Schema); err != nil {
		return nil, terror.ErrSchemaTrackerCannotCreateSchema.Delegate(err, origTable.Schema)
	}

	// if table already exists in checkpoint, create it in schema tracker
	if ti = s.checkpoint.GetFlushedTableInfo(origTable); ti != nil {
		if err = s.schemaTracker.CreateTableIfNotExists(origTable, ti); err != nil {
			return nil, terror.ErrSchemaTrackerCannotCreateTable.Delegate(err, origTable)
		}
		tctx.L().Debug("lazy init table info in schema tracker", zap.Stringer("table", origTable))
		return ti, nil
	}

	// in optimistic shard mode, we should try to get the init schema (the one before modified by other tables) first.
	if s.cfg.ShardMode == config.ShardOptimistic {
		ti, err = s.trackInitTableInfoOptimistic(origTable, targetTable)
		if err != nil {
			return nil, err
		}
	}

	// if the table does not exist (IsTableNotExists(err)), continue to fetch the table from downstream and create it.
	if ti == nil {
		err = s.trackTableInfoFromDownstream(tctx, origTable, targetTable)
		if err != nil {
			return nil, err
		}
	}

	ti, err = s.schemaTracker.GetTableInfo(origTable)
	if err != nil {
		return nil, terror.ErrSchemaTrackerCannotGetTable.Delegate(err, origTable)
	}
	return ti, nil
}

// trackTableInfoFromDownstream tries to track the table info from the downstream. It will not overwrite existing table.
func (s *Syncer) trackTableInfoFromDownstream(tctx *tcontext.Context, origTable, targetTable *filter.Table) error {
	// TODO: Switch to use the HTTP interface to retrieve the TableInfo directly if HTTP port is available
	// use parser for downstream.
	parser2, err := utils.GetParserForConn(tctx.Ctx, s.ddlDBConn.BaseConn.DBConn)
	if err != nil {
		return terror.ErrSchemaTrackerCannotParseDownstreamTable.Delegate(err, targetTable, origTable)
	}

	rows, err := s.ddlDBConn.QuerySQL(tctx, "SHOW CREATE TABLE "+targetTable.String())
	if err != nil {
		return terror.ErrSchemaTrackerCannotFetchDownstreamTable.Delegate(err, targetTable, origTable)
	}
	defer rows.Close()

	for rows.Next() {
		var tableName, createSQL string
		if err = rows.Scan(&tableName, &createSQL); err != nil {
			return terror.WithScope(terror.DBErrorAdapt(err, terror.ErrDBDriverError), terror.ScopeDownstream)
		}

		// rename the table back to original.
		var createNode ast.StmtNode
		createNode, err = parser2.ParseOneStmt(createSQL, "", "")
		if err != nil {
			return terror.ErrSchemaTrackerCannotParseDownstreamTable.Delegate(err, targetTable, origTable)
		}
		createStmt := createNode.(*ast.CreateTableStmt)
		createStmt.IfNotExists = true
		createStmt.Table.Schema = model.NewCIStr(origTable.Schema)
		createStmt.Table.Name = model.NewCIStr(origTable.Name)

		// schema tracker sets non-clustered index, so can't handle auto_random.
		if v, _ := s.schemaTracker.GetSystemVar(schema.TiDBClusteredIndex); v == "OFF" {
			for _, col := range createStmt.Cols {
				for i, opt := range col.Options {
					if opt.Tp == ast.ColumnOptionAutoRandom {
						// col.Options is unordered
						col.Options[i] = col.Options[len(col.Options)-1]
						col.Options = col.Options[:len(col.Options)-1]
						break
					}
				}
			}
		}

		var newCreateSQLBuilder strings.Builder
		restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &newCreateSQLBuilder)
		if err = createStmt.Restore(restoreCtx); err != nil {
			return terror.ErrSchemaTrackerCannotParseDownstreamTable.Delegate(err, targetTable, origTable)
		}
		newCreateSQL := newCreateSQLBuilder.String()
		tctx.L().Debug("reverse-synchronized table schema",
			zap.Stringer("origTable", origTable),
			zap.Stringer("targetTable", targetTable),
			zap.String("sql", newCreateSQL),
		)
		if err = s.schemaTracker.Exec(tctx.Ctx, origTable.Schema, newCreateSQL); err != nil {
			return terror.ErrSchemaTrackerCannotCreateTable.Delegate(err, origTable)
		}
	}

	if err = rows.Err(); err != nil {
		return terror.WithScope(terror.DBErrorAdapt(err, terror.ErrDBDriverError), terror.ScopeDownstream)
	}
	return nil
}

func (s *Syncer) addCount(isFinished bool, queueBucket string, tp opType, n int64, targetTable *filter.Table) {
	m := metrics.AddedJobsTotal
	if isFinished {
		s.count.Add(n)
		m = metrics.FinishedJobsTotal
	}
	switch tp {
	case insert, update, del, ddl, flush:
		m.WithLabelValues(tp.String(), s.cfg.Name, queueBucket, s.cfg.SourceID, s.cfg.WorkerName, targetTable.Schema, targetTable.Name).Add(float64(n))
	case skip, xid:
		// ignore skip/xid jobs
	default:
		s.tctx.L().Warn("unknown job operation type", zap.Stringer("type", tp))
	}
}

func (s *Syncer) calcReplicationLag(headerTS int64) int64 {
	return time.Now().Unix() - s.tsOffset.Load() - headerTS
}

// updateReplicationJobTS store job TS, it is called after every batch dml job / one skip job / one ddl job is added and committed.
func (s *Syncer) updateReplicationJobTS(job *job, jobIdx int) {
	// when job is nil mean no job in this bucket, need do reset this bucket job ts to 0
	if job == nil {
		s.workerJobTSArray[jobIdx].Store(0)
	} else {
		s.workerJobTSArray[jobIdx].Store(int64(job.eventHeader.Timestamp))
	}
}

func (s *Syncer) updateReplicationLagMetric() {
	var lag int64
	var minTS int64

	for idx := range s.workerJobTSArray {
		if ts := s.workerJobTSArray[idx].Load(); ts != int64(0) {
			if minTS == int64(0) || ts < minTS {
				minTS = ts
			}
		}
	}
	if minTS != int64(0) {
		lag = s.calcReplicationLag(minTS)
	}
	metrics.ReplicationLagHistogram.WithLabelValues(s.cfg.Name, s.cfg.SourceID, s.cfg.WorkerName).Observe(float64(lag))
	metrics.ReplicationLagGauge.WithLabelValues(s.cfg.Name, s.cfg.SourceID, s.cfg.WorkerName).Set(float64(lag))
	s.secondsBehindMaster.Store(lag)

	failpoint.Inject("ShowLagInLog", func(v failpoint.Value) {
		minLag := v.(int)
		if int(lag) >= minLag {
			s.tctx.L().Info("ShowLagInLog", zap.Int64("lag", lag))
		}
	})

	// reset skip job TS in case of skip job TS is never updated
	if minTS == s.workerJobTSArray[skipJobIdx].Load() {
		s.workerJobTSArray[skipJobIdx].Store(0)
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

func (s *Syncer) saveTablePoint(table *filter.Table, location binlog.Location) {
	ti, err := s.schemaTracker.GetTableInfo(table)
	if err != nil && table.Name != "" {
		s.tctx.L().DPanic("table info missing from schema tracker",
			zap.Stringer("table", table),
			zap.Stringer("location", location),
			zap.Error(err))
	}
	s.checkpoint.SaveTablePoint(table, location, ti)
}

// only used in tests.
var (
	lastLocation    binlog.Location
	lastLocationNum int
	waitJobsDone    bool
	failExecuteSQL  bool
	failOnce        atomic.Bool
)

func (s *Syncer) addJob(job *job) error {
	s.waitTransactionLock.Lock()
	defer s.waitTransactionLock.Unlock()

	failpoint.Inject("countJobFromOneEvent", func() {
		if job.currentLocation.Position.Compare(lastLocation.Position) == 0 {
			lastLocationNum++
		} else {
			lastLocation = job.currentLocation
			lastLocationNum = 1
		}
		// trigger a flush after see one job
		if lastLocationNum == 1 {
			waitJobsDone = true
			s.tctx.L().Info("meet the first job of an event", zap.Any("binlog position", lastLocation))
		}
		// mock a execution error after see two jobs.
		if lastLocationNum == 2 {
			failExecuteSQL = true
			s.tctx.L().Info("meet the second job of an event", zap.Any("binlog position", lastLocation))
		}
	})
	failpoint.Inject("countJobFromOneGTID", func() {
		if binlog.CompareLocation(job.currentLocation, lastLocation, true) == 0 {
			lastLocationNum++
		} else {
			lastLocation = job.currentLocation
			lastLocationNum = 1
		}
		// trigger a flush after see one job
		if lastLocationNum == 1 {
			waitJobsDone = true
			s.tctx.L().Info("meet the first job of a GTID", zap.Any("binlog position", lastLocation))
		}
		// mock a execution error after see two jobs.
		if lastLocationNum == 2 {
			failExecuteSQL = true
			s.tctx.L().Info("meet the second job of a GTID", zap.Any("binlog position", lastLocation))
		}
	})

	failpoint.Inject("checkCheckpointInMiddleOfTransaction", func() {
		if waitXIDStatus(s.waitXIDJob.Load()) == waiting {
			s.tctx.L().Info("not receive xid job yet", zap.Any("next job", job))
		}
	})

	if waitXIDStatus(s.waitXIDJob.Load()) == waitComplete {
		s.tctx.L().Info("All jobs is completed before syncer close, the coming job will be reject", zap.Any("job", job))
		return nil
	}
	var queueBucket int
	switch job.tp {
	case xid:
		s.waitXIDJob.CAS(int64(waiting), int64(waitComplete))
		s.saveGlobalPoint(job.location)
		s.isTransactionEnd = true
		return nil
	case skip:
		s.updateReplicationJobTS(job, skipJobIdx)
	case flush:
		s.addCount(false, adminQueueName, job.tp, 1, job.targetTable)
		// ugly code addJob and sync, refine it later
		s.jobWg.Add(s.cfg.WorkerCount)
		for i := 0; i < s.cfg.WorkerCount; i++ {
			startTime := time.Now()
			s.jobs[i] <- job
			// flush for every DML queue
			metrics.AddJobDurationHistogram.WithLabelValues("flush", s.cfg.Name, s.queueBucketMapping[i], s.cfg.SourceID).Observe(time.Since(startTime).Seconds())
		}
		s.jobWg.Wait()
		s.addCount(true, adminQueueName, job.tp, 1, job.targetTable)
		return s.flushCheckPoints()
	case ddl:
		s.jobWg.Wait()
		s.addCount(false, adminQueueName, job.tp, 1, job.targetTable)
		s.updateReplicationJobTS(job, ddlJobIdx)
		s.jobWg.Add(1)
		queueBucket = s.cfg.WorkerCount
		startTime := time.Now()
		s.jobs[queueBucket] <- job
		metrics.AddJobDurationHistogram.WithLabelValues("ddl", s.cfg.Name, adminQueueName, s.cfg.SourceID).Observe(time.Since(startTime).Seconds())
	case insert, update, del:
		s.jobWg.Add(1)
		queueBucket = int(utils.GenHashKey(job.key)) % s.cfg.WorkerCount
		s.addCount(false, s.queueBucketMapping[queueBucket], job.tp, 1, job.targetTable)
		startTime := time.Now()
		s.tctx.L().Debug("queue for key", zap.Int("queue", queueBucket), zap.String("key", job.key))
		s.jobs[queueBucket] <- job
		s.isTransactionEnd = false
		failpoint.Inject("checkCheckpointInMiddleOfTransaction", func() {
			s.tctx.L().Info("receive dml job", zap.Any("dml job", job))
			time.Sleep(100 * time.Millisecond)
		})
		metrics.AddJobDurationHistogram.WithLabelValues(job.tp.String(), s.cfg.Name, s.queueBucketMapping[queueBucket], s.cfg.SourceID).Observe(time.Since(startTime).Seconds())
	}

	// nolint:ifshort
	wait := s.checkWait(job)
	failpoint.Inject("flushFirstJob", func() {
		if waitJobsDone {
			s.tctx.L().Info("trigger flushFirstJob")
			waitJobsDone = false
			wait = true
		}
	})
	if wait {
		s.jobWg.Wait()
		s.c.reset()
	}

	if s.execError.Load() != nil {
		// nolint:nilerr
		return nil
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
		for sourceSchema, tbs := range job.sourceTbls {
			if len(sourceSchema) == 0 {
				continue
			}
			for _, sourceTable := range tbs {
				s.saveTablePoint(sourceTable, job.location)
			}
		}
		// reset sharding group after checkpoint saved
		s.resetShardingGroup(job.targetTable)
	case insert, update, del:
		// save job's current pos for DML events
		for sourceSchema, tbs := range job.sourceTbls {
			if len(sourceSchema) == 0 {
				continue
			}
			for _, sourceTable := range tbs {
				s.saveTablePoint(sourceTable, job.currentLocation)
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
	s.checkpoint.SaveGlobalPoint(globalLocation)
}

func (s *Syncer) resetShardingGroup(table *filter.Table) {
	if s.cfg.ShardMode == config.ShardPessimistic {
		// for DDL sharding group, reset group after checkpoint saved
		group := s.sgk.Group(table)
		if group != nil {
			group.Reset()
		}
	}
}

// flushCheckPoints flushes previous saved checkpoint in memory to persistent storage, like TiDB
// we flush checkpoints in four cases:
//   1. DDL executed
//   2. at intervals (and job executed)
//   3. pausing / stopping the sync (driven by `s.flushJobs`)
//   4. IsFreshTask return true
// but when error occurred, we can not flush checkpoint, otherwise data may lost
// and except rejecting to flush the checkpoint, we also need to rollback the checkpoint saved before
//   this should be handled when `s.Run` returned
//
// we may need to refactor the concurrency model to make the work-flow more clearer later.
func (s *Syncer) flushCheckPoints() error {
	err := s.execError.Load()
	// TODO: for now, if any error occurred (including user canceled), checkpoint won't be updated. But if we have put
	// optimistic shard info, DM-master may resolved the optimistic lock and let other worker execute DDL. So after this
	// worker resume, it can not execute the DML/DDL in old binlog because of downstream table structure mismatching.
	// We should find a way to (compensating) implement a transaction containing interaction with both etcd and SQL.
	if err != nil && (terror.ErrDBExecuteFailed.Equal(err) || terror.ErrDBUnExpect.Equal(err)) {
		s.tctx.L().Warn("error detected when executing SQL job, skip flush checkpoint",
			zap.Stringer("checkpoint", s.checkpoint),
			zap.Error(err))
		return nil
	}

	var (
		exceptTableIDs map[string]bool
		exceptTables   []*filter.Table
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

	err = s.checkpoint.FlushPointsExcept(s.tctx, exceptTables, shardMetaSQLs, shardMetaArgs)
	if err != nil {
		return terror.Annotatef(err, "flush checkpoint %s", s.checkpoint)
	}
	s.tctx.L().Info("flushed checkpoint", zap.Stringer("checkpoint", s.checkpoint))

	// update current active relay log after checkpoint flushed
	err = s.updateActiveRelayLog(s.checkpoint.GlobalPoint().Position)
	if err != nil {
		return err
	}

	now := time.Now()
	if !s.lastCheckpointFlushedTime.IsZero() {
		duration := now.Sub(s.lastCheckpointFlushedTime).Seconds()
		metrics.FlushCheckPointsTimeInterval.WithLabelValues(s.cfg.WorkerName, s.cfg.Name, s.cfg.SourceID).Observe(duration)
	}
	s.lastCheckpointFlushedTime = now

	s.tctx.L().Info("after last flushing checkpoint, DM has ignored row changes by expression filter",
		zap.Int64("number of filtered insert", s.filteredInsert.Load()),
		zap.Int64("number of filtered update", s.filteredUpdate.Load()),
		zap.Int64("number of filtered delete", s.filteredDelete.Load()))

	s.filteredInsert.Store(0)
	s.filteredUpdate.Store(0)
	s.filteredDelete.Store(0)

	return nil
}

// DDL synced one by one, so we only need to process one DDL at a time.
func (s *Syncer) syncDDL(tctx *tcontext.Context, queueBucket string, db *dbconn.DBConn, ddlJobChan chan *job) {
	defer s.wg.Done()

	var err error
	for {
		ddlJob, ok := <-ddlJobChan
		if !ok {
			return
		}

		failpoint.Inject("BlockDDLJob", func(v failpoint.Value) {
			t := v.(int) // sleep time
			s.tctx.L().Info("BlockDDLJob", zap.Any("job", ddlJob), zap.Int("sleep time", t))
			time.Sleep(time.Second * time.Duration(t))
		})

		var (
			ignore           = false
			shardPessimistOp *pessimism.Operation
		)
		switch s.cfg.ShardMode {
		case config.ShardPessimistic:
			shardPessimistOp = s.pessimist.PendingOperation()
			if shardPessimistOp != nil && !shardPessimistOp.Exec {
				ignore = true
				tctx.L().Info("ignore shard DDLs in pessimistic shard mode", zap.Strings("ddls", ddlJob.ddls))
			}
		case config.ShardOptimistic:
			if len(ddlJob.ddls) == 0 {
				ignore = true
				tctx.L().Info("ignore shard DDLs in optimistic mode", zap.Stringer("info", s.optimist.PendingInfo()))
			}
		}

		failpoint.Inject("ExecDDLError", func() {
			s.tctx.L().Warn("execute ddl error", zap.Strings("DDL", ddlJob.ddls), zap.String("failpoint", "ExecDDLError"))
			err = terror.ErrDBUnExpect.Delegate(errors.Errorf("execute ddl %v error", ddlJob.ddls))
			failpoint.Goto("bypass")
		})

		if !ignore {
			var affected int
			affected, err = db.ExecuteSQLWithIgnore(tctx, ignoreDDLError, ddlJob.ddls)
			if err != nil {
				err = s.handleSpecialDDLError(tctx, err, ddlJob.ddls, affected, db)
				err = terror.WithScope(err, terror.ScopeDownstream)
			}
		}
		failpoint.Label("bypass")
		failpoint.Inject("SafeModeExit", func(val failpoint.Value) {
			if intVal, ok := val.(int); ok && (intVal == 2 || intVal == 3) {
				s.tctx.L().Warn("mock safe mode error", zap.Strings("DDL", ddlJob.ddls), zap.String("failpoint", "SafeModeExit"))
				if intVal == 2 {
					err = terror.ErrWorkerDDLLockInfoNotFound.Generatef("DDL info not found")
				} else {
					err = terror.ErrDBExecuteFailed.Delegate(errors.Errorf("execute ddl %v error", ddlJob.ddls))
				}
			}
		})
		// If downstream has error (which may cause by tracker is more compatible than downstream), we should stop handling
		// this job, set `s.execError` to let caller of `addJob` discover error
		if err != nil {
			s.execError.Store(err)
			if !utils.IsContextCanceledError(err) {
				err = s.handleEventError(err, ddlJob.startLocation, ddlJob.currentLocation, true, ddlJob.originSQL)
				s.runFatalChan <- unit.NewProcessError(err)
			}
			s.jobWg.Done()
			continue
		}

		switch s.cfg.ShardMode {
		case config.ShardPessimistic:
			// for sharding DDL syncing, send result back
			shardInfo := s.pessimist.PendingInfo()
			switch {
			case shardInfo == nil:
				// no need to do the shard DDL handle for `CREATE DATABASE/TABLE` now.
				tctx.L().Warn("skip shard DDL handle in pessimistic shard mode", zap.Strings("ddl", ddlJob.ddls))
			case shardPessimistOp == nil:
				err = terror.ErrWorkerDDLLockOpNotFound.Generate(shardInfo)
			default:
				err = s.pessimist.DoneOperationDeleteInfo(*shardPessimistOp, *shardInfo)
			}
		case config.ShardOptimistic:
			shardInfo := s.optimist.PendingInfo()
			switch {
			case shardInfo == nil:
				// no need to do the shard DDL handle for `DROP DATABASE/TABLE` now.
				// but for `CREATE DATABASE` and `ALTER DATABASE` we execute it to the downstream directly without `shardInfo`.
				if ignore { // actually ignored.
					tctx.L().Warn("skip shard DDL handle in optimistic shard mode", zap.Strings("ddl", ddlJob.ddls))
				}
			case s.optimist.PendingOperation() == nil:
				err = terror.ErrWorkerDDLLockOpNotFound.Generate(shardInfo)
			default:
				err = s.optimist.DoneOperation(*(s.optimist.PendingOperation()))
			}
		}
		if err != nil {
			if s.execError.Load() == nil {
				s.execError.Store(err)
			}
			if !utils.IsContextCanceledError(err) {
				err = s.handleEventError(err, ddlJob.startLocation, ddlJob.currentLocation, true, ddlJob.originSQL)
				s.runFatalChan <- unit.NewProcessError(err)
			}
			s.jobWg.Done()
			continue
		}
		s.jobWg.Done()
		s.addCount(true, queueBucket, ddlJob.tp, int64(len(ddlJob.ddls)), ddlJob.targetTable)
		// reset job TS when this ddl is finished.
		s.updateReplicationJobTS(nil, ddlJobIdx)
	}
}

// DML synced in batch by one worker.
func (s *Syncer) syncDML(
	tctx *tcontext.Context, queueBucket string, db *dbconn.DBConn, jobChan chan *job, workerJobIdx int) {
	defer s.wg.Done()

	idx := 0
	count := s.cfg.Batch
	jobs := make([]*job, 0, count)
	// db_schema->db_table->opType
	tpCnt := make(map[string]map[string]map[opType]int64)
	queueID := fmt.Sprint(dmlWorkerJobIdxToQueueID(workerJobIdx))

	// clearF is used to reset job queue.
	clearF := func() {
		for i := 0; i < idx; i++ {
			s.jobWg.Done()
		}
		idx = 0
		jobs = jobs[0:0]
		// clear tpCnt map
		tpCnt = make(map[string]map[string]map[opType]int64)
	}

	// successF is used to calculate lag metric and q/tps.
	successF := func() {
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
				metrics.FinishedTransactionTotal.WithLabelValues(s.cfg.Name, s.cfg.WorkerName, s.cfg.SourceID).Inc()
			}
		}
		// calculate qps
		for dbSchema, tableM := range tpCnt {
			for dbTable, tpM := range tableM {
				for tpName, cnt := range tpM {
					s.addCount(true, queueBucket, tpName, cnt, &filter.Table{Schema: dbSchema, Name: dbTable})
				}
			}
		}
		// reset job TS when this batch is finished.
		s.updateReplicationJobTS(nil, workerJobIdx)
		metrics.ReplicationTransactionBatch.WithLabelValues(s.cfg.WorkerName, s.cfg.Name, s.cfg.SourceID, queueBucket).Observe(float64(len(jobs)))
	}

	fatalF := func(affected int, err error) {
		s.execError.Store(err)
		if !utils.IsContextCanceledError(err) {
			err = s.handleEventError(err, jobs[affected].startLocation, jobs[affected].currentLocation, false, "")
			s.runFatalChan <- unit.NewProcessError(err)
		}
		clearF()
	}

	executeSQLs := func() (int, error) {
		if len(jobs) == 0 {
			return 0, nil
		}
		failpoint.Inject("BlockExecuteSQLs", func(v failpoint.Value) {
			t := v.(int) // sleep time
			s.tctx.L().Info("BlockExecuteSQLs", zap.Any("job", jobs[0]), zap.Int("sleep time", t))
			time.Sleep(time.Second * time.Duration(t))
		})

		failpoint.Inject("failSecondJob", func() {
			if failExecuteSQL && failOnce.CAS(false, true) {
				s.tctx.L().Info("trigger failSecondJob")
				failpoint.Return(0, terror.ErrDBExecuteFailed.Delegate(errors.New("failSecondJob"), "mock"))
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
		ctctx, cancel := tctx.WithTimeout(maxDMLExecutionDuration)
		defer cancel()
		affect, err := db.ExecuteSQL(ctctx, queries, args...)
		failpoint.Inject("SafeModeExit", func(val failpoint.Value) {
			if intVal, ok := val.(int); ok && intVal == 4 && len(jobs) > 0 {
				s.tctx.L().Warn("fail to exec DML", zap.String("failpoint", "SafeModeExit"))
				affect, err = 0, terror.ErrDBExecuteFailed.Delegate(errors.New("SafeModeExit"), "mock")
			}
		})
		return affect, err
	}

	var err error
	var affect int
	tickerInterval := waitTime
	failpoint.Inject("changeTickerInterval", func(val failpoint.Value) {
		t := val.(int)
		tickerInterval = time.Duration(t) * time.Second
		tctx.L().Info("changeTickerInterval", zap.Int("current ticker interval second", t))
	})
	timer := time.NewTimer(tickerInterval)
	defer timer.Stop()

	for {
		// resets the time interval for each loop to prevent a certain amount of time being spent on the previous ticker
		// execution to `executeSQLs` resulting in the next ticker not waiting for the full waitTime.
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(tickerInterval)
		failpoint.Inject("noJobInQueueLog", func() {
			tctx.L().Debug("timer Reset",
				zap.Int("workerJobIdx", workerJobIdx),
				zap.Duration("tickerInterval", tickerInterval),
				zap.Int64("current ts", time.Now().Unix()))
		})

		select {
		case sqlJob, ok := <-jobChan:
			metrics.QueueSizeGauge.WithLabelValues(s.cfg.Name, queueID, s.cfg.SourceID).Set(float64(len(jobChan)))
			if !ok {
				if len(jobs) > 0 {
					tctx.L().Warn("have unexecuted jobs when close job chan!", zap.Any("rest job", jobs))
				}
				return
			}
			idx++
			if sqlJob.tp != flush && len(sqlJob.sql) > 0 {
				if len(jobs) == 0 {
					// set job TS when received first job of this batch.
					s.updateReplicationJobTS(sqlJob, workerJobIdx)
				}
				jobs = append(jobs, sqlJob)
				schemaName, tableName := sqlJob.targetTable.Schema, sqlJob.targetTable.Name
				if _, ok := tpCnt[schemaName]; !ok {
					tpCnt[schemaName] = make(map[string]map[opType]int64)
				}
				if _, ok := tpCnt[schemaName][tableName]; !ok {
					tpCnt[schemaName][tableName] = make(map[opType]int64)
				}
				tpCnt[schemaName][tableName][sqlJob.tp]++
			}

			if idx >= count || sqlJob.tp == flush {
				affect, err = executeSQLs()
				if err != nil {
					fatalF(affect, err)
					continue
				}
				successF()
				clearF()
			}
		case <-timer.C:
			if len(jobs) > 0 {
				failpoint.Inject("syncDMLTicker", func() {
					tctx.L().Info("job queue not full, executeSQLs by ticker")
				})
				affect, err = executeSQLs()
				if err != nil {
					fatalF(affect, err)
					continue
				}
				successF()
				clearF()
			} else {
				failpoint.Inject("noJobInQueueLog", func() {
					tctx.L().Debug("no job in queue, update lag to zero", zap.Int(
						"workerJobIdx", workerJobIdx), zap.Int64("current ts", time.Now().Unix()))
				})
				// reset job TS when there is no job in the queue
				s.updateReplicationJobTS(nil, workerJobIdx)
			}
		}
	}
}

// Run starts running for sync, we should guarantee it can rerun when paused.
func (s *Syncer) Run(ctx context.Context) (err error) {
	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()
	tctx := s.tctx.WithContext(runCtx)

	defer func() {
		if s.done != nil {
			close(s.done)
		}
	}()

	go func() {
		<-ctx.Done()
		select {
		case <-runCtx.Done():
		default:
			tctx.L().Info("received subtask's done")

			s.waitTransactionLock.Lock()
			if s.isTransactionEnd {
				s.waitXIDJob.Store(int64(waitComplete))
				s.jobWg.Wait()
				tctx.L().Info("the last job is transaction end, done directly")
				runCancel()
				s.waitTransactionLock.Unlock()
				return
			}
			s.waitXIDJob.Store(int64(waiting))
			s.waitTransactionLock.Unlock()

			select {
			case <-runCtx.Done():
				tctx.L().Info("received syncer's done")
			case <-time.After(maxPauseOrStopWaitTime):
				tctx.L().Info("wait transaction end timeout")
				s.jobWg.Wait()
				runCancel()
			}
		}
	}()

	// some initialization that can't be put in Syncer.Init
	fresh, err := s.IsFreshTask(runCtx)
	if err != nil {
		return err
	} else if fresh {
		// for fresh task, we try to load checkpoints from meta (file or config item)
		err = s.checkpoint.LoadMeta()
		if err != nil {
			return err
		}
	}

	var (
		flushCheckpoint bool
		delLoadTask     bool
		cleanDumpFile   = s.cfg.CleanDumpFile
	)
	flushCheckpoint, err = s.adjustGlobalPointGTID(tctx)
	if err != nil {
		return err
	}
	if s.cfg.Mode == config.ModeAll && fresh {
		delLoadTask = true
		flushCheckpoint = true
		// TODO: loadTableStructureFromDump in future
	} else {
		cleanDumpFile = false
	}

	if flushCheckpoint {
		if err = s.flushCheckPoints(); err != nil {
			tctx.L().Warn("fail to flush checkpoints when starting task", zap.Error(err))
			return err
		}
	}
	if delLoadTask {
		if err = s.delLoadTask(); err != nil {
			tctx.L().Warn("error when del load task in etcd", zap.Error(err))
		}
	}
	if cleanDumpFile {
		tctx.L().Info("try to remove all dump files")
		if err = os.RemoveAll(s.cfg.Dir); err != nil {
			tctx.L().Warn("error when remove loaded dump folder", zap.String("data folder", s.cfg.Dir), zap.Error(err))
		}
	}

	failpoint.Inject("AdjustGTIDExit", func() {
		tctx.L().Warn("exit triggered", zap.String("failpoint", "AdjustGTIDExit"))
		s.streamerController.Close(tctx)
		utils.OsExit(1)
	})

	updateTSOffset := func() error {
		t1 := time.Now()
		ts, tsErr := s.fromDB.GetServerUnixTS(runCtx)
		rtt := time.Since(t1).Seconds()
		if tsErr == nil {
			s.tsOffset.Store(time.Now().Unix() - ts - int64(rtt/2))
		}
		return tsErr
	}
	// before sync run, we get the tsoffset from upstream first
	if utErr := updateTSOffset(); utErr != nil {
		return utErr
	}
	// start background task to get/update current ts offset between dm and upstream
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		// temporarily hard code there. if this metrics works well add this to config file.
		updateTicker := time.NewTicker(time.Minute * 10)
		defer updateTicker.Stop()
		for {
			select {
			case <-updateTicker.C:
				if utErr := updateTSOffset(); utErr != nil {
					s.tctx.L().Error("get server unix ts err", zap.Error(utErr))
				}
			case <-runCtx.Done():
				return
			}
		}
	}()

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
	tctx.L().Info("replicate binlog from checkpoint", zap.Stringer("checkpoint", lastLocation))

	if s.streamerController.IsClosed() {
		err = s.streamerController.Start(tctx, lastLocation)
		if err != nil {
			return terror.Annotate(err, "fail to restart streamer controller")
		}
	}

	s.queueBucketMapping = make([]string, 0, s.cfg.WorkerCount+1)
	for i := 0; i < s.cfg.WorkerCount; i++ {
		s.wg.Add(1)
		name := queueBucketName(i)
		s.queueBucketMapping = append(s.queueBucketMapping, name)
		go func(i int, name string) {
			s.syncDML(tctx, name, s.toDBConns[i], s.jobs[i], dmlWorkerJobIdx(i))
		}(i, name)
	}

	s.queueBucketMapping = append(s.queueBucketMapping, adminQueueName)
	s.wg.Add(1)
	go func() {
		s.syncDDL(tctx, adminQueueName, s.ddlDBConn, s.jobs[s.cfg.WorkerCount])
	}()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		updateLagTicker := time.NewTicker(time.Millisecond * 100)
		defer updateLagTicker.Stop()
		for {
			select {
			case <-updateLagTicker.C:
				s.updateReplicationLagMetric()
			case <-runCtx.Done():
				return
			}
		}
	}()

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
		traceSource             = fmt.Sprintf("%s.syncer.%s", s.cfg.SourceID, s.cfg.Name)
	)

	defer func() {
		if err1 := recover(); err1 != nil {
			failpoint.Inject("ExitAfterSaveOnlineDDL", func() {
				tctx.L().Info("force panic")
				panic("ExitAfterSaveOnlineDDL")
			})
			tctx.L().Error("panic log", zap.Reflect("error message", err1), zap.Stack("stack"))
			err = terror.ErrSyncerUnitPanic.Generate(err1)
		}

		s.jobWg.Wait()
		var (
			err2            error
			exitSafeModeLoc binlog.Location
		)
		if binlog.CompareLocation(currentLocation, savedGlobalLastLocation, s.cfg.EnableGTID) > 0 {
			exitSafeModeLoc = currentLocation.Clone()
		} else {
			exitSafeModeLoc = savedGlobalLastLocation.Clone()
		}
		s.checkpoint.SaveSafeModeExitPoint(&exitSafeModeLoc)
		if err2 = s.execError.Load(); err2 != nil && (terror.ErrDBExecuteFailed.Equal(err2) || terror.ErrDBUnExpect.Equal(err2)) {
			err2 = s.checkpoint.FlushSafeModeExitPoint(s.tctx)
		} else {
			err2 = s.flushCheckPoints()
		}
		if err2 != nil {
			tctx.L().Warn("failed to flush checkpoints when exit task", zap.Error(err2))
		} else {
			tctx.L().Info("flush checkpoints when exit task")
		}
	}()

	now := time.Now()
	s.start.Store(now)
	s.lastTime.Store(now)

	tryReSync := true

	// safeMode makes syncer reentrant.
	// we make each operator reentrant to make syncer reentrant.
	// `replace` and `delete` are naturally reentrant.
	// use `delete`+`replace` to represent `update` can make `update`  reentrant.
	// but there are no ways to make `update` idempotent,
	// if we start syncer at an early position, database must bear a period of inconsistent state,
	// it's eventual consistency.
	s.safeMode = sm.NewSafeMode()
	s.enableSafeModeInitializationPhase(tctx)

	closeShardingResync := func() error {
		if shardingReSync == nil {
			return nil
		}

		// if remaining DDLs in sequence, redirect global stream to the next sharding DDL position
		if !shardingReSync.allResolved {
			nextLocation, err2 := s.sgk.ActiveDDLFirstLocation(shardingReSync.targetTable)
			if err2 != nil {
				return err2
			}

			currentLocation = nextLocation
			lastLocation = nextLocation
		} else {
			currentLocation = savedGlobalLastLocation
			lastLocation = savedGlobalLastLocation // restore global last pos
		}
		// if suffix>0, we are replacing error
		s.isReplacingErr = currentLocation.Suffix != 0

		err3 := s.streamerController.RedirectStreamer(tctx, currentLocation)
		if err3 != nil {
			return err3
		}

		shardingReSync = nil
		return nil
	}

	maybeSkipNRowsEvent := func(n int) error {
		if s.cfg.EnableGTID && n > 0 {
			for i := 0; i < n; {
				e, err1 := s.getEvent(tctx, currentLocation)
				if err1 != nil {
					return err
				}
				if _, ok := e.Event.(*replication.RowsEvent); ok {
					i++
				}
			}
			log.L().Info("discard event already consumed", zap.Int("count", n),
				zap.Any("cur_loc", currentLocation))
		}
		return nil
	}

	// eventIndex is the rows event index in this transaction, it's used to avoiding read duplicate event in gtid mode
	eventIndex := 0
	// the relay log file may be truncated(not end with an RotateEvent), in this situation, we may read some rows events
	// and then read from the gtid again, so we force enter safe-mode for one more transaction to avoid failure due to
	// conflict
	for {
		if s.execError.Load() != nil {
			return nil
		}
		s.currentLocationMu.Lock()
		s.currentLocationMu.currentLocation = currentLocation
		s.currentLocationMu.Unlock()

		// fetch from sharding resync channel if needed, and redirect global
		// stream to current binlog position recorded by ShardingReSync
		if shardingReSync == nil && len(shardingReSyncCh) > 0 {
			// some sharding groups need to re-syncing
			shardingReSync = <-shardingReSyncCh
			savedGlobalLastLocation = lastLocation // save global last location
			lastLocation = shardingReSync.currLocation

			currentLocation = shardingReSync.currLocation
			// if suffix>0, we are replacing error
			s.isReplacingErr = currentLocation.Suffix != 0
			err = s.streamerController.RedirectStreamer(tctx, shardingReSync.currLocation)
			if err != nil {
				return err
			}

			failpoint.Inject("ReSyncExit", func() {
				tctx.L().Warn("exit triggered", zap.String("failpoint", "ReSyncExit"))
				utils.OsExit(1)
			})
		}

		var e *replication.BinlogEvent

		startTime := time.Now()
		e, err = s.getEvent(tctx, currentLocation)

		failpoint.Inject("SafeModeExit", func(val failpoint.Value) {
			if intVal, ok := val.(int); ok && intVal == 1 {
				s.tctx.L().Warn("fail to get event", zap.String("failpoint", "SafeModeExit"))
				err = errors.New("connect: connection refused")
			}
		})
		failpoint.Inject("GetEventErrorInTxn", func(val failpoint.Value) {
			if intVal, ok := val.(int); ok && intVal == eventIndex {
				err = errors.New("failpoint triggered")
				s.tctx.L().Warn("failed to get event", zap.Int("event_index", eventIndex),
					zap.Any("cur_pos", currentLocation), zap.Any("las_pos", lastLocation),
					zap.Any("pos", e.Header.LogPos), log.ShortError(err))
			}
		})
		switch {
		case err == context.Canceled:
			tctx.L().Info("binlog replication main routine quit(context canceled)!", zap.Stringer("last location", lastLocation))
			return nil
		case err == context.DeadlineExceeded:
			tctx.L().Info("deadline exceeded when fetching binlog event")
			continue
		case isDuplicateServerIDError(err):
			// if the server id is already used, need to use a new server id
			tctx.L().Info("server id is already used by another slave, will change to a new server id and get event again")
			err1 := s.streamerController.UpdateServerIDAndResetReplication(tctx, lastLocation)
			if err1 != nil {
				return err1
			}
			continue
		case err == streamer.ErrorMaybeDuplicateEvent:
			tctx.L().Warn("read binlog met a truncated file, need to open safe-mode until the next transaction")
			err = maybeSkipNRowsEvent(eventIndex)
			if err == nil {
				continue
			}
			log.L().Warn("skip duplicate rows event failed", zap.Error(err))
		}

		if err != nil {
			tctx.L().Error("fail to fetch binlog", log.ShortError(err))

			if isConnectionRefusedError(err) {
				return err
			}

			if s.streamerController.CanRetry(err) {
				// GlobalPoint is the last finished GTID
				err = s.streamerController.ResetReplicationSyncer(tctx, s.checkpoint.GlobalPoint())
				if err != nil {
					return err
				}
				log.L().Info("reset replication binlog puller", zap.Any("pos", s.checkpoint.GlobalPoint()))
				if err = maybeSkipNRowsEvent(eventIndex); err != nil {
					return err
				}
				continue
			}

			// try to re-sync in gtid mode
			if tryReSync && s.cfg.EnableGTID && utils.IsErrBinlogPurged(err) && s.cfg.AutoFixGTID {
				time.Sleep(retryTimeout)
				err = s.reSyncBinlog(*tctx, lastLocation)
				if err != nil {
					return err
				}
				tryReSync = false
				continue
			}

			return terror.ErrSyncerGetEvent.Generate(err)
		}

		failpoint.Inject("IgnoreSomeTypeEvent", func(val failpoint.Value) {
			if e.Header.EventType.String() == val.(string) {
				tctx.L().Debug("IgnoreSomeTypeEvent", zap.Reflect("event", e))
				failpoint.Continue()
			}
		})

		// time duration for reading an event from relay log or upstream master.
		metrics.BinlogReadDurationHistogram.WithLabelValues(s.cfg.Name, s.cfg.SourceID).Observe(time.Since(startTime).Seconds())
		startTime = time.Now() // reset start time for the next metric.

		// get binlog event, reset tryReSync, so we can re-sync binlog while syncer meets errors next time
		tryReSync = true
		metrics.BinlogPosGauge.WithLabelValues("syncer", s.cfg.Name, s.cfg.SourceID).Set(float64(e.Header.LogPos))
		index, err := binlog.GetFilenameIndex(lastLocation.Position.Name)
		if err != nil {
			tctx.L().Warn("fail to get index number of binlog file, may because only specify GTID and hasn't saved according binlog position", log.ShortError(err))
		} else {
			metrics.BinlogFileGauge.WithLabelValues("syncer", s.cfg.Name, s.cfg.SourceID).Set(float64(index))
		}
		s.binlogSizeCount.Add(int64(e.Header.EventSize))
		metrics.BinlogEventSizeHistogram.WithLabelValues(s.cfg.Name, s.cfg.WorkerName, s.cfg.SourceID).Observe(float64(e.Header.EventSize))

		failpoint.Inject("ProcessBinlogSlowDown", nil)

		tctx.L().Debug("receive binlog event", zap.Reflect("header", e.Header))

		// TODO: support all event
		// we calculate startLocation and endLocation(currentLocation) for Query event here
		// set startLocation empty for other events to avoid misuse
		startLocation = binlog.Location{}
		if ev, ok := e.Event.(*replication.QueryEvent); ok {
			startLocation = binlog.InitLocation(
				mysql.Position{
					Name: lastLocation.Position.Name,
					Pos:  e.Header.LogPos - e.Header.EventSize,
				},
				lastLocation.GetGTID(),
			)
			startLocation.Suffix = currentLocation.Suffix

			endSuffix := startLocation.Suffix
			if s.isReplacingErr {
				endSuffix++
			}
			currentLocation = binlog.InitLocation(
				mysql.Position{
					Name: lastLocation.Position.Name,
					Pos:  e.Header.LogPos,
				},
				lastLocation.GetGTID(),
			)
			currentLocation.Suffix = endSuffix

			err = currentLocation.SetGTID(ev.GSet)
			if err != nil {
				return terror.Annotatef(err, "fail to record GTID %v", ev.GSet)
			}

			if !s.isReplacingErr {
				apply, op := s.errOperatorHolder.MatchAndApply(startLocation, currentLocation, e.Header.Timestamp)
				if apply {
					if op == pb.ErrorOp_Replace {
						s.isReplacingErr = true
						// revert currentLocation to startLocation
						currentLocation = startLocation
					} else if op == pb.ErrorOp_Skip {
						s.saveGlobalPoint(currentLocation)
						err = s.flushJobs()
						if err != nil {
							tctx.L().Warn("failed to flush jobs when handle-error skip", zap.Error(err))
						} else {
							tctx.L().Info("flush jobs when handle-error skip")
						}
					}
					// skip the current event
					continue
				}
			}
			// set endLocation.Suffix=0 of last replace event
			// also redirect stream to next event
			if currentLocation.Suffix > 0 && e.Header.EventSize > 0 {
				currentLocation.Suffix = 0
				s.isReplacingErr = false
				err = s.streamerController.RedirectStreamer(tctx, currentLocation)
				if err != nil {
					return err
				}
			}
		}

		// check pass SafeModeExitLoc and try disable safe mode, but not in sharding or replacing error
		safeModeExitLoc := s.checkpoint.SafeModeExitPoint()
		if safeModeExitLoc != nil && !s.isReplacingErr && shardingReSync == nil {
			// TODO: for RowsEvent (in fact other than QueryEvent), `currentLocation` is updated in `handleRowsEvent`
			// so here the meaning of `currentLocation` is the location of last event
			if binlog.CompareLocation(currentLocation, *safeModeExitLoc, s.cfg.EnableGTID) > 0 {
				s.checkpoint.SaveSafeModeExitPoint(nil)
				// must flush here to avoid the following situation:
				// 1. quit safe mode
				// 2. push forward and replicate some sqls after safeModeExitPoint to downstream
				// 3. quit because of network error, fail to flush global checkpoint and new safeModeExitPoint to downstream
				// 4. restart again, quit safe mode at safeModeExitPoint, but some sqls after this location have already been replicated to the downstream
				if err = s.checkpoint.FlushSafeModeExitPoint(s.tctx); err != nil {
					return err
				}
				if err = s.safeMode.Add(tctx, -1); err != nil {
					return err
				}
			}
		}

		ec := eventContext{
			tctx:                tctx,
			header:              e.Header,
			startLocation:       &startLocation,
			currentLocation:     &currentLocation,
			lastLocation:        &lastLocation,
			shardingReSync:      shardingReSync,
			closeShardingResync: closeShardingResync,
			traceSource:         traceSource,
			safeMode:            s.safeMode.Enable(),
			tryReSync:           tryReSync,
			startTime:           startTime,
			shardingReSyncCh:    &shardingReSyncCh,
		}

		var originSQL string // show origin sql when error, only ddl now
		var err2 error

		switch ev := e.Event.(type) {
		case *replication.RotateEvent:
			err2 = s.handleRotateEvent(ev, ec)
		case *replication.RowsEvent:
			eventIndex++
			metrics.BinlogEventRowHistogram.WithLabelValues(s.cfg.WorkerName, s.cfg.Name, s.cfg.SourceID).Observe(float64(len(ev.Rows)))
			err2 = s.handleRowsEvent(ev, ec)
		case *replication.QueryEvent:
			originSQL = strings.TrimSpace(string(ev.Query))
			err2 = s.handleQueryEvent(ev, ec, originSQL)
		case *replication.XIDEvent:
			// reset eventIndex and force safeMode flag here.
			eventIndex = 0
			if shardingReSync != nil {
				shardingReSync.currLocation.Position.Pos = e.Header.LogPos
				shardingReSync.currLocation.Suffix = currentLocation.Suffix
				err = shardingReSync.currLocation.SetGTID(ev.GSet)
				if err != nil {
					return terror.Annotatef(err, "fail to record GTID %v", ev.GSet)
				}

				// only need compare binlog position?
				lastLocation = shardingReSync.currLocation
				if binlog.CompareLocation(shardingReSync.currLocation, shardingReSync.latestLocation, s.cfg.EnableGTID) >= 0 {
					tctx.L().Info("re-replicate shard group was completed", zap.String("event", "XID"), zap.Stringer("re-shard", shardingReSync))
					err = closeShardingResync()
					if err != nil {
						return terror.Annotatef(err, "shard group current location %s", shardingReSync.currLocation)
					}
					continue
				}
			}

			currentLocation.Position.Pos = e.Header.LogPos
			err = currentLocation.SetGTID(ev.GSet)
			if err != nil {
				return terror.Annotatef(err, "fail to record GTID %v", ev.GSet)
			}

			tctx.L().Debug("", zap.String("event", "XID"), zap.Stringer("last location", lastLocation), log.WrapStringerField("location", currentLocation))
			lastLocation.Position.Pos = e.Header.LogPos // update lastPos
			err = lastLocation.SetGTID(ev.GSet)
			if err != nil {
				return terror.Annotatef(err, "fail to record GTID %v", ev.GSet)
			}

			job := newXIDJob(currentLocation, startLocation, currentLocation)
			err2 = s.addJobFunc(job)
		case *replication.GenericEvent:
			if e.Header.EventType == replication.HEARTBEAT_EVENT {
				// flush checkpoint even if there are no real binlog events
				if s.checkpoint.CheckGlobalPoint() {
					tctx.L().Info("meet heartbeat event and then flush jobs")
					err2 = s.flushJobs()
				}
			}
		}
		if err2 != nil {
			if err := s.handleEventError(err2, startLocation, currentLocation, e.Header.EventType == replication.QUERY_EVENT, originSQL); err != nil {
				return err
			}
		}
		if waitXIDStatus(s.waitXIDJob.Load()) == waitComplete {
			return nil
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
	closeShardingResync func() error
	traceSource         string
	// safeMode is the value of syncer.safeMode when process this event
	// syncer.safeMode's value may change on the fly, e.g. after event by pass the safeModeExitPoint
	safeMode         bool
	tryReSync        bool
	startTime        time.Time
	shardingReSyncCh *chan *ShardingReSync
}

// TODO: Further split into smaller functions and group common arguments into a context struct.
func (s *Syncer) handleRotateEvent(ev *replication.RotateEvent, ec eventContext) error {
	failpoint.Inject("MakeFakeRotateEvent", func(val failpoint.Value) {
		ec.header.LogPos = 0
		ev.NextLogName = []byte(val.(string))
		ec.tctx.L().Info("MakeFakeRotateEvent", zap.String("fake file name", string(ev.NextLogName)))
	})

	if utils.IsFakeRotateEvent(ec.header) {
		if fileName := string(ev.NextLogName); mysql.CompareBinlogFileName(fileName, ec.lastLocation.Position.Name) <= 0 {
			// NOTE A fake rotate event is also generated when a master-slave switch occurs upstream, and the binlog filename may be rolled back in this case
			// when the DM is updating based on the GTID, we also update the filename of the lastLocation
			if s.cfg.EnableGTID {
				ec.lastLocation.Position.Name = fileName
			}
			return nil // not rotate to the next binlog file, ignore it
		}
		// when user starts a new task with GTID and no binlog file name, we can't know active relay log at init time
		// at this case, we update active relay log when receive fake rotate event
		if !s.recordedActiveRelayLog {
			if err := s.updateActiveRelayLog(mysql.Position{
				Name: string(ev.NextLogName),
				Pos:  uint32(ev.Position),
			}); err != nil {
				ec.tctx.L().Warn("failed to update active relay log, will try to update when flush checkpoint",
					zap.ByteString("NextLogName", ev.NextLogName),
					zap.Uint64("Position", ev.Position),
					zap.Error(err))
			} else {
				s.recordedActiveRelayLog = true
			}
		}
	}

	*ec.currentLocation = binlog.InitLocation(
		mysql.Position{
			Name: string(ev.NextLogName),
			Pos:  uint32(ev.Position),
		},
		ec.currentLocation.GetGTID(),
	)

	if binlog.CompareLocation(*ec.currentLocation, *ec.lastLocation, s.cfg.EnableGTID) >= 0 {
		*ec.lastLocation = *ec.currentLocation
	}

	if ec.shardingReSync != nil {
		if binlog.CompareLocation(*ec.currentLocation, ec.shardingReSync.currLocation, s.cfg.EnableGTID) > 0 {
			ec.shardingReSync.currLocation = *ec.currentLocation
		}

		if binlog.CompareLocation(ec.shardingReSync.currLocation, ec.shardingReSync.latestLocation, s.cfg.EnableGTID) >= 0 {
			ec.tctx.L().Info("re-replicate shard group was completed", zap.String("event", "rotate"), zap.Stringer("re-shard", ec.shardingReSync))
			err := ec.closeShardingResync()
			if err != nil {
				return err
			}
		} else {
			ec.tctx.L().Debug("re-replicate shard group", zap.String("event", "rotate"), log.WrapStringerField("location", ec.currentLocation), zap.Reflect("re-shard", ec.shardingReSync))
		}
		return nil
	}

	ec.tctx.L().Info("", zap.String("event", "rotate"), log.WrapStringerField("location", ec.currentLocation))
	return nil
}

func (s *Syncer) handleRowsEvent(ev *replication.RowsEvent, ec eventContext) error {
	originTable := &filter.Table{
		Schema: string(ev.Table.Schema),
		Name:   string(ev.Table.Table),
	}
	targetTable := s.route(originTable)

	*ec.currentLocation = binlog.InitLocation(
		mysql.Position{
			Name: ec.lastLocation.Position.Name,
			Pos:  ec.header.LogPos,
		},
		ec.lastLocation.GetGTID(),
	)

	if ec.shardingReSync != nil {
		ec.shardingReSync.currLocation = *ec.currentLocation
		if binlog.CompareLocation(ec.shardingReSync.currLocation, ec.shardingReSync.latestLocation, s.cfg.EnableGTID) >= 0 {
			ec.tctx.L().Info("re-replicate shard group was completed", zap.String("event", "row"), zap.Stringer("re-shard", ec.shardingReSync))
			return ec.closeShardingResync()
		}
		if ec.shardingReSync.targetTable.String() != targetTable.String() {
			// in re-syncing, ignore non current sharding group's events
			ec.tctx.L().Debug("skip event in re-replicating shard group", zap.String("event", "row"), zap.Reflect("re-shard", ec.shardingReSync))
			return nil
		}
	}

	// For DML position before table checkpoint, ignore it. When the position equals to table checkpoint, this event may
	// be partially replicated to downstream, we rely on safe-mode to handle it.
	if s.checkpoint.IsOlderThanTablePoint(originTable, *ec.currentLocation, false) {
		ec.tctx.L().Debug("ignore obsolete event that is old than table checkpoint",
			zap.String("event", "row"),
			log.WrapStringerField("location", ec.currentLocation),
			zap.Stringer("origin table", originTable))
		return nil
	}

	ec.tctx.L().Debug("",
		zap.String("event", "row"),
		zap.Stringer("origin table", originTable),
		zap.Stringer("target table", targetTable),
		log.WrapStringerField("location", ec.currentLocation),
		zap.Reflect("raw event data", ev.Rows))

	// TODO(ehco) remove heartbeat
	if s.cfg.EnableHeartbeat {
		s.heartbeat.TryUpdateTaskTS(s.cfg.Name, originTable.Schema, originTable.Name, ev.Rows)
	}
	// ENDTODO

	ignore, err := s.filterRowsEvent(originTable, ec.header.EventType)
	if err != nil {
		return err
	}
	if ignore {
		metrics.SkipBinlogDurationHistogram.WithLabelValues("rows", s.cfg.Name, s.cfg.SourceID).Observe(time.Since(ec.startTime).Seconds())
		// for RowsEvent, we should record lastLocation rather than currentLocation
		return s.recordSkipSQLsLocation(&ec)
	}

	if s.cfg.ShardMode == config.ShardPessimistic {
		if s.sgk.InSyncing(originTable, targetTable, *ec.currentLocation) {
			// if in unsync stage and not before active DDL, ignore it
			// if in sharding re-sync stage and not before active DDL (the next DDL to be synced), ignore it
			ec.tctx.L().Debug("replicate sharding DDL, ignore Rows event",
				zap.String("event", "row"),
				zap.Stringer("source", originTable),
				log.WrapStringerField("location", ec.currentLocation))
			return nil
		}
	}

	// TODO(csuzhangxc): check performance of `getTable` from schema tracker.
	tableInfo, err := s.getTableInfo(ec.tctx, originTable, targetTable)
	if err != nil {
		return terror.WithScope(err, terror.ScopeDownstream)
	}
	rows, err := s.mappingDML(originTable, tableInfo, ev.Rows)
	if err != nil {
		return err
	}
	if err2 := checkLogColumns(ev.SkippedColumns); err2 != nil {
		return err2
	}

	prunedColumns, prunedRows, err := pruneGeneratedColumnDML(tableInfo, rows)
	if err != nil {
		return err
	}

	var (
		sqls    []string
		keys    [][]string
		args    [][]interface{}
		jobType opType
	)

	param := &genDMLParam{
		tableID:           utils.GenTableID(targetTable),
		data:              prunedRows,
		originalData:      rows,
		columns:           prunedColumns,
		originalTableInfo: tableInfo,
	}

	switch ec.header.EventType {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		exprFilter, err2 := s.exprFilterGroup.GetInsertExprs(originTable, tableInfo)
		if err2 != nil {
			return err2
		}

		param.safeMode = ec.safeMode
		sqls, keys, args, err = s.genInsertSQLs(param, exprFilter)
		if err != nil {
			return terror.Annotatef(err, "gen insert sqls failed, originTable: %v, targetTable: %v", originTable, targetTable)
		}
		metrics.BinlogEventCost.WithLabelValues(metrics.BinlogEventCostStageGenWriteRows, s.cfg.Name, s.cfg.WorkerName, s.cfg.SourceID).Observe(time.Since(ec.startTime).Seconds())
		jobType = insert

	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		oldExprFilter, newExprFilter, err2 := s.exprFilterGroup.GetUpdateExprs(originTable, tableInfo)
		if err2 != nil {
			return err2
		}

		param.safeMode = ec.safeMode
		sqls, keys, args, err = s.genUpdateSQLs(param, oldExprFilter, newExprFilter)
		if err != nil {
			return terror.Annotatef(err, "gen update sqls failed, originTable: %v, targetTable: %v", originTable, targetTable)
		}
		metrics.BinlogEventCost.WithLabelValues(metrics.BinlogEventCostStageGenUpdateRows, s.cfg.Name, s.cfg.WorkerName, s.cfg.SourceID).Observe(time.Since(ec.startTime).Seconds())
		jobType = update

	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		exprFilter, err2 := s.exprFilterGroup.GetDeleteExprs(originTable, tableInfo)
		if err2 != nil {
			return err2
		}

		sqls, keys, args, err = s.genDeleteSQLs(param, exprFilter)
		if err != nil {
			return terror.Annotatef(err, "gen delete sqls failed, originTable: %v, targetTable: %v", originTable, targetTable)
		}
		metrics.BinlogEventCost.WithLabelValues(metrics.BinlogEventCostStageGenDeleteRows, s.cfg.Name, s.cfg.WorkerName, s.cfg.SourceID).Observe(time.Since(ec.startTime).Seconds())
		jobType = del

	default:
		ec.tctx.L().Debug("ignoring unrecognized event", zap.String("event", "row"), zap.Stringer("type", ec.header.EventType))
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
		err = s.commitJob(jobType, originTable, targetTable, sqls[i], arg, key, &ec)
		if err != nil {
			return err
		}
	}
	metrics.DispatchBinlogDurationHistogram.WithLabelValues(jobType.String(), s.cfg.Name, s.cfg.SourceID).Observe(time.Since(startTime).Seconds())
	return nil
}

type queryEventContext struct {
	*eventContext

	p         *parser.Parser // used parser
	ddlSchema string         // used schema
	originSQL string         // before split
	// split multi-schema change DDL into multiple one schema change DDL due to TiDB's limitation
	splitedDDLs    []string // after split before online ddl
	appliedDDLs    []string // after onlineDDL apply if onlineDDL != nil and track, before route
	needHandleDDLs []string // after route

	ddlInfo        *shardingDDLInfo
	needTrackDDLs  []trackedDDL
	sourceTbls     map[string]map[string]struct{} // db name -> tb name
	onlineDDLTable *filter.Table
}

func (qec *queryEventContext) String() string {
	var startLocation, currentLocation, lastLocation string
	if qec.startLocation != nil {
		startLocation = qec.startLocation.String()
	}
	if qec.currentLocation != nil {
		currentLocation = qec.currentLocation.String()
	}
	if qec.lastLocation != nil {
		lastLocation = qec.lastLocation.String()
	}
	var needHandleDDLs, shardingReSync string
	if qec.needHandleDDLs != nil {
		needHandleDDLs = strings.Join(qec.needHandleDDLs, ",")
	}
	if qec.shardingReSync != nil {
		shardingReSync = qec.shardingReSync.String()
	}
	return fmt.Sprintf("{schema: %s, originSQL: %s, startLocation: %s, currentLocation: %s, lastLocation: %s, re-sync: %s, needHandleDDLs: %s}",
		qec.ddlSchema, qec.originSQL, startLocation, currentLocation, lastLocation, shardingReSync, needHandleDDLs)
}

func (s *Syncer) handleQueryEvent(ev *replication.QueryEvent, ec eventContext, originSQL string) (err error) {
	if originSQL == "BEGIN" {
		// GTID event: GTID_NEXT = xxx:11
		// Query event: BEGIN (GTID set = xxx:1-11)
		// Rows event: ... (GTID set = xxx:1-11)  if we update lastLocation below,
		//                                        otherwise that is xxx:1-10 when dealing with table checkpoints
		// Xid event: GTID set = xxx:1-11  this event is related to global checkpoint
		*ec.lastLocation = *ec.currentLocation
		return nil
	}

	qec := &queryEventContext{
		eventContext: &ec,
		ddlSchema:    string(ev.Schema),
		originSQL:    utils.TrimCtrlChars(originSQL),
		splitedDDLs:  make([]string, 0),
		appliedDDLs:  make([]string, 0),
		sourceTbls:   make(map[string]map[string]struct{}),
	}
	qec.p, err = event.GetParserForStatusVars(ev.StatusVars)
	if err != nil {
		log.L().Warn("found error when get sql_mode from binlog status_vars", zap.Error(err))
	}

	stmt, err := s.parseDDLSQL(qec)
	if err != nil {
		ec.tctx.L().Error("fail to parse statement",
			zap.String("event", "query"),
			zap.String("statement", qec.originSQL),
			zap.String("schema", qec.ddlSchema),
			zap.Stringer("last location", ec.lastLocation),
			log.WrapStringerField("location", ec.currentLocation),
			log.ShortError(err))
		// return error if parse fail and filter fail
		needSkip, err2 := s.filterSQL(qec.originSQL)
		if err2 != nil {
			return err2
		}
		if !needSkip {
			return err
		}
		// don't return error if parse fail and filter success
		metrics.SkipBinlogDurationHistogram.WithLabelValues("query", s.cfg.Name, s.cfg.SourceID).Observe(time.Since(ec.startTime).Seconds())
		ec.tctx.L().Warn("skip event", zap.String("event", "query"), zap.String("statement", qec.originSQL), zap.String("schema", qec.ddlSchema))
		*ec.lastLocation = *ec.currentLocation // before record skip location, update lastLocation
		// Q&A: If parse fail, ok must be false. Can we delete it?
		// Q&A: Former logic will postpone to filterAndApply
		return s.recordSkipSQLsLocation(&ec)
	}

	if node, ok := stmt.(ast.DMLNode); ok {
		// if DML can be ignored, we do not report an error
		table, err2 := getTableByDML(node)
		if err2 == nil {
			if len(table.Schema) == 0 {
				table.Schema = qec.ddlSchema
			}
			ignore, err2 := s.filterRowsEvent(table, replication.QUERY_EVENT)
			if err2 == nil && ignore {
				return nil
			}
		}
		return terror.Annotatef(terror.ErrSyncUnitDMLStatementFound.Generate(), "query %s", qec.originSQL)
	}

	if qec.shardingReSync != nil {
		qec.shardingReSync.currLocation = *qec.currentLocation
		if binlog.CompareLocation(qec.shardingReSync.currLocation, qec.shardingReSync.latestLocation, s.cfg.EnableGTID) >= 0 {
			qec.tctx.L().Info("re-replicate shard group was completed", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))
			err2 := qec.closeShardingResync()
			if err2 != nil {
				return err2
			}
		} else {
			// in re-syncing, we can simply skip all DDLs,
			// as they have been added to sharding DDL sequence
			// only update lastPos when the query is a real DDL
			*qec.lastLocation = qec.shardingReSync.currLocation
			qec.tctx.L().Debug("skip event in re-replicating sharding group", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))
		}
		return nil
	}

	qec.tctx.L().Info("", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))
	*qec.lastLocation = *qec.currentLocation // update lastLocation, because we have checked `isDDL`

	qec.splitedDDLs, err = parserpkg.SplitDDL(stmt, qec.ddlSchema)
	if err != nil {
		return err
	}
	// TiDB can't handle multi schema change DDL, so we split it here.
	// for DDL, we don't apply operator until we try to execute it. so can handle sharding cases
	// We use default parser because inside function where need parser, sqls are came from parserpkg.SplitDDL, which is StringSingleQuotes, KeyWordUppercase and NameBackQuotes
	// TODO: save stmt, tableName to avoid parse the sql to get them again
	qec.p = parser.New()
	for _, sql := range qec.splitedDDLs {
		sqls, err2 := s.processSplitedDDL(qec, sql)
		if err2 != nil {
			qec.tctx.L().Error("fail to split statement", zap.String("event", "query"), zap.Stringer("queryEventContext", qec), log.ShortError(err2))
			return err2
		}
		qec.appliedDDLs = append(qec.appliedDDLs, sqls...)
	}
	qec.tctx.L().Info("resolve sql", zap.String("event", "query"), zap.Strings("appliedDDLs", qec.appliedDDLs), zap.Stringer("queryEventContext", qec))

	metrics.BinlogEventCost.WithLabelValues(metrics.BinlogEventCostStageGenQuery, s.cfg.Name, s.cfg.WorkerName, s.cfg.SourceID).Observe(time.Since(qec.startTime).Seconds())

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

	qec.needHandleDDLs = make([]string, 0, len(qec.appliedDDLs))
	qec.needTrackDDLs = make([]trackedDDL, 0, len(qec.appliedDDLs))

	// handle one-schema change DDL
	for _, sql := range qec.appliedDDLs {
		// We use default parser because sqls are came from above *Syncer.splitAndFilterDDL, which is StringSingleQuotes, KeyWordUppercase and NameBackQuotes
		stmt, err2 := qec.p.ParseOneStmt(sql, "", "")
		if err2 != nil {
			return terror.Annotatef(terror.ErrSyncerUnitParseStmt.New(err2.Error()), "ddl %s", sql)
		}

		originTables, err2 := parserpkg.FetchDDLTables(qec.ddlSchema, stmt, s.SourceTableNamesFlavor)
		if err2 != nil {
			return err2
		}

		routedTables := make([]*filter.Table, 0, len(originTables))
		for i := range originTables {
			routedTable := s.route(originTables[i])
			routedTables = append(routedTables, routedTable)
		}

		sqlDDL, err2 := parserpkg.RenameDDLTable(stmt, routedTables)
		if err2 != nil {
			return err2
		}

		if len(sqlDDL) == 0 {
			metrics.SkipBinlogDurationHistogram.WithLabelValues("query", s.cfg.Name, s.cfg.SourceID).Observe(time.Since(qec.startTime).Seconds())
			qec.tctx.L().Warn("skip event", zap.String("event", "query"), zap.String("statement", sql), zap.String("schema", qec.ddlSchema))
			continue
		}

		// DDL is sequentially synchronized in this syncer's main process goroutine
		// ignore DDL that is older or same as table checkpoint, to avoid sync again for already synced DDLs
		if s.checkpoint.IsOlderThanTablePoint(originTables[0], *qec.currentLocation, true) {
			qec.tctx.L().Info("ignore obsolete DDL", zap.String("event", "query"), zap.String("statement", sql), log.WrapStringerField("location", qec.currentLocation))
			continue
		}

		// pre-filter of sharding
		if s.cfg.ShardMode == config.ShardPessimistic {
			switch stmt.(type) {
			case *ast.DropDatabaseStmt:
				err = s.dropSchemaInSharding(qec.tctx, originTables[0].Schema)
				if err != nil {
					return err
				}
				continue
			case *ast.DropTableStmt:
				sourceTableID := utils.GenTableID(originTables[0])
				err = s.sgk.LeaveGroup(routedTables[0], []string{sourceTableID})
				if err != nil {
					return err
				}
				err = s.checkpoint.DeleteTablePoint(qec.tctx, originTables[0])
				if err != nil {
					return err
				}
				continue
			case *ast.TruncateTableStmt:
				qec.tctx.L().Info("ignore truncate table statement in shard group", zap.String("event", "query"), zap.String("statement", sqlDDL))
				continue
			}

			// in sharding mode, we only support to do one ddl in one event
			if qec.ddlInfo == nil {
				qec.ddlInfo = &shardingDDLInfo{
					name:   originTables[0].String(),
					tables: [][]*filter.Table{originTables, routedTables},
					stmt:   stmt,
				}
			} else if qec.ddlInfo.name != originTables[0].String() {
				return terror.ErrSyncerUnitDDLOnMultipleTable.Generate(string(ev.Query))
			}
		} else if s.cfg.ShardMode == config.ShardOptimistic {
			switch stmt.(type) {
			case *ast.TruncateTableStmt:
				qec.tctx.L().Info("ignore truncate table statement in shard group", zap.String("event", "query"), zap.String("statement", sqlDDL))
				continue
			case *ast.RenameTableStmt:
				return terror.ErrSyncerUnsupportedStmt.Generate("RENAME TABLE", config.ShardOptimistic)
			}
		}

		qec.needHandleDDLs = append(qec.needHandleDDLs, sqlDDL)
		qec.needTrackDDLs = append(qec.needTrackDDLs, trackedDDL{rawSQL: sql, stmt: stmt, tables: [][]*filter.Table{originTables, routedTables}})
		// TODO: current table checkpoints will be deleted in track ddls, but created and updated in flush checkpoints,
		//       we should use a better mechanism to combine these operations
		if s.cfg.ShardMode == "" {
			recordSourceTbls(qec.sourceTbls, stmt, originTables[0])
		}
	}

	qec.tctx.L().Info("prepare to handle ddls", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))
	if len(qec.needHandleDDLs) == 0 {
		qec.tctx.L().Info("skip event, need handled ddls is empty", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))
		return s.recordSkipSQLsLocation(qec.eventContext)
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

	switch s.cfg.ShardMode {
	case "":
		return s.handleQueryEventNoSharding(qec)
	case config.ShardOptimistic:
		return s.handleQueryEventOptimistic(qec)
	case config.ShardPessimistic:
		return s.handleQueryEventPessimistic(qec)
	}
	return errors.Errorf("unsupported shard-mode %s, should not happened", s.cfg.ShardMode)
}

func (s *Syncer) handleQueryEventNoSharding(qec *queryEventContext) error {
	qec.tctx.L().Info("start to handle ddls in normal mode", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))

	// interrupted after flush old checkpoint and before track DDL.
	failpoint.Inject("FlushCheckpointStage", func(val failpoint.Value) {
		err := handleFlushCheckpointStage(1, val.(int), "before track DDL")
		if err != nil {
			failpoint.Return(err)
		}
	})

	// run trackDDL before add ddl job to make sure checkpoint can be flushed
	for _, td := range qec.needTrackDDLs {
		if err := s.trackDDL(qec.ddlSchema, td.rawSQL, td.tables, td.stmt, qec.eventContext); err != nil {
			return err
		}
	}

	// interrupted after track DDL and before execute DDL.
	failpoint.Inject("FlushCheckpointStage", func(val failpoint.Value) {
		err := handleFlushCheckpointStage(2, val.(int), "before execute DDL")
		if err != nil {
			failpoint.Return(err)
		}
	})

	job := newDDLJob(qec)
	err := s.addJobFunc(job)
	if err != nil {
		return err
	}

	// when add ddl job, will execute ddl and then flush checkpoint.
	// if execute ddl failed, the execError will be set to that error.
	// return nil here to avoid duplicate error message
	err = s.execError.Load()
	if err != nil {
		qec.tctx.L().Error("error detected when executing SQL job", log.ShortError(err))
		// nolint:nilerr
		return nil
	}

	qec.tctx.L().Info("finish to handle ddls in normal mode", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))

	qec.tctx.L().Info("finish online ddl and clear online ddl metadata in normal mode",
		zap.String("event", "query"),
		zap.Strings("ddls", qec.needHandleDDLs),
		zap.String("raw statement", qec.originSQL),
		zap.Stringer("table", qec.onlineDDLTable))
	err2 := s.onlineDDL.Finish(qec.tctx, qec.onlineDDLTable)
	if err2 != nil {
		return terror.Annotatef(err2, "finish online ddl on %v", qec.onlineDDLTable)
	}

	return nil
}

func (s *Syncer) handleQueryEventPessimistic(qec *queryEventContext) error {
	var (
		err                error
		needShardingHandle bool
		group              *ShardingGroup
		synced             bool
		active             bool
		remain             int

		sourceTableID  = utils.GenTableID(qec.ddlInfo.tables[0][0])
		needHandleDDLs = qec.needHandleDDLs
		ddlInfo        = qec.ddlInfo
		// for sharding DDL, the firstPos should be the `Pos` of the binlog, not the `End_log_pos`
		// so when restarting before sharding DDLs synced, this binlog can be re-sync again to trigger the TrySync
		startLocation   = qec.startLocation
		currentLocation = qec.currentLocation
	)

	var annotate string
	switch ddlInfo.stmt.(type) {
	case *ast.CreateDatabaseStmt:
		// for CREATE DATABASE, we do nothing. when CREATE TABLE under this DATABASE, sharding groups will be added
	case *ast.CreateTableStmt:
		// for CREATE TABLE, we add it to group
		needShardingHandle, group, synced, remain, err = s.sgk.AddGroup(ddlInfo.tables[1][0], []string{sourceTableID}, nil, true)
		if err != nil {
			return err
		}
		annotate = "add table to shard group"
	default:
		needShardingHandle, group, synced, active, remain, err = s.sgk.TrySync(ddlInfo.tables[0][0], ddlInfo.tables[1][0], *startLocation, *qec.currentLocation, needHandleDDLs)
		if err != nil {
			return err
		}
		annotate = "try to sync table in shard group"
		// meets DDL that will not be processed in sequence sharding
		if !active {
			qec.tctx.L().Info("skip in-activeDDL",
				zap.String("event", "query"),
				zap.Stringer("queryEventContext", qec),
				zap.String("sourceTableID", sourceTableID),
				zap.Bool("in-sharding", needShardingHandle),
				zap.Bool("is-synced", synced),
				zap.Int("unsynced", remain))
			return nil
		}
	}

	qec.tctx.L().Info(annotate,
		zap.String("event", "query"),
		zap.Stringer("queryEventContext", qec),
		zap.String("sourceTableID", sourceTableID),
		zap.Bool("in-sharding", needShardingHandle),
		zap.Bool("is-synced", synced),
		zap.Int("unsynced", remain))

	// interrupted after flush old checkpoint and before track DDL.
	failpoint.Inject("FlushCheckpointStage", func(val failpoint.Value) {
		err = handleFlushCheckpointStage(1, val.(int), "before track DDL")
		if err != nil {
			failpoint.Return(err)
		}
	})

	for _, td := range qec.needTrackDDLs {
		if err = s.trackDDL(qec.ddlSchema, td.rawSQL, td.tables, td.stmt, qec.eventContext); err != nil {
			return err
		}
	}

	if needShardingHandle {
		metrics.UnsyncedTableGauge.WithLabelValues(s.cfg.Name, ddlInfo.tables[1][0].String(), s.cfg.SourceID).Set(float64(remain))
		err = s.safeMode.IncrForTable(qec.tctx, ddlInfo.tables[1][0]) // try enable safe-mode when starting syncing for sharding group
		if err != nil {
			return err
		}

		// save checkpoint in memory, don't worry, if error occurred, we can rollback it
		// for non-last sharding DDL's table, this checkpoint will be used to skip binlog event when re-syncing
		// NOTE: when last sharding DDL executed, all this checkpoints will be flushed in the same txn
		qec.tctx.L().Info("save table checkpoint for source",
			zap.String("event", "query"),
			zap.String("sourceTableID", sourceTableID),
			zap.Stringer("start location", startLocation),
			log.WrapStringerField("end location", currentLocation))
		s.saveTablePoint(ddlInfo.tables[0][0], *currentLocation)
		if !synced {
			qec.tctx.L().Info("source shard group is not synced",
				zap.String("event", "query"),
				zap.String("sourceTableID", sourceTableID),
				zap.Stringer("start location", startLocation),
				log.WrapStringerField("end location", currentLocation))
			return nil
		}

		qec.tctx.L().Info("source shard group is synced",
			zap.String("event", "query"),
			zap.String("sourceTableID", sourceTableID),
			zap.Stringer("start location", startLocation),
			log.WrapStringerField("end location", currentLocation))
		err = s.safeMode.DescForTable(qec.tctx, ddlInfo.tables[1][0]) // try disable safe-mode after sharding group synced
		if err != nil {
			return err
		}
		// maybe multi-groups' sharding DDL synced in this for-loop (one query-event, multi tables)
		if cap(*qec.shardingReSyncCh) < len(needHandleDDLs) {
			*qec.shardingReSyncCh = make(chan *ShardingReSync, len(needHandleDDLs))
		}
		firstEndLocation := group.FirstEndPosUnresolved()
		if firstEndLocation == nil {
			return terror.ErrSyncerUnitFirstEndPosNotFound.Generate(sourceTableID)
		}

		allResolved, err2 := s.sgk.ResolveShardingDDL(ddlInfo.tables[1][0])
		if err2 != nil {
			return err2
		}
		*qec.shardingReSyncCh <- &ShardingReSync{
			currLocation:   *firstEndLocation,
			latestLocation: *currentLocation,
			targetTable:    ddlInfo.tables[1][0],
			allResolved:    allResolved,
		}

		// Don't send new DDLInfo to dm-master until all local sql jobs finished
		// since jobWg is flushed by flushJobs before, we don't wait here any more

		// NOTE: if we need singleton Syncer (without dm-master) to support sharding DDL sync
		// we should add another config item to differ, and do not save DDLInfo, and not wait for ddlExecInfo

		// construct & send shard DDL info into etcd, DM-master will handle it.
		shardInfo := s.pessimist.ConstructInfo(ddlInfo.tables[1][0].Schema, ddlInfo.tables[1][0].Name, needHandleDDLs)
		rev, err2 := s.pessimist.PutInfo(qec.tctx.Ctx, shardInfo)
		if err2 != nil {
			return err2
		}
		metrics.ShardLockResolving.WithLabelValues(s.cfg.Name, s.cfg.SourceID).Set(1) // block and wait DDL lock to be synced
		qec.tctx.L().Info("putted shard DDL info", zap.Stringer("info", shardInfo), zap.Int64("revision", rev))

		shardOp, err2 := s.pessimist.GetOperation(qec.tctx.Ctx, shardInfo, rev+1)
		metrics.ShardLockResolving.WithLabelValues(s.cfg.Name, s.cfg.SourceID).Set(0)
		if err2 != nil {
			return err2
		}

		if shardOp.Exec {
			failpoint.Inject("ShardSyncedExecutionExit", func() {
				qec.tctx.L().Warn("exit triggered", zap.String("failpoint", "ShardSyncedExecutionExit"))
				//nolint:errcheck
				s.flushCheckPoints()
				utils.OsExit(1)
			})
			failpoint.Inject("SequenceShardSyncedExecutionExit", func() {
				group := s.sgk.Group(ddlInfo.tables[1][0])
				if group != nil {
					// exit in the first round sequence sharding DDL only
					if group.meta.ActiveIdx() == 1 {
						qec.tctx.L().Warn("exit triggered", zap.String("failpoint", "SequenceShardSyncedExecutionExit"))
						//nolint:errcheck
						s.flushCheckPoints()
						utils.OsExit(1)
					}
				}
			})

			qec.tctx.L().Info("execute DDL job",
				zap.String("event", "query"),
				zap.Stringer("queryEventContext", qec),
				zap.String("sourceTableID", sourceTableID),
				zap.Stringer("operation", shardOp))
		} else {
			qec.tctx.L().Info("ignore DDL job",
				zap.String("event", "query"),
				zap.Stringer("queryEventContext", qec),
				zap.String("sourceTableID", sourceTableID),
				zap.Stringer("operation", shardOp))
		}
	}

	qec.tctx.L().Info("start to handle ddls in shard mode", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))

	// interrupted after track DDL and before execute DDL.
	failpoint.Inject("FlushCheckpointStage", func(val failpoint.Value) {
		err = handleFlushCheckpointStage(2, val.(int), "before execute DDL")
		if err != nil {
			failpoint.Return(err)
		}
	})

	job := newDDLJob(qec)
	err = s.addJobFunc(job)
	if err != nil {
		return err
	}

	err = s.execError.Load()
	if err != nil {
		qec.tctx.L().Error("error detected when executing SQL job", log.ShortError(err))
		// nolint:nilerr
		return nil
	}

	if qec.onlineDDLTable != nil {
		err = s.clearOnlineDDL(qec.tctx, ddlInfo.tables[1][0])
		if err != nil {
			return err
		}
	}

	qec.tctx.L().Info("finish to handle ddls in shard mode", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))
	return nil
}

// input `sql` should be a single DDL, which came from parserpkg.SplitDDL
// tableNames[0] is source (upstream) tableNames, tableNames[1] is target (downstream) tableNames.
func (s *Syncer) trackDDL(usedSchema string, sql string, tableNames [][]*filter.Table, stmt ast.StmtNode, ec *eventContext) error {
	srcTables, targetTables := tableNames[0], tableNames[1]
	srcTable := srcTables[0]

	// Make sure the needed tables are all loaded into the schema tracker.
	var (
		shouldExecDDLOnSchemaTracker bool
		shouldSchemaExist            bool
		shouldTableExistNum          int  // tableNames[:shouldTableExistNum] should exist
		shouldRefTableExistNum       int  // tableNames[1:shouldTableExistNum] should exist, since first one is "caller table"
		tryFetchDownstreamTable      bool // to make sure if not exists will execute correctly
	)

	switch node := stmt.(type) {
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
	case *ast.RecoverTableStmt:
		shouldExecDDLOnSchemaTracker = true
		shouldSchemaExist = true
	case *ast.CreateTableStmt, *ast.CreateViewStmt:
		shouldExecDDLOnSchemaTracker = true
		shouldSchemaExist = true
		// for CREATE TABLE LIKE/AS, the reference tables should exist
		shouldRefTableExistNum = len(srcTables)
		tryFetchDownstreamTable = true
	case *ast.DropTableStmt:
		shouldExecDDLOnSchemaTracker = true
		if err := s.checkpoint.DeleteTablePoint(ec.tctx, srcTable); err != nil {
			return err
		}
	case *ast.RenameTableStmt, *ast.CreateIndexStmt, *ast.DropIndexStmt, *ast.RepairTableStmt:
		shouldExecDDLOnSchemaTracker = true
		shouldSchemaExist = true
		shouldTableExistNum = 1
	case *ast.AlterTableStmt:
		shouldSchemaExist = true
		// for DDL that adds FK, since TiDB doesn't fully support it yet, we simply ignore execution of this DDL.
		switch {
		case len(node.Specs) == 1 && node.Specs[0].Constraint != nil && node.Specs[0].Constraint.Tp == ast.ConstraintForeignKey:
			shouldTableExistNum = 1
			shouldExecDDLOnSchemaTracker = false
		case node.Specs[0].Tp == ast.AlterTableRenameTable:
			shouldTableExistNum = 1
			shouldExecDDLOnSchemaTracker = true
		default:
			shouldTableExistNum = len(srcTables)
			shouldExecDDLOnSchemaTracker = true
		}
	case *ast.LockTablesStmt, *ast.UnlockTablesStmt, *ast.CleanupTableLockStmt, *ast.TruncateTableStmt:
		break
	default:
		ec.tctx.L().DPanic("unhandled DDL type cannot be tracked", zap.Stringer("type", reflect.TypeOf(stmt)))
	}

	if shouldSchemaExist {
		if err := s.schemaTracker.CreateSchemaIfNotExists(srcTable.Schema); err != nil {
			return terror.ErrSchemaTrackerCannotCreateSchema.Delegate(err, srcTable.Schema)
		}
	}
	for i := 0; i < shouldTableExistNum; i++ {
		if _, err := s.getTableInfo(ec.tctx, srcTables[i], targetTables[i]); err != nil {
			return err
		}
	}
	// skip getTable before in above loop
	// nolint:ifshort
	start := 1
	if shouldTableExistNum > start {
		start = shouldTableExistNum
	}
	for i := start; i < shouldRefTableExistNum; i++ {
		if err := s.schemaTracker.CreateSchemaIfNotExists(srcTables[i].Schema); err != nil {
			return terror.ErrSchemaTrackerCannotCreateSchema.Delegate(err, srcTables[i].Schema)
		}
		if _, err := s.getTableInfo(ec.tctx, srcTables[i], targetTables[i]); err != nil {
			return err
		}
	}

	if tryFetchDownstreamTable {
		// ignore table not exists error, just try to fetch table from downstream.
		_, _ = s.getTableInfo(ec.tctx, srcTables[0], targetTables[0])
	}

	if shouldExecDDLOnSchemaTracker {
		if err := s.schemaTracker.Exec(ec.tctx.Ctx, usedSchema, sql); err != nil {
			ec.tctx.L().Error("cannot track DDL", zap.String("schema", usedSchema), zap.String("statement", sql), log.WrapStringerField("location", ec.currentLocation), log.ShortError(err))
			return terror.ErrSchemaTrackerCannotExecDDL.Delegate(err, sql)
		}
		s.exprFilterGroup.ResetExprs(srcTable)
	}

	return nil
}

func (s *Syncer) commitJob(tp opType, sourceTable, targetTable *filter.Table, sql string, args []interface{}, keys []string, ec *eventContext) error {
	startTime := time.Now()
	key, err := s.resolveCasuality(keys)
	if err != nil {
		return terror.ErrSyncerUnitResolveCasualityFail.Generate(err)
	}
	s.tctx.L().Debug("key for keys", zap.String("key", key), zap.Strings("keys", keys))
	metrics.ConflictDetectDurationHistogram.WithLabelValues(s.cfg.Name, s.cfg.SourceID).Observe(time.Since(startTime).Seconds())

	job := newDMLJob(tp, sql, sourceTable, targetTable, args, key, *ec.lastLocation, *ec.startLocation, *ec.currentLocation, ec.header)
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

//nolint:unused
func (s *Syncer) loadTableStructureFromDump(ctx context.Context) error {
	logger := s.tctx.L()

	files, err := utils.CollectDirFiles(s.cfg.Dir)
	if err != nil {
		logger.Warn("fail to get dump files", zap.Error(err))
		return err
	}
	var dbs, tables []string
	var tableFiles [][2]string // [db, filename]
	for f := range files {
		if db, ok := utils.GetDBFromDumpFilename(f); ok {
			dbs = append(dbs, db)
			continue
		}
		if db, table, ok := utils.GetTableFromDumpFilename(f); ok {
			tables = append(tables, dbutil.TableName(db, table))
			tableFiles = append(tableFiles, [2]string{db, f})
			continue
		}
	}
	logger.Info("fetch table structure form dump files",
		zap.Strings("database", dbs),
		zap.Any("tables", tables))
	for _, db := range dbs {
		if err = s.schemaTracker.CreateSchemaIfNotExists(db); err != nil {
			return err
		}
	}

	var firstErr error
	setFirstErr := func(err error) {
		if firstErr == nil {
			firstErr = err
		}
	}

	for _, dbAndFile := range tableFiles {
		db, file := dbAndFile[0], dbAndFile[1]
		filepath := path.Join(s.cfg.Dir, file)
		content, err2 := common2.GetFileContent(filepath)
		if err2 != nil {
			logger.Warn("fail to read file for creating table in schema tracker",
				zap.String("db", db),
				zap.String("file", filepath),
				zap.Error(err))
			setFirstErr(err2)
			continue
		}
		stmts := bytes.Split(content, []byte(";"))
		for _, stmt := range stmts {
			stmt = bytes.TrimSpace(stmt)
			if len(stmt) == 0 || bytes.HasPrefix(stmt, []byte("/*")) {
				continue
			}
			err = s.schemaTracker.Exec(ctx, db, string(stmt))
			if err != nil {
				logger.Warn("fail to create table for dump files",
					zap.Any("file", filepath),
					zap.ByteString("statement", stmt),
					zap.Error(err))
				setFirstErr(err)
			}
		}
	}
	return firstErr
}

func (s *Syncer) createDBs(ctx context.Context) error {
	var err error
	dbCfg := s.cfg.From
	dbCfg.RawDBCfg = config.DefaultRawDBConfig().SetReadTimeout(maxDMLConnectionTimeout)
	s.fromDB, err = dbconn.NewUpStreamConn(dbCfg)
	if err != nil {
		return err
	}
	conn, err := s.fromDB.BaseDB.GetBaseConn(ctx)
	if err != nil {
		return err
	}
	lcFlavor, err := utils.FetchLowerCaseTableNamesSetting(ctx, conn.DBConn)
	if err != nil {
		return err
	}
	s.SourceTableNamesFlavor = lcFlavor

	hasSQLMode := false
	// get sql_mode from upstream db
	if s.cfg.To.Session == nil {
		s.cfg.To.Session = make(map[string]string)
	} else {
		for k := range s.cfg.To.Session {
			if strings.ToLower(k) == "sql_mode" {
				hasSQLMode = true
				break
			}
		}
	}
	if !hasSQLMode {
		sqlMode, err2 := utils.GetGlobalVariable(ctx, s.fromDB.BaseDB.DB, "sql_mode")
		if err2 != nil {
			s.tctx.L().Warn("cannot get sql_mode from upstream database, the sql_mode will be assigned \"IGNORE_SPACE, NO_AUTO_VALUE_ON_ZERO, ALLOW_INVALID_DATES\"", log.ShortError(err2))
		}
		sqlModes, err3 := utils.AdjustSQLModeCompatible(sqlMode)
		if err3 != nil {
			s.tctx.L().Warn("cannot adjust sql_mode compatible, the sql_mode will be assigned  stay the same", log.ShortError(err3))
		}
		s.cfg.To.Session["sql_mode"] = sqlModes
	}

	dbCfg = s.cfg.To
	dbCfg.RawDBCfg = config.DefaultRawDBConfig().
		SetReadTimeout(maxDMLConnectionTimeout).
		SetMaxIdleConns(s.cfg.WorkerCount)

	s.toDB, s.toDBConns, err = dbconn.CreateConns(s.tctx, s.cfg, dbCfg, s.cfg.WorkerCount)
	if err != nil {
		dbconn.CloseUpstreamConn(s.tctx, s.fromDB) // release resources acquired before return with error
		return err
	}
	// baseConn for ddl
	dbCfg = s.cfg.To
	dbCfg.RawDBCfg = config.DefaultRawDBConfig().SetReadTimeout(maxDDLConnectionTimeout)

	var ddlDBConns []*dbconn.DBConn
	s.ddlDB, ddlDBConns, err = dbconn.CreateConns(s.tctx, s.cfg, dbCfg, 1)
	if err != nil {
		dbconn.CloseUpstreamConn(s.tctx, s.fromDB)
		dbconn.CloseBaseDB(s.tctx, s.toDB)
		return err
	}
	s.ddlDBConn = ddlDBConns[0]
	printServerVersion(s.tctx, s.fromDB.BaseDB, "upstream")
	printServerVersion(s.tctx, s.toDB, "downstream")

	return nil
}

// closeBaseDB closes all opened DBs, rollback for createConns.
func (s *Syncer) closeDBs() {
	dbconn.CloseUpstreamConn(s.tctx, s.fromDB)
	dbconn.CloseBaseDB(s.tctx, s.toDB)
	dbconn.CloseBaseDB(s.tctx, s.ddlDB)
}

// record skip ddl/dml sqls' position
// make newJob's sql argument empty to distinguish normal sql and skips sql.
func (s *Syncer) recordSkipSQLsLocation(ec *eventContext) error {
	job := newSkipJob(ec)
	return s.addJobFunc(job)
}

func (s *Syncer) flushJobs() error {
	s.tctx.L().Info("flush all jobs", zap.Stringer("global checkpoint", s.checkpoint))
	job := newFlushJob()
	return s.addJobFunc(job)
}

func (s *Syncer) reSyncBinlog(tctx tcontext.Context, location binlog.Location) error {
	if err := s.retrySyncGTIDs(); err != nil {
		return err
	}
	// close still running sync
	return s.streamerController.ReopenWithRetry(&tctx, location)
}

func (s *Syncer) route(table *filter.Table) *filter.Table {
	if table.Schema == "" {
		return table
	}
	targetSchema, targetTable, err := s.tableRouter.Route(table.Schema, table.Name)
	if err != nil {
		s.tctx.L().Error("fail to route table", zap.Stringer("table", table), zap.Error(err)) // log the error, but still continue
	}
	if targetSchema == "" {
		return table
	}
	if targetTable == "" {
		targetTable = table.Name
	}

	return &filter.Table{Schema: targetSchema, Name: targetTable}
}

func (s *Syncer) isClosed() bool {
	return s.closed.Load()
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

	metrics.RemoveLabelValuesWithTaskInMetrics(s.cfg.Name)

	s.closed.Store(true)
}

// stopSync stops syncing, now it used by Close and Pause
// maybe we can refine the workflow more clear.
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
		err := s.heartbeat.RemoveTask(s.cfg.Name)
		if err != nil {
			s.tctx.L().Error("fail to remove task for heartbeat", zap.Error(err))
		}
	}
}

// Pause implements Unit.Pause.
func (s *Syncer) Pause() {
	if s.isClosed() {
		s.tctx.L().Warn("try to pause, but already closed")
		return
	}
	s.stopSync()
}

// Resume resumes the paused process.
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
// now no config diff implemented, so simply re-init use new config.
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

		err = s.initShardingGroups(context.Background(), false) // FIXME: fix context when re-implementing `Update`
		if err != nil {
			return err
		}
	case config.ShardOptimistic:
		err = s.initOptimisticShardDDL(context.Background()) // FIXME: fix context when re-implementing `Update`
		if err != nil {
			return err
		}
	}

	// update l.cfg
	s.cfg.BAList = cfg.BAList
	s.cfg.RouteRules = cfg.RouteRules
	s.cfg.FilterRules = cfg.FilterRules
	s.cfg.ColumnMappingRules = cfg.ColumnMappingRules

	// update timezone
	s.setTimezone()

	return nil
}

// assume that reset master before switching to new master, and only the new master would write
// it's a weak function to try best to fix gtid set while switching master/slave.
func (s *Syncer) retrySyncGTIDs() error {
	// NOTE: our (per-table based) checkpoint does not support GTID yet, implement it if needed
	// TODO: support GTID
	s.tctx.L().Warn("our (per-table based) checkpoint does not support GTID yet")
	return nil
}

// checkpointID returns ID which used for checkpoint table.
func (s *Syncer) checkpointID() string {
	if len(s.cfg.SourceID) > 0 {
		return s.cfg.SourceID
	}
	return strconv.FormatUint(uint64(s.cfg.ServerID), 10)
}

// UpdateFromConfig updates config for `From`.
func (s *Syncer) UpdateFromConfig(cfg *config.SubTaskConfig) error {
	s.Lock()
	defer s.Unlock()
	s.fromDB.BaseDB.Close()

	s.cfg.From = cfg.From

	var err error
	s.cfg.From.RawDBCfg = config.DefaultRawDBConfig().SetReadTimeout(maxDMLConnectionTimeout)
	s.fromDB, err = dbconn.NewUpStreamConn(s.cfg.From)
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

func (s *Syncer) setTimezone() {
	s.tctx.L().Info("use timezone", log.WrapStringerField("location", time.UTC))
	s.timezone = time.UTC
}

func (s *Syncer) setSyncCfg() error {
	var tlsConfig *tls.Config
	var err error
	if s.cfg.From.Security != nil {
		if loadErr := s.cfg.From.Security.LoadTLSContent(); loadErr != nil {
			return terror.ErrCtlLoadTLSCfg.Delegate(loadErr)
		}
		tlsConfig, err = toolutils.ToTLSConfigWithVerifyByRawbytes(s.cfg.From.Security.SSLCABytes,
			s.cfg.From.Security.SSLCertBytes, s.cfg.From.Security.SSLKEYBytes, s.cfg.From.Security.CertAllowedCN)
		if err != nil {
			return terror.ErrConnInvalidTLSConfig.Delegate(err)
		}
		if tlsConfig != nil {
			tlsConfig.InsecureSkipVerify = true
		}
	}

	syncCfg := replication.BinlogSyncerConfig{
		ServerID:                s.cfg.ServerID,
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

// ShardDDLOperation returns the current pending to handle shard DDL lock operation.
func (s *Syncer) ShardDDLOperation() *pessimism.Operation {
	return s.pessimist.PendingOperation()
}

func (s *Syncer) setErrLocation(startLocation, endLocation *binlog.Location, isQueryEventEvent bool) {
	s.errLocation.Lock()
	defer s.errLocation.Unlock()

	s.errLocation.isQueryEvent = isQueryEventEvent
	if s.errLocation.startLocation == nil || startLocation == nil {
		s.errLocation.startLocation = startLocation
	} else if binlog.CompareLocation(*startLocation, *s.errLocation.startLocation, s.cfg.EnableGTID) < 0 {
		s.errLocation.startLocation = startLocation
	}

	if s.errLocation.endLocation == nil || endLocation == nil {
		s.errLocation.endLocation = endLocation
	} else if binlog.CompareLocation(*endLocation, *s.errLocation.endLocation, s.cfg.EnableGTID) < 0 {
		s.errLocation.endLocation = endLocation
	}
}

func (s *Syncer) getErrLocation() (*binlog.Location, bool) {
	s.errLocation.Lock()
	defer s.errLocation.Unlock()
	return s.errLocation.startLocation, s.errLocation.isQueryEvent
}

func (s *Syncer) handleEventError(err error, startLocation, endLocation binlog.Location, isQueryEvent bool, originSQL string) error {
	if err == nil {
		return nil
	}

	s.setErrLocation(&startLocation, &endLocation, isQueryEvent)
	if len(originSQL) > 0 {
		return terror.Annotatef(err, "startLocation: [%s], endLocation: [%s], origin SQL: [%s]", startLocation, endLocation, originSQL)
	}
	return terror.Annotatef(err, "startLocation: [%s], endLocation: [%s]", startLocation, endLocation)
}

// getEvent gets an event from streamerController or errOperatorHolder.
func (s *Syncer) getEvent(tctx *tcontext.Context, startLocation binlog.Location) (*replication.BinlogEvent, error) {
	// next event is a replace event
	if s.isReplacingErr {
		s.tctx.L().Info("try to get replace event", zap.Stringer("location", startLocation))
		return s.errOperatorHolder.GetEvent(startLocation)
	}

	return s.streamerController.GetEvent(tctx)
}

func (s *Syncer) adjustGlobalPointGTID(tctx *tcontext.Context) (bool, error) {
	location := s.checkpoint.GlobalPoint()
	// situations that don't need to adjust
	// 1. GTID is not enabled
	// 2. location already has GTID position
	// 3. location is totally new, has no position info
	if !s.cfg.EnableGTID || location.GTIDSetStr() != "" || location.Position.Name == "" {
		return false, nil
	}
	// set enableGTID to false for new streamerController
	streamerController := NewStreamerController(s.syncCfg, false, s.fromDB, s.binlogType, s.cfg.RelayDir, s.timezone)

	endPos := binlog.AdjustPosition(location.Position)
	startPos := mysql.Position{
		Name: endPos.Name,
		Pos:  0,
	}
	startLocation := location.Clone()
	startLocation.Position = startPos

	err := streamerController.Start(tctx, startLocation)
	if err != nil {
		return false, err
	}
	defer streamerController.Close(tctx)

	gs, err := reader.GetGTIDsForPosFromStreamer(tctx.Context(), streamerController.streamer, endPos)
	if err != nil {
		s.tctx.L().Warn("fail to get gtids for global location", zap.Stringer("pos", location), zap.Error(err))
		return false, err
	}
	dbConn, err := s.fromDB.BaseDB.GetBaseConn(tctx.Context())
	if err != nil {
		s.tctx.L().Warn("fail to build connection", zap.Stringer("pos", location), zap.Error(err))
		return false, err
	}
	gs, err = utils.AddGSetWithPurged(tctx.Context(), gs, dbConn.DBConn)
	if err != nil {
		s.tctx.L().Warn("fail to merge purged gtidSet", zap.Stringer("pos", location), zap.Error(err))
		return false, err
	}
	err = location.SetGTID(gs.Origin())
	if err != nil {
		s.tctx.L().Warn("fail to set gtid for global location", zap.Stringer("pos", location),
			zap.String("adjusted_gtid", gs.String()), zap.Error(err))
		return false, err
	}
	s.saveGlobalPoint(location)
	// redirect streamer for new gtid set location
	err = s.streamerController.RedirectStreamer(tctx, location)
	if err != nil {
		s.tctx.L().Warn("fail to redirect streamer for global location", zap.Stringer("pos", location),
			zap.String("adjusted_gtid", gs.String()), zap.Error(err))
		return false, err
	}
	return true, nil
}

// delLoadTask is called when finish restoring data, to delete load worker in etcd.
func (s *Syncer) delLoadTask() error {
	_, _, err := ha.DelLoadTask(s.cli, s.cfg.Name, s.cfg.SourceID)
	if err != nil {
		return err
	}
	s.tctx.Logger.Info("delete load worker in etcd for all mode", zap.String("task", s.cfg.Name), zap.String("source", s.cfg.SourceID))
	return nil
}
