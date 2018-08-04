// Copyright 2016 PingCAP, Inc.
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
	"database/sql"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-enterprise-tools/dm/config"
	"github.com/pingcap/tidb-enterprise-tools/dm/pb"
	"github.com/pingcap/tidb-enterprise-tools/dm/unit"
	"github.com/pingcap/tidb-enterprise-tools/pkg/filter"
	"github.com/pingcap/tidb-enterprise-tools/pkg/streamer"
	"github.com/pingcap/tidb-enterprise-tools/pkg/utils"
	"github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/sync2"
	"golang.org/x/net/context"
)

var (
	maxRetryCount = 100

	retryTimeout = 3 * time.Second
	waitTime     = 10 * time.Millisecond
	maxWaitTime  = 3 * time.Second
	eventTimeout = 1 * time.Hour
	statusTime   = 30 * time.Second

	maxDMLConnectionTimeout = "1m"
	maxDDLConnectionTimeout = "3h"
)

type BinlogType uint8

const (
	RemoteBinlog BinlogType = iota + 1
	LocalBinlog
)

// safeMode makes syncer reentrant.
// we make each operator reentrant to make syncer reentrant.
// `replace` and `delete` are naturally reentrant.
// use `delete`+`replace` to represent `update` can make `update`  reentrant.
// but there are no ways to make `update` idempotent,
// if we start syncer at an early position, database must bear a period of inconsistent state,
// it's eventual consistency.
var safeMode sync2.AtomicBool

// Syncer can sync your MySQL data to another MySQL database.
type Syncer struct {
	sync.RWMutex

	cfg     *config.SubTaskConfig
	syncCfg replication.BinlogSyncerConfig

	meta Meta

	// TODO: extract to interface?
	syncer      *replication.BinlogSyncer
	localReader *streamer.BinlogReader
	binlogType  BinlogType

	wg    sync.WaitGroup
	jobWg sync.WaitGroup

	tables map[string]*table

	fromDB *sql.DB
	toDBs  []*sql.DB
	ddlDB  *sql.DB

	jobs       []chan *job
	jobsClosed sync2.AtomicBool

	c *causality

	tableRouter *router.Table

	closed sync2.AtomicBool

	start    time.Time
	lastTime time.Time

	lastCount sync2.AtomicInt64
	count     sync2.AtomicInt64
	totalTps  sync2.AtomicInt64
	tps       sync2.AtomicInt64

	done chan struct{}

	filter       *filter.Filter
	skipDMLRules *SkipDMLRules

	lackOfReplClientPrivilege bool
	lastSlaveConnectionID     uint32

	unitType pb.UnitType

	// record process error rather than log.Fatal
	runFatalChan chan *pb.ProcessError
}

// NewSyncer creates a new Syncer.
func NewSyncer(cfg *config.SubTaskConfig) *Syncer {
	syncer := new(Syncer)
	syncer.cfg = cfg
	syncer.meta = NewLocalMeta(cfg.Meta, cfg.Flavor)
	syncer.jobsClosed.Set(true) // not open yet
	syncer.closed.Set(false)
	syncer.lastCount.Set(0)
	syncer.count.Set(0)
	syncer.tables = make(map[string]*table)
	syncer.c = newCausality()
	syncer.tableRouter, _ = router.NewTableRouter([]*router.TableRule{})
	syncer.done = make(chan struct{})
	syncer.unitType = pb.UnitType_Sync
	rules := &filter.Rules{
		DoDBs:        cfg.DoDBs,
		DoTables:     cfg.DoTables,
		IgnoreDBs:    cfg.IgnoreDBs,
		IgnoreTables: cfg.IgnoreTables,
	}
	syncer.filter = filter.New(rules)

	syncer.syncCfg = replication.BinlogSyncerConfig{
		ServerID:       uint32(syncer.cfg.ServerID),
		Flavor:         syncer.cfg.Flavor,
		Host:           syncer.cfg.From.Host,
		Port:           uint16(syncer.cfg.From.Port),
		User:           syncer.cfg.From.User,
		Password:       syncer.cfg.From.Password,
		UseDecimal:     true,
		VerifyChecksum: syncer.cfg.VerifyChecksum,
	}

	syncer.binlogType = toBinlogType(cfg.BinlogType)
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
	for _, ch := range s.jobs {
		close(ch)
	}
	s.jobsClosed.Set(true)
}

// Type implements Unit.Type
func (s *Syncer) Type() pb.UnitType {
	return s.unitType
}

// Init initializes syncer for a sync task, but not start Process.
// if fail, it should not call s.Close.
// some check may move to checker later.
func (s *Syncer) Init() error {
	err := s.meta.Load()
	if err != nil {
		return errors.Trace(err)
	}

	err = s.createDBs()
	if err != nil {
		return errors.Trace(err)
	}

	err = checkBinlogFormat(s.fromDB)
	if err != nil {
		return errors.Trace(err)
	}

	// support regex
	s.genSkipDMLRules()
	err = s.genRouter()
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Process implements the dm.Unit interface.
func (s *Syncer) Process(ctx context.Context, pr chan pb.ProcessResult) {
	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	if s.binlogType == RemoteBinlog {
		// create new binlog-syncer
		if s.syncer != nil {
			s.syncer.Close()
		}

		s.syncer = replication.NewBinlogSyncer(s.syncCfg)
	} else if s.binlogType == LocalBinlog {
		s.localReader = streamer.NewBinlogReader(&streamer.BinlogReaderConfig{BinlogDir: s.cfg.RelayDir})
	}
	// create new done chan
	s.done = make(chan struct{})
	// create new job chans
	s.newJobChans(s.cfg.WorkerCount + 1)

	s.runFatalChan = make(chan *pb.ProcessError, s.cfg.WorkerCount+1)
	errs := make([]*pb.ProcessError, 0, 2)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case err, ok := <-s.runFatalChan:
				if !ok {
					return
				}
				cancel() // cancel s.Run
				errs = append(errs, err)
			}
		}
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
	pr <- pb.ProcessResult{
		IsCanceled: isCanceled,
		Errors:     errs,
	}
}

func (s *Syncer) getServerUUID() (string, error) {
	return getServerUUID(s.fromDB)
}

func (s *Syncer) getMasterStatus() (mysql.Position, GTIDSet, error) {
	return getMasterStatus(s.fromDB, s.cfg.Flavor)
}

func (s *Syncer) clearTables() {
	s.tables = make(map[string]*table)
}

func (s *Syncer) getTableFromDB(db *sql.DB, schema string, name string) (*table, error) {
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

func (s *Syncer) getTable(schema string, table string) (*table, error) {
	key := fmt.Sprintf("%s.%s", schema, table)

	value, ok := s.tables[key]
	if ok {
		return value, nil
	}

	db := s.toDBs[len(s.toDBs)-1]
	t, err := s.getTableFromDB(db, schema, table)
	if err != nil {
		return nil, errors.Trace(err)
	}

	s.tables[key] = t
	return t, nil
}

func (s *Syncer) addCount(tp opType, n int64) {
	switch tp {
	case insert:
		sqlJobsTotal.WithLabelValues("insert").Add(float64(n))
	case update:
		sqlJobsTotal.WithLabelValues("update").Add(float64(n))
	case del:
		sqlJobsTotal.WithLabelValues("del").Add(float64(n))
	case ddl:
		sqlJobsTotal.WithLabelValues("ddl").Add(float64(n))
	case xid:
		// ignore xid jobs
	case flush:
		sqlJobsTotal.WithLabelValues("flush").Add(float64(n))
	default:
		log.Warnf("unknown optype %v", tp)
	}

	s.count.Add(n)
}

func (s *Syncer) checkWait(job *job) bool {
	if job.tp == ddl {
		return true
	}

	if s.meta.Check() {
		return true
	}

	return false
}

func (s *Syncer) addJob(job *job) error {
	switch job.tp {
	case xid:
		return s.meta.Save(job.pos, job.gtidSet, false)
	case ddl:
		// wait 3 seconds? refine it later
		// while meet ddl, we should wait all dmls finished firstly
		s.jobWg.Wait()
	case flush:
		// ugly code addJob and sync, refine it later
		s.jobWg.Add(s.cfg.WorkerCount)
		for i := 0; i < s.cfg.WorkerCount; i++ {
			s.jobs[i] <- job
		}
		s.jobWg.Wait()
		err := s.meta.Flush()
		return errors.Trace(err)
	}

	if len(job.sql) > 0 {
		s.jobWg.Add(1)
		if job.tp == ddl {
			s.jobs[s.cfg.WorkerCount] <- job
		} else {
			idx := int(utils.GenHashKey(job.key)) % s.cfg.WorkerCount
			s.jobs[idx] <- job
		}
	}

	wait := s.checkWait(job)
	if wait {
		s.jobWg.Wait()
		s.c.reset()
	}

	err := s.meta.Save(job.pos, job.gtidSet, wait)
	return errors.Trace(err)
}

func (s *Syncer) sync(ctx context.Context, db *sql.DB, jobChan chan *job) {
	defer s.wg.Done()

	idx := 0
	count := s.cfg.Batch
	sqls := make([]string, 0, count)
	args := make([][]interface{}, 0, count)
	lastSyncTime := time.Now()
	tpCnt := make(map[opType]int64)

	clearF := func() {
		for i := 0; i < idx; i++ {
			s.jobWg.Done()
		}

		idx = 0
		sqls = sqls[0:0]
		args = args[0:0]
		lastSyncTime = time.Now()
		for tpName, v := range tpCnt {
			s.addCount(tpName, v)
			tpCnt[tpName] = 0
		}
	}

	fatalF := func(err error, errType pb.ErrorType) {
		clearF()
		s.runFatalChan <- unit.NewProcessError(errType, errors.ErrorStack(err))
	}

	var err error
	for {
		select {
		case job, ok := <-jobChan:
			if !ok {
				return
			}
			idx++

			if job.tp == ddl {
				err = executeSQL(db, sqls, args, s.cfg.MaxRetry)
				if err != nil {
					fatalF(err, pb.ErrorType_ExecSQL)
					continue
				}

				// executes DDL SQL
				err = executeSQL(db, []string{job.sql}, [][]interface{}{job.args}, s.cfg.MaxRetry)
				if err != nil {
					if !ignoreDDLError(err) {
						fatalF(err, pb.ErrorType_ExecSQL)
						continue
					} else {
						log.Warnf("[ignore ddl error][sql]%s[args]%v[error]%v", job.sql, job.args, err)
					}
				}

				tpCnt[job.tp]++
				clearF()

			} else if job.tp != flush {
				sqls = append(sqls, job.sql)
				args = append(args, job.args)
				tpCnt[job.tp]++
			}

			if idx >= count || job.tp == flush {
				err = executeSQL(db, sqls, args, s.cfg.MaxRetry)
				if err != nil {
					fatalF(err, pb.ErrorType_ExecSQL)
					continue
				}
				clearF()
			}

		default:
			now := time.Now()
			if now.Sub(lastSyncTime) >= maxWaitTime {
				err = executeSQL(db, sqls, args, s.cfg.MaxRetry)
				if err != nil {
					fatalF(err, pb.ErrorType_ExecSQL)
					continue
				}
				clearF()
			}

			time.Sleep(waitTime)
		}
	}
}

// Run starts running for sync, we should guarantee it can rerun when paused.
func (s *Syncer) Run(ctx context.Context) (err error) {
	defer func() {
		close(s.done)
	}()

	streamer, err := s.getBinlogStreamer()
	if err != nil {
		return errors.Trace(err)
	}

	pos := s.meta.Pos()
	gs, err := s.meta.GTID()
	if err != nil {
		return errors.Trace(err)
	}

	for i := 0; i < s.cfg.WorkerCount; i++ {
		s.wg.Add(1)
		go func(i int) {
			ctx, cancel := context.WithCancel(ctx)
			s.sync(ctx, s.toDBs[i], s.jobs[i])
			cancel()
		}(i)
	}
	s.wg.Add(1)
	go func() {
		ctx, cancel := context.WithCancel(ctx)
		s.sync(ctx, s.ddlDB, s.jobs[s.cfg.WorkerCount])
		cancel()
	}()
	s.wg.Add(1)
	go func() {
		ctx, cancel := context.WithCancel(ctx)
		s.printStatus(ctx)
		cancel()
	}()

	defer func() {
		if err1 := recover(); err1 != nil {
			log.Errorf("panic. err: %s, stack: %s", err1, debug.Stack())
			err = errors.Errorf("panic error: %v", err1)
		}
		if err1 := s.flushJobs(); err1 != nil {
			log.Errorf("fail to finish all jobs error: %v", err1)
		}
	}()

	s.enableSafeModeInitializationPhase(ctx)

	s.start = time.Now()
	s.lastTime = s.start
	tryReSync := true
	for {
		ctx, cancel := context.WithTimeout(ctx, eventTimeout)
		e, err := streamer.GetEvent(ctx)
		cancel()

		if err == context.Canceled {
			log.Infof("ready to quit! [%v]", pos)
			return nil
		} else if err == context.DeadlineExceeded {
			log.Info("deadline exceeded.")
			if s.needResync() {
				log.Info("timeout, resync")
				streamer, err = s.reopenWithRetry(s.syncCfg)
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
				streamer, err = s.reSyncBinlog(s.syncCfg)
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
		binlogPos.WithLabelValues("syncer").Set(float64(e.Header.LogPos))
		binlogFile.WithLabelValues("syncer").Set(getBinlogIndex(s.meta.Pos().Name))

		switch ev := e.Event.(type) {
		case *replication.RotateEvent:
			binlogEventsTotal.WithLabelValues("rotate").Inc()
			currentPos := mysql.Position{
				Name: string(ev.NextLogName),
				Pos:  uint32(ev.Position),
			}
			if currentPos.Name > pos.Name {
				pos = currentPos
				err = s.meta.Save(pos, gs, true)
				if err != nil {
					return errors.Trace(err)
				}
			}
			log.Infof("rotate binlog to %v", currentPos)
		case *replication.RowsEvent:
			// binlogEventsTotal.WithLabelValues("type", "rows").Add(1)
			schemaName, tableName := s.renameShardingSchema(string(ev.Table.Schema), string(ev.Table.Table))
			log.Debugf("source-db:%s table:%s; target-db:%s table:%s, RowsEvent data: %v", ev.Table.Schema, ev.Table.Table, schemaName, tableName, ev.Rows)

			table := &table{}
			if s.skipRowEvent(schemaName, tableName, e.Header.EventType) {
				binlogSkippedEventsTotal.WithLabelValues("rows").Inc()
				if err = s.recordSkipSQLsPos(insert, pos, gs); err != nil {
					return errors.Trace(err)
				}

				continue
			}
			table, err = s.getTable(schemaName, tableName)
			if err != nil {
				return errors.Trace(err)
			}

			var (
				sqls []string
				keys [][]string
				args [][]interface{}
			)
			switch e.Header.EventType {
			case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				binlogEventsTotal.WithLabelValues("write_rows").Inc()

				sqls, keys, args, err = genInsertSQLs(table.schema, table.name, ev.Rows, table.columns, table.indexColumns)
				if err != nil {
					return errors.Errorf("gen insert sqls failed: %v, schema: %s, table: %s", err, table.schema, table.name)
				}

				for i := range sqls {
					err = s.commitJob(insert, sqls[i], args[i], keys[i], true, pos, gs)
					if err != nil {
						return errors.Trace(err)
					}
				}
			case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				binlogEventsTotal.WithLabelValues("update_rows").Inc()

				sqls, keys, args, err = genUpdateSQLs(table.schema, table.name, ev.Rows, table.columns, table.indexColumns)
				if err != nil {
					return errors.Errorf("gen update sqls failed: %v, schema: %s, table: %s", err, table.schema, table.name)
				}

				for i := range sqls {
					err = s.commitJob(update, sqls[i], args[i], keys[i], true, pos, gs)
					if err != nil {
						return errors.Trace(err)
					}
				}
			case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				binlogEventsTotal.WithLabelValues("delete_rows").Inc()

				sqls, keys, args, err = genDeleteSQLs(table.schema, table.name, ev.Rows, table.columns, table.indexColumns)
				if err != nil {
					return errors.Errorf("gen delete sqls failed: %v, schema: %s, table: %s", err, table.schema, table.name)
				}

				for i := range sqls {
					err = s.commitJob(del, sqls[i], args[i], keys[i], true, pos, gs)
					if err != nil {
						return errors.Trace(err)
					}
				}
			}
		case *replication.QueryEvent:
			binlogEventsTotal.WithLabelValues("query").Inc()

			sql := strings.TrimSpace(string(ev.Query))
			nextPos := mysql.Position{
				Name: pos.Name,
				Pos:  e.Header.LogPos,
			}

			log.Infof("[query]%s [current pos]%v [next pos]%v [current gtid set]%v [next gtid set]%v", sql, pos, nextPos, gs, ev.GSet)
			sqls, err := resolveDDLSQL(sql)
			if err != nil {
				if s.skipQueryEvent(sql) {
					binlogSkippedEventsTotal.WithLabelValues("query").Inc()
					log.Warnf("[skip query-sql]%s  [schema]:%s", sql, ev.Schema)
					continue
				}
				log.Errorf("fail to be parsed, error %v", err)
				return errors.Trace(err)
			}

			if len(sqls) > 0 {
				gs.Set(ev.GSet)
				pos = nextPos
			}
			for _, sql := range sqls {
				// refine skip later
				if s.skipQueryEvent(sql) {
					binlogSkippedEventsTotal.WithLabelValues("query").Inc()
					log.Warnf("[skip query-sql]%s  [schema]:%s", sql, ev.Schema)
					continue
				}

				tableNames, err := s.fetchDDLTableNames(sql, string(ev.Schema))
				if err != nil {
					return errors.Trace(err)
				}
				if s.skipQueryDDL(sql, tableNames[1]) {
					binlogSkippedEventsTotal.WithLabelValues("query_ddl").Inc()
					if err = s.recordSkipSQLsPos(ddl, pos, gs); err != nil {
						return errors.Trace(err)
					}

					log.Warnf("[skip query-ddl-sql]%s [schema]%s", sql, ev.Schema)
					continue
				}

				sql, err = genDDLSQL(sql, tableNames[0], tableNames[1])
				if err != nil {
					return errors.Trace(err)
				}

				log.Infof("[ddl][schema]%s [start]%s", string(ev.Schema), sql)

				job := newJob(ddl, sql, nil, "", false, pos, gs)
				err = s.addJob(job)
				if err != nil {
					return errors.Trace(err)
				}

				log.Infof("[ddl][end]%s", sql)

				s.clearTables()
			}
		case *replication.XIDEvent:
			pos.Pos = e.Header.LogPos
			gs.Set(ev.GSet)

			log.Debugf("[XID event][pos]%v [gtid set]%v", pos, gs)

			job := newXIDJob(pos, gs)
			s.addJob(job)
		}
	}
}

func (s *Syncer) commitJob(tp opType, sql string, args []interface{}, keys []string, retry bool, pos mysql.Position, gs GTIDSet) error {
	key, err := s.resolveCasuality(keys)
	if err != nil {
		return errors.Errorf("resolve karam error %v", err)
	}
	job := newJob(tp, sql, args, key, retry, pos, gs)
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

func (s *Syncer) genSkipDMLRules() {
	s.skipDMLRules = &SkipDMLRules{
		skipByDML:    make(map[dmlType]struct{}),
		skipBySchema: make(map[string]map[dmlType]struct{}),
		skipByTable:  make(map[string]map[string]map[dmlType]struct{}),
	}

	// for backward compatibility
	for _, dml := range s.cfg.SkipEvents {
		dt := toDmlType(dml)
		if dt == dmlInvalid {
			continue
		}
		s.skipDMLRules.skipByDML[dt] = struct{}{}
	}

	for _, skipDML := range s.cfg.SkipDMLs {
		dt := toDmlType(skipDML.Type)
		if dt == dmlInvalid {
			continue
		}
		if skipDML.Schema == "" && skipDML.Table == "" {
			s.skipDMLRules.skipByDML[dt] = struct{}{}

		} else if skipDML.Schema == "" && skipDML.Table != "" {
			// it shouldn't have such rule since wo do precheck in config `adjust` function,
			// but check it case we really have one.
			log.Warnf("[syncer] invalid skip dml rule %+v and ignore it", skipDML)
			continue

		} else if skipDML.Schema != "" && skipDML.Table == "" {
			if _, ok := s.skipDMLRules.skipBySchema[skipDML.Schema]; !ok {
				s.skipDMLRules.skipBySchema[skipDML.Schema] = make(map[dmlType]struct{})
			}
			s.skipDMLRules.skipBySchema[skipDML.Schema][dt] = struct{}{}

		} else {
			if _, ok := s.skipDMLRules.skipByTable[skipDML.Schema]; !ok {
				s.skipDMLRules.skipByTable[skipDML.Schema] = make(map[string]map[dmlType]struct{})
			}
			if _, ok := s.skipDMLRules.skipByTable[skipDML.Schema][skipDML.Table]; !ok {
				s.skipDMLRules.skipByTable[skipDML.Schema][skipDML.Table] = make(map[dmlType]struct{})
			}
			s.skipDMLRules.skipByTable[skipDML.Schema][skipDML.Table][dt] = struct{}{}
		}
	}
}

func (s *Syncer) printStatus(ctx context.Context) {
	defer s.wg.Done()

	timer := time.NewTicker(statusTime)
	defer timer.Stop()

	var (
		err                 error
		latestMasterPos     mysql.Position
		latestmasterGTIDSet GTIDSet
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

			tps, totalTps := int64(0), int64(0)
			if seconds > 0 {
				tps = (total - last) / seconds
				totalTps = total / totalSeconds
			}

			if !s.lackOfReplClientPrivilege {
				latestMasterPos, latestmasterGTIDSet, err = s.getMasterStatus()
				if err != nil {
					if isAccessDeniedError(err) {
						s.lackOfReplClientPrivilege = true
					}
					log.Errorf("[syncer] get master status error %s", err)
				} else {
					binlogPos.WithLabelValues("master").Set(float64(latestMasterPos.Pos))
					binlogFile.WithLabelValues("master").Set(getBinlogIndex(latestMasterPos.Name))
				}
			}

			log.Infof("[syncer]total events = %d, total tps = %d, recent tps = %d, master-binlog = %v, master-binlog-gtid=%v, %s",
				total, totalTps, tps, latestMasterPos, latestmasterGTIDSet, s.meta)

			s.lastCount.Set(total)
			s.lastTime = time.Now()
			s.totalTps.Set(totalTps)
			s.tps.Set(tps)
		}
	}
}

func (s *Syncer) getBinlogStreamer() (streamer.Streamer, error) {
	if s.binlogType == RemoteBinlog {
		return s.getRemoteBinlogStreamer()
	}
	return s.getLocalBinlogStreamer()
}

func (s *Syncer) getLocalBinlogStreamer() (streamer.Streamer, error) {
	return s.localReader.StartSync(s.meta.Pos())
}

func (s *Syncer) getRemoteBinlogStreamer() (streamer.Streamer, error) {
	defer func() {
		s.lastSlaveConnectionID = s.syncer.LastConnectionID()
		log.Infof("[syncer] last slave connection id %d", s.lastSlaveConnectionID)
	}()
	if s.cfg.EnableGTID {
		gs, err := s.meta.GTID()
		if err != nil {
			return nil, errors.Trace(err)
		}

		streamer, err := s.syncer.StartSyncGTID(gs.Origin())
		if err != nil {
			log.Errorf("start sync in gtid mode error %v", err)
			return s.startSyncByPosition()
		}

		return streamer, errors.Trace(err)
	}

	return s.startSyncByPosition()

}

func (s *Syncer) createDBs() error {
	var err error
	s.fromDB, err = createDB(s.cfg.From, maxDMLConnectionTimeout)
	if err != nil {
		return errors.Trace(err)
	}

	s.toDBs = make([]*sql.DB, 0, s.cfg.WorkerCount)
	s.toDBs, err = createDBs(s.cfg.To, s.cfg.WorkerCount, maxDMLConnectionTimeout)
	if err != nil {
		return errors.Trace(err)
	}
	// db for ddl
	s.ddlDB, err = createDB(s.cfg.To, maxDDLConnectionTimeout)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// record skip ddl/dml sqls' position
// make newJob's sql argument empty to distinguish normal sql and skips sql
func (s *Syncer) recordSkipSQLsPos(op opType, pos mysql.Position, gtidSet GTIDSet) error {
	job := newJob(op, "", nil, "", false, pos, gtidSet)
	err := s.addJob(job)
	return errors.Trace(err)
}

func (s *Syncer) flushJobs() error {
	log.Infof("flush all jobs meta = %v", s.meta)
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
		if isRetryableError(err) {
			log.Infof("[syncer] retry open binlog streamer %v", err)
			time.Sleep(retryTimeout)
			continue
		}
		break
	}
	return nil, errors.Trace(err)
}

func (s *Syncer) reopen(cfg replication.BinlogSyncerConfig) (streamer.Streamer, error) {
	s.syncer.Close()
	log.Infof("[syncer] killing connectionid %d", s.lastSlaveConnectionID)
	// kill last slave connection by `KILL [id]`
	if s.lastSlaveConnectionID > 0 {
		err := killConn(s.fromDB, s.lastSlaveConnectionID)
		if err != nil {
			if !isNoSuchThreadError(err) {
				return nil, errors.Trace(err)
			}
		}
	}

	s.syncer = replication.NewBinlogSyncer(cfg)
	return s.getBinlogStreamer()
}

func (s *Syncer) startSyncByPosition() (streamer.Streamer, error) {
	streamer, err := s.syncer.StartSync(s.meta.Pos())
	return streamer, errors.Trace(err)
}

// the result contains [source TableNames, target TableNames]
// the detail of TableNames refs `parserDDLTableNames()`
func (s *Syncer) fetchDDLTableNames(sql string, schema string) ([][]*filter.Table, error) {
	tableNames, err := parserDDLTableNames(sql)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var targetTableNames []*filter.Table
	for i := range tableNames {
		if tableNames[i].Schema == "" {
			tableNames[i].Schema = schema
		}
		schema, table := s.renameShardingSchema(tableNames[i].Schema, tableNames[i].Name)
		tableName := &filter.Table{
			Schema: schema,
			Name:   table,
		}
		targetTableNames = append(targetTableNames, tableName)
	}

	return [][]*filter.Table{tableNames, targetTableNames}, nil
}

func (s *Syncer) renameShardingSchema(schema, table string) (string, string) {
	if schema == "" {
		return schema, table
	}
	schemaL := strings.ToLower(schema)
	tableL := strings.ToLower(table)
	targetSchema, targetTable, err := s.tableRouter.Route(schemaL, tableL)
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

	s.stopSync()

	closeDBs(s.fromDB)
	closeDBs(s.toDBs...)
	closeDBs(s.ddlDB)

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
	if s.lastSlaveConnectionID > 0 {
		err := killConn(s.fromDB, s.lastSlaveConnectionID)
		if err != nil {
			log.Errorf("[syncer] kill last connection %d err %v", s.lastSlaveConnectionID, err)
		}
		s.lastSlaveConnectionID = 0
	}
	if s.syncer != nil {
		s.syncer.Close()
		s.syncer = nil
	}
	if s.localReader != nil {
		s.localReader.Close()
	}
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

func (s *Syncer) needResync() bool {
	if s.lackOfReplClientPrivilege {
		return false
	}
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
	return compareBinlogPos(masterPos, s.meta.Pos(), 190) == 1
}

func (s *Syncer) enableSafeModeInitializationPhase(ctx context.Context) {
	safeMode.Set(true)

	go func() {
		ctx, cancel := context.WithCancel(ctx)
		defer func() {
			cancel()
			safeMode.Set(s.cfg.SafeMode)
		}()

		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Minute):
		}
	}()
}
