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

package loader

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/unit"
	"github.com/pingcap/dm/pkg/conn"
	tcontext "github.com/pingcap/dm/pkg/context"
	fr "github.com/pingcap/dm/pkg/func-rollback"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	cm "github.com/pingcap/tidb-tools/pkg/column-mapping"
	"github.com/pingcap/tidb-tools/pkg/filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/siddontang/go/sync2"
	"go.uber.org/zap"
)

const (
	jobCount = 1000
)

// FilePosSet represents a set in mathematics.
type FilePosSet map[string][]int64

// DataFiles represent all data files for a single table
type DataFiles []string

// Tables2DataFiles represent all data files of a table collection as a map
type Tables2DataFiles map[string]DataFiles
type dataJob struct {
	sql        string
	schema     string
	file       string
	offset     int64
	lastOffset int64
}

type fileJob struct {
	schema   string
	table    string
	dataFile string
	offset   int64
	info     *tableInfo
}

// Worker represents a worker.
type Worker struct {
	id         int
	cfg        *config.SubTaskConfig
	checkPoint CheckPoint
	conn       *DBConn
	wg         sync.WaitGroup
	jobQueue   chan *dataJob
	loader     *Loader

	tctx *tcontext.Context

	closed int64
}

// NewWorker returns a Worker.
func NewWorker(loader *Loader, id int) (worker *Worker, err error) {
	ctctx := loader.logCtx.WithLogger(loader.logCtx.L().WithFields(zap.Int("worker ID", id)))

	return &Worker{
		id:         id,
		cfg:        loader.cfg,
		checkPoint: loader.checkPoint,
		conn:       loader.toDBConns[id],
		jobQueue:   make(chan *dataJob, jobCount),
		loader:     loader,
		tctx:       ctctx,
	}, nil
}

// Close closes worker
func (w *Worker) Close() {
	// simulate the case that doesn't wait all doJob goroutine exit
	failpoint.Inject("workerCantClose", func(_ failpoint.Value) {
		w.tctx.L().Info("", zap.String("failpoint", "workerCantClose"))
		failpoint.Return()
	})

	if !atomic.CompareAndSwapInt64(&w.closed, 0, 1) {
		w.wg.Wait()
		w.tctx.L().Info("already closed...")
		return
	}

	w.tctx.L().Info("start to close...")
	close(w.jobQueue)
	w.wg.Wait()
	w.tctx.L().Info("closed !!!")
}

func (w *Worker) run(ctx context.Context, fileJobQueue chan *fileJob, runFatalChan chan *pb.ProcessError) {
	atomic.StoreInt64(&w.closed, 0)

	newCtx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
		// make sure all doJob goroutines exit
		w.Close()
	}()

	ctctx := w.tctx.WithContext(newCtx)

	doJob := func() {
		for {
			select {
			case <-newCtx.Done():
				w.tctx.L().Info("context canceled, execution goroutine exits")
				return
			case job, ok := <-w.jobQueue:
				if !ok {
					w.tctx.L().Info("job queue was closed, execution goroutine exits")
					return
				}
				if job == nil {
					w.tctx.L().Info("jobs are finished, execution goroutine exits")
					return
				}
				sqls := make([]string, 0, 3)
				sqls = append(sqls, fmt.Sprintf("USE `%s`;", job.schema))
				sqls = append(sqls, job.sql)

				offsetSQL := w.checkPoint.GenSQL(job.file, job.offset)
				sqls = append(sqls, offsetSQL)

				failpoint.Inject("LoadExceedOffsetExit", func(val failpoint.Value) {
					threshold, _ := val.(int)
					if job.offset >= int64(threshold) {
						w.tctx.L().Warn("load offset execeeds threshold, it will exit", zap.Int64("load offset", job.offset), zap.Int("value", threshold), zap.String("failpoint", "LoadExceedOffsetExit"))
						utils.OsExit(1)
					}
				})

				failpoint.Inject("LoadDataSlowDown", nil)

				err := w.conn.executeSQL(ctctx, sqls)
				failpoint.Inject("executeSQLError", func(_ failpoint.Value) {
					w.tctx.L().Info("", zap.String("failpoint", "executeSQLError"))
					err = errors.New("inject failpoint executeSQLError")
				})
				if err != nil {
					// expect pause rather than exit
					err = terror.WithScope(terror.Annotatef(err, "file %s", job.file), terror.ScopeDownstream)
					if !utils.IsContextCanceledError(err) {
						runFatalChan <- unit.NewProcessError(pb.ErrorType_ExecSQL, err)
					}
					return
				}
				w.loader.finishedDataSize.Add(job.offset - job.lastOffset)
			}
		}
	}

	// worker main routine
	for {
		select {
		case <-newCtx.Done():
			w.tctx.L().Info("context canceled, main goroutine exits")
			return
		case job, ok := <-fileJobQueue:
			if !ok {
				w.tctx.L().Info("file queue was closed, main routine exit.")
				return
			}

			w.wg.Add(1)
			go func() {
				defer w.wg.Done()
				doJob()
			}()

			// restore a table
			if err := w.restoreDataFile(ctx, filepath.Join(w.cfg.Dir, job.dataFile), job.offset, job.info); err != nil {
				// expect pause rather than exit
				err = terror.Annotatef(err, "restore data file (%v) failed", job.dataFile)
				runFatalChan <- unit.NewProcessError(pb.ErrorType_UnknownError, err)
				return
			}
		}
	}
}

func (w *Worker) restoreDataFile(ctx context.Context, filePath string, offset int64, table *tableInfo) error {
	w.tctx.L().Info("start to restore dump sql file", zap.String("data file", filePath))
	err := w.dispatchSQL(ctx, filePath, offset, table)
	if err != nil {
		return err
	}

	failpoint.Inject("dispatchError", func(_ failpoint.Value) {
		w.tctx.L().Info("", zap.String("failpoint", "dispatchError"))
		failpoint.Return(errors.New("inject failpoint dispatchError"))
	})

	// dispatchSQL completed, send nil to make sure all dmls are applied to target database
	// we don't want to close and re-make chan frequently
	// but if we need to re-call w.run, we need re-make jobQueue chan
	w.jobQueue <- nil
	w.wg.Wait()

	w.tctx.L().Info("finish to restore dump sql file", zap.String("data file", filePath))
	return nil
}

func (w *Worker) dispatchSQL(ctx context.Context, file string, offset int64, table *tableInfo) error {
	var (
		f   *os.File
		err error
		cur int64
	)

	f, err = os.Open(file)
	if err != nil {
		return terror.ErrLoadUnitDispatchSQLFromFile.Delegate(err)
	}
	defer f.Close()

	finfo, err := f.Stat()
	if err != nil {
		return terror.ErrLoadUnitDispatchSQLFromFile.Delegate(err)
	}

	baseFile := filepath.Base(file)
	err = w.checkPoint.Init(w.tctx.WithContext(ctx), baseFile, finfo.Size())
	if err != nil {
		w.tctx.L().Error("fail to initial checkpoint", zap.String("data file", file), log.ShortError(err))
		return err
	}

	cur, err = f.Seek(offset, io.SeekStart)
	if err != nil {
		return terror.ErrLoadUnitDispatchSQLFromFile.Delegate(err)
	}
	w.tctx.L().Debug("read file", zap.String("data file", file), zap.Int64("offset", offset))

	lastOffset := cur

	data := make([]byte, 0, 1024*1024)
	br := bufio.NewReader(f)
	for {
		select {
		case <-ctx.Done():
			w.tctx.L().Info("sql dispatcher is ready to quit.", zap.String("data file", file))
			return nil
		default:
			// do nothing
		}
		line, err := br.ReadString('\n')
		cur += int64(len(line))

		if err == io.EOF {
			w.tctx.L().Info("data are scanned finished.", zap.String("data file", file))
			break
		}

		realLine := strings.TrimSpace(line[:len(line)-1])
		if len(realLine) == 0 {
			continue
		}

		data = append(data, []byte(line)...)
		if realLine[len(realLine)-1] == ';' {
			query := strings.TrimSpace(string(data))
			if strings.HasPrefix(query, "/*") && strings.HasSuffix(query, "*/;") {
				data = data[0:0]
				continue
			}

			if w.loader.columnMapping != nil {
				// column mapping and route table
				query, err = reassemble(data, table, w.loader.columnMapping)
				if err != nil {
					return terror.Annotatef(err, "file %s", file)
				}
			} else if table.sourceTable != table.targetTable {
				query = renameShardingTable(query, table.sourceTable, table.targetTable)
			}

			idx := strings.Index(query, "INSERT INTO")
			if idx < 0 {
				return terror.ErrLoadUnitInvalidInsertSQL.Generate(query)
			}

			data = data[0:0]

			j := &dataJob{
				sql:        query,
				schema:     table.targetSchema,
				file:       baseFile,
				offset:     cur,
				lastOffset: lastOffset,
			}
			lastOffset = cur

			w.jobQueue <- j
		}

	}

	return nil
}

type tableInfo struct {
	sourceSchema   string
	sourceTable    string
	targetSchema   string
	targetTable    string
	columnNameList []string
	insertHeadStmt string
}

// Loader can load your mydumper data into TiDB database.
type Loader struct {
	sync.RWMutex

	cfg        *config.SubTaskConfig
	checkPoint CheckPoint

	logCtx *tcontext.Context

	// db -> tables
	// table -> data files
	db2Tables  map[string]Tables2DataFiles
	tableInfos map[string]*tableInfo

	// for every worker goroutine, not for every data file
	workerWg *sync.WaitGroup

	fileJobQueue       chan *fileJob
	fileJobQueueClosed sync2.AtomicBool

	tableRouter   *router.Table
	bwList        *filter.Filter
	columnMapping *cm.Mapping

	closed sync2.AtomicBool

	toDB      *conn.BaseDB
	toDBConns []*DBConn

	totalDataSize    sync2.AtomicInt64
	totalFileCount   sync2.AtomicInt64 // schema + table + data
	finishedDataSize sync2.AtomicInt64
	metaBinlog       sync2.AtomicString

	// record process error rather than log.Fatal
	runFatalChan chan *pb.ProcessError
}

// NewLoader creates a new Loader.
func NewLoader(cfg *config.SubTaskConfig) *Loader {
	loader := &Loader{
		cfg:        cfg,
		db2Tables:  make(map[string]Tables2DataFiles),
		tableInfos: make(map[string]*tableInfo),
		workerWg:   new(sync.WaitGroup),
		logCtx:     tcontext.Background().WithLogger(log.With(zap.String("task", cfg.Name), zap.String("unit", "load"))),
	}
	loader.fileJobQueueClosed.Set(true) // not open yet
	return loader
}

// Type implements Unit.Type
func (l *Loader) Type() pb.UnitType {
	return pb.UnitType_Load
}

// Init initializes loader for a load task, but not start Process.
// if fail, it should not call l.Close.
func (l *Loader) Init(ctx context.Context) (err error) {
	rollbackHolder := fr.NewRollbackHolder("loader")
	defer func() {
		if err != nil {
			rollbackHolder.RollbackReverseOrder()
		}
	}()

	tctx := l.logCtx.WithContext(ctx)

	checkpoint, err := newRemoteCheckPoint(tctx, l.cfg, l.checkpointID())
	failpoint.Inject("ignoreLoadCheckpointErr", func(_ failpoint.Value) {
		l.logCtx.L().Info("", zap.String("failpoint", "ignoreLoadCheckpointErr"))
		err = nil
	})
	if err != nil {
		return err
	}
	l.checkPoint = checkpoint
	rollbackHolder.Add(fr.FuncRollback{Name: "close-checkpoint", Fn: l.checkPoint.Close})

	l.bwList, err = filter.New(l.cfg.CaseSensitive, l.cfg.BWList)
	if err != nil {
		return terror.ErrLoadUnitGenBWList.Delegate(err)
	}

	if l.cfg.RemoveMeta {
		err2 := l.checkPoint.Clear(tctx)
		if err2 != nil {
			return err2
		}
		l.logCtx.L().Info("all previous checkpoints cleared")
	}

	err = l.genRouter(l.cfg.RouteRules)
	if err != nil {
		return err
	}

	if len(l.cfg.ColumnMappingRules) > 0 {
		l.columnMapping, err = cm.NewMapping(l.cfg.CaseSensitive, l.cfg.ColumnMappingRules)
		if err != nil {
			return terror.ErrLoadUnitGenColumnMapping.Delegate(err)
		}
	}

	dbCfg := l.cfg.To
	dbCfg.RawDBCfg = config.DefaultRawDBConfig().
		SetMaxIdleConns(l.cfg.PoolSize)

	l.toDB, l.toDBConns, err = createConns(tctx, l.cfg, l.cfg.PoolSize)
	if err != nil {
		return err
	}

	return nil
}

// Process implements Unit.Process
func (l *Loader) Process(ctx context.Context, pr chan pb.ProcessResult) {
	loaderExitWithErrorCounter.WithLabelValues(l.cfg.Name).Add(0)

	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	l.newFileJobQueue()
	if err := l.getMydumpMetadata(); err != nil {
		loaderExitWithErrorCounter.WithLabelValues(l.cfg.Name).Inc()
		pr <- pb.ProcessResult{
			Errors: []*pb.ProcessError{unit.NewProcessError(pb.ErrorType_UnknownError, err)},
		}
		return
	}

	l.runFatalChan = make(chan *pb.ProcessError, 2*l.cfg.PoolSize)
	errs := make([]*pb.ProcessError, 0, 2)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range l.runFatalChan {
			cancel() // cancel l.Restore
			errs = append(errs, err)
		}
	}()

	err := l.Restore(newCtx)
	close(l.runFatalChan) // Restore returned, all potential fatal sent to l.runFatalChan

	failpoint.Inject("dontWaitWorkerExit", func(_ failpoint.Value) {
		l.logCtx.L().Info("", zap.String("failpoint", "dontWaitWorkerExit"))
		l.workerWg.Wait()
	})

	wg.Wait() // wait for receive all fatal from l.runFatalChan

	if err != nil {
		loaderExitWithErrorCounter.WithLabelValues(l.cfg.Name).Inc()
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
		l.Pause()
	}
	pr <- pb.ProcessResult{
		IsCanceled: isCanceled,
		Errors:     errs,
	}
}

func (l *Loader) newFileJobQueue() {
	l.closeFileJobQueue()
	l.fileJobQueue = make(chan *fileJob, jobCount)
	l.fileJobQueueClosed.Set(false)
}

func (l *Loader) closeFileJobQueue() {
	if l.fileJobQueueClosed.Get() {
		return
	}
	close(l.fileJobQueue)
	l.fileJobQueueClosed.Set(true)
}

func (l *Loader) skipSchemaAndTable(table *filter.Table) bool {
	if filter.IsSystemSchema(table.Schema) {
		return true
	}

	tbs := []*filter.Table{table}
	tbs = l.bwList.ApplyOn(tbs)
	return len(tbs) == 0
}

func (l *Loader) isClosed() bool {
	return l.closed.Get()
}

// IsFreshTask implements Unit.IsFreshTask
func (l *Loader) IsFreshTask(ctx context.Context) (bool, error) {
	count, err := l.checkPoint.Count(l.logCtx.WithContext(ctx))
	return count == 0, err
}

// Restore begins the restore process.
func (l *Loader) Restore(ctx context.Context) error {
	// reset some counter used to calculate progress
	l.totalDataSize.Set(0)
	l.finishedDataSize.Set(0) // reset before load from checkpoint

	if err := l.prepare(); err != nil {
		l.logCtx.L().Error("scan directory failed", zap.String("directory", l.cfg.Dir), log.ShortError(err))
		return err
	}

	// not update checkpoint in memory when restoring, so when re-Restore, we need to load checkpoint from DB
	err := l.checkPoint.Load(l.logCtx.WithContext(ctx))
	if err != nil {
		return err
	}
	err = l.checkPoint.CalcProgress(l.db2Tables)
	if err != nil {
		l.logCtx.L().Error("calc load process", log.ShortError(err))
	}
	l.loadFinishedSize()

	if err2 := l.initAndStartWorkerPool(ctx); err2 != nil {
		l.logCtx.L().Error("initial and start worker pools failed", log.ShortError(err))
		return err2
	}

	go l.PrintStatus(ctx)

	begin := time.Now()
	err = l.restoreData(ctx)

	failpoint.Inject("dontWaitWorkerExit", func(_ failpoint.Value) {
		l.logCtx.L().Info("", zap.String("failpoint", "dontWaitWorkerExit"))
		failpoint.Return(nil)
	})

	// make sure all workers exit
	l.closeFileJobQueue() // all data file dispatched, close it
	l.workerWg.Wait()

	if err == nil {
		l.logCtx.L().Info("all data files have been finished", zap.Duration("cost time", time.Since(begin)))
	} else if errors.Cause(err) != context.Canceled {
		return err
	}

	return nil
}

func (l *Loader) loadFinishedSize() {
	results := l.checkPoint.GetAllRestoringFileInfo()
	for _, pos := range results {
		l.finishedDataSize.Add(pos[0])
	}
}

// Close does graceful shutdown
func (l *Loader) Close() {
	l.Lock()
	defer l.Unlock()
	if l.isClosed() {
		return
	}

	l.stopLoad()

	err := l.toDB.Close()
	if err != nil {
		l.logCtx.L().Error("close downstream DB error", log.ShortError(err))
	}
	l.checkPoint.Close()
	l.closed.Set(true)
}

// stopLoad stops loading, now it used by Close and Pause
// maybe we can refine the workflow more clear
func (l *Loader) stopLoad() {
	// before re-write workflow, simply close all job queue and job workers
	// when resuming, re-create them
	l.logCtx.L().Info("stop importing data process")

	l.closeFileJobQueue()
	l.workerWg.Wait()

	l.logCtx.L().Debug("all workers have been closed")
}

// Pause pauses the process, and it can be resumed later
// should cancel context from external
func (l *Loader) Pause() {
	if l.isClosed() {
		l.logCtx.L().Warn("try to pause, but already closed")
		return
	}

	l.stopLoad()
}

// Resume resumes the paused process
func (l *Loader) Resume(ctx context.Context, pr chan pb.ProcessResult) {
	if l.isClosed() {
		l.logCtx.L().Warn("try to resume, but already closed")
		return
	}

	err := l.resetDBs(ctx)
	if err != nil {
		pr <- pb.ProcessResult{
			IsCanceled: false,
			Errors: []*pb.ProcessError{
				unit.NewProcessError(pb.ErrorType_UnknownError, err),
			},
		}
		return
	}
	// continue the processing
	l.Process(ctx, pr)
}

func (l *Loader) resetDBs(ctx context.Context) error {
	var err error
	tctx := l.logCtx.WithContext(ctx)

	for i := 0; i < len(l.toDBConns); i++ {
		err = l.toDBConns[i].resetConn(tctx)
		if err != nil {
			return terror.WithScope(err, terror.ScopeDownstream)
		}
	}

	err = l.checkPoint.ResetConn(tctx)
	if err != nil {
		return terror.WithScope(err, terror.ScopeDownstream)
	}

	return nil
}

// Update implements Unit.Update
// now, only support to update config for routes, filters, column-mappings, black-white-list
// now no config diff implemented, so simply re-init use new config
// no binlog filter for loader need to update
func (l *Loader) Update(cfg *config.SubTaskConfig) error {
	var (
		err              error
		oldBwList        *filter.Filter
		oldTableRouter   *router.Table
		oldColumnMapping *cm.Mapping
	)

	defer func() {
		if err == nil {
			return
		}
		if oldBwList != nil {
			l.bwList = oldBwList
		}
		if oldTableRouter != nil {
			l.tableRouter = oldTableRouter
		}
		if oldColumnMapping != nil {
			l.columnMapping = oldColumnMapping
		}
	}()

	// update black-white-list
	oldBwList = l.bwList
	l.bwList, err = filter.New(cfg.CaseSensitive, cfg.BWList)
	if err != nil {
		return terror.ErrLoadUnitGenBWList.Delegate(err)
	}

	// update route, for loader, this almost useless, because schemas often have been restored
	oldTableRouter = l.tableRouter
	l.tableRouter, err = router.NewTableRouter(cfg.CaseSensitive, cfg.RouteRules)
	if err != nil {
		return terror.ErrLoadUnitGenTableRouter.Delegate(err)
	}

	// update column-mappings
	oldColumnMapping = l.columnMapping
	l.columnMapping, err = cm.NewMapping(cfg.CaseSensitive, cfg.ColumnMappingRules)
	if err != nil {
		return terror.ErrLoadUnitGenColumnMapping.Delegate(err)
	}

	// update l.cfg
	l.cfg.BWList = cfg.BWList
	l.cfg.RouteRules = cfg.RouteRules
	l.cfg.ColumnMappingRules = cfg.ColumnMappingRules
	return nil
}

func (l *Loader) genRouter(rules []*router.TableRule) error {
	l.tableRouter, _ = router.NewTableRouter(l.cfg.CaseSensitive, []*router.TableRule{})
	for _, rule := range rules {
		err := l.tableRouter.AddRule(rule)
		if err != nil {
			return terror.ErrLoadUnitGenTableRouter.Delegate(err)
		}
	}
	schemaRules, tableRules := l.tableRouter.AllRules()
	l.logCtx.L().Debug("all route rules", zap.Reflect("schema route rules", schemaRules), zap.Reflect("table route rules", tableRules))
	return nil
}

func (l *Loader) initAndStartWorkerPool(ctx context.Context) error {
	for i := 0; i < l.cfg.PoolSize; i++ {
		worker, err := NewWorker(l, i)
		if err != nil {
			return err
		}

		l.workerWg.Add(1) // for every worker goroutine, Add(1)
		go func() {
			defer l.workerWg.Done()
			worker.run(ctx, l.fileJobQueue, l.runFatalChan)
		}()
	}
	return nil
}

func (l *Loader) prepareDbFiles(files map[string]struct{}) error {
	// reset some variables
	l.db2Tables = make(map[string]Tables2DataFiles)
	l.totalFileCount.Set(0) // reset
	schemaFileCount := 0
	for file := range files {
		if !strings.HasSuffix(file, "-schema-create.sql") {
			continue
		}

		idx := strings.Index(file, "-schema-create.sql")
		if idx > 0 {
			schemaFileCount++
			db := file[:idx]
			if l.skipSchemaAndTable(&filter.Table{Schema: db}) {
				l.logCtx.L().Warn("ignore schema file", zap.String("schema file", file))
				continue
			}

			l.db2Tables[db] = make(Tables2DataFiles)
			l.totalFileCount.Add(1) // for schema
		}
	}

	if schemaFileCount == 0 {
		l.logCtx.L().Warn("invalid mydumper files for there are no `-schema-create.sql` files found, and will generate later")
	}
	if len(l.db2Tables) == 0 {
		l.logCtx.L().Warn("no available `-schema-create.sql` files, check mydumper parameter matches black-white-list in task config, will generate later")
	}

	return nil
}

func (l *Loader) prepareTableFiles(files map[string]struct{}) error {
	for file := range files {
		if !strings.HasSuffix(file, "-schema.sql") {
			continue
		}

		idx := strings.Index(file, "-schema.sql")
		name := file[:idx]
		fields := strings.Split(name, ".")
		if len(fields) != 2 {
			l.logCtx.L().Warn("invalid table schema file", zap.String("file", file))
			continue
		}

		db, table := fields[0], fields[1]
		if l.skipSchemaAndTable(&filter.Table{Schema: db, Name: table}) {
			l.logCtx.L().Warn("ignore table file", zap.String("table file", file))
			continue
		}
		tables, ok := l.db2Tables[db]
		if !ok {
			l.logCtx.L().Warn("can't find schema create file, will generate one", zap.String("schema", db))
			if err := generateSchemaCreateFile(l.cfg.Dir, db); err != nil {
				return err
			}
			l.db2Tables[db] = make(Tables2DataFiles)
			tables = l.db2Tables[db]
			l.totalFileCount.Add(1)
		}

		if _, ok := tables[table]; ok {
			return terror.ErrLoadUnitDuplicateTableFile.Generate(file)
		}
		tableCounter.WithLabelValues(l.cfg.Name).Inc()
		tables[table] = make(DataFiles, 0, 16)
		l.totalFileCount.Add(1) // for table
	}

	return nil
}

func (l *Loader) prepareDataFiles(files map[string]struct{}) error {
	for file := range files {
		if !strings.HasSuffix(file, ".sql") || strings.Contains(file, "-schema.sql") ||
			strings.Contains(file, "-schema-create.sql") {
			continue
		}

		// ignore view / triggers
		if strings.Contains(file, "-schema-view.sql") || strings.Contains(file, "-schema-triggers.sql") ||
			strings.Contains(file, "-schema-post.sql") {
			l.logCtx.L().Warn("ignore unsupport view/trigger file", zap.String("file", file))
			continue
		}

		idx := strings.Index(file, ".sql")
		name := file[:idx]
		fields := strings.Split(name, ".")
		if len(fields) != 2 && len(fields) != 3 {
			l.logCtx.L().Warn("invalid db table sql file", zap.String("file", file))
			continue
		}

		db, table := fields[0], fields[1]
		if l.skipSchemaAndTable(&filter.Table{Schema: db, Name: table}) {
			l.logCtx.L().Warn("ignore data file", zap.String("data file", file))
			continue
		}
		tables, ok := l.db2Tables[db]
		if !ok {
			return terror.ErrLoadUnitNoDBFile.Generate(file)
		}

		dataFiles, ok := tables[table]
		if !ok {
			return terror.ErrLoadUnitNoTableFile.Generate(file)
		}

		size, err := utils.GetFileSize(filepath.Join(l.cfg.Dir, file))
		if err != nil {
			return err
		}
		l.totalDataSize.Add(size)
		l.totalFileCount.Add(1) // for data

		dataFiles = append(dataFiles, file)
		dataFileCounter.WithLabelValues(l.cfg.Name).Inc()
		tables[table] = dataFiles
	}

	dataSizeCounter.WithLabelValues(l.cfg.Name).Add(float64(l.totalDataSize.Get()))
	return nil
}

func (l *Loader) prepare() error {
	begin := time.Now()
	defer func() {
		l.logCtx.L().Info("prepare loading", zap.Duration("cost time", time.Since(begin)))
	}()

	// check if mydumper dir data exists.
	if !utils.IsDirExists(l.cfg.Dir) {
		// compatibility with no `.name` suffix
		dirSuffix := "." + l.cfg.Name
		var trimmed bool
		if strings.HasSuffix(l.cfg.Dir, dirSuffix) {
			dirPrefix := strings.TrimSuffix(l.cfg.Dir, dirSuffix)
			if utils.IsDirExists(dirPrefix) {
				l.logCtx.L().Warn("directory doesn't exist, try to load data from old fashion directory", zap.String("directory", l.cfg.Dir), zap.String("old fashion directory", dirPrefix))
				l.cfg.Dir = dirPrefix
				trimmed = true
			}
		}
		if !trimmed {
			return terror.ErrLoadUnitDumpDirNotFound.Generate(l.cfg.Dir)
		}
	}

	// collect dir files.
	files := CollectDirFiles(l.cfg.Dir)

	l.logCtx.L().Debug("collected files", zap.Reflect("files", files))

	/* Mydumper file names format
	 * db    {db}-schema-create.sql
	 * table {db}.{table}-schema.sql
	 * sql   {db}.{table}.{part}.sql or {db}.{table}.sql
	 */

	// Sql file for create db
	if err := l.prepareDbFiles(files); err != nil {
		return err
	}

	// Sql file for create table
	if err := l.prepareTableFiles(files); err != nil {
		return err
	}

	// Sql file for restore data
	return l.prepareDataFiles(files)
}

// restoreSchema creates schema
func (l *Loader) restoreSchema(ctx context.Context, conn *DBConn, sqlFile, schema string) error {
	err := l.restoreStructure(ctx, conn, sqlFile, schema, "")
	if err != nil {
		if isErrDBExists(err) {
			l.logCtx.L().Info("database already exists, skip it", zap.String("db schema file", sqlFile))
		} else {
			return terror.Annotatef(err, "run db schema failed - dbfile %s", sqlFile)
		}
	}
	return nil
}

// restoreTable creates table
func (l *Loader) restoreTable(ctx context.Context, conn *DBConn, sqlFile, schema, table string) error {
	err := l.restoreStructure(ctx, conn, sqlFile, schema, table)
	if err != nil {
		if isErrTableExists(err) {
			l.logCtx.L().Info("table already exists, skip it", zap.String("table schema file", sqlFile))
		} else {
			return terror.Annotatef(err, "run table schema failed - dbfile %s", sqlFile)
		}
	}
	return nil
}

// restoreStruture creates schema or table
func (l *Loader) restoreStructure(ctx context.Context, conn *DBConn, sqlFile string, schema string, table string) error {
	f, err := os.Open(sqlFile)
	if err != nil {
		return terror.ErrLoadUnitReadSchemaFile.Delegate(err)
	}
	defer f.Close()

	tctx := l.logCtx.WithContext(ctx)

	data := make([]byte, 0, 1024*1024)
	br := bufio.NewReader(f)
	for {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			break
		}

		realLine := strings.TrimSpace(line[:len(line)-1])
		if len(realLine) == 0 {
			continue
		}

		data = append(data, []byte(realLine)...)
		if data[len(data)-1] == ';' {
			query := string(data)
			data = data[0:0]
			if strings.HasPrefix(query, "/*") && strings.HasSuffix(query, "*/;") {
				continue
			}

			var sqls []string
			dstSchema, dstTable := fetchMatchedLiteral(tctx, l.tableRouter, schema, table)
			// for table
			if table != "" {
				sqls = append(sqls, fmt.Sprintf("USE `%s`;", dstSchema))
				query = renameShardingTable(query, table, dstTable)
			} else {
				query = renameShardingSchema(query, schema, dstSchema)
			}

			l.logCtx.L().Debug("schema create statement", zap.String("sql", query))

			sqls = append(sqls, query)
			err = conn.executeSQL(tctx, sqls)
			if err != nil {
				return terror.WithScope(err, terror.ScopeDownstream)
			}
		}

	}

	return nil
}

// renameShardingTable replaces srcTable with dstTable in query
func renameShardingTable(query, srcTable, dstTable string) string {
	return SQLReplace(query, srcTable, dstTable)
}

// renameShardingSchema replaces srcSchema with dstSchema in query
func renameShardingSchema(query, srcSchema, dstSchema string) string {
	return SQLReplace(query, srcSchema, dstSchema)
}

func fetchMatchedLiteral(ctx *tcontext.Context, router *router.Table, schema, table string) (targetSchema string, targetTable string) {
	if schema == "" {
		// nothing change
		return schema, table
	}

	targetSchema, targetTable, err := router.Route(schema, table)
	if err != nil {
		ctx.L().Error("fail to route table", zap.Error(err)) // log the error, but still continue
	}
	if targetSchema == "" {
		// nothing change
		return schema, table
	}
	if targetTable == "" {
		// table still same;
		targetTable = table
	}

	return targetSchema, targetTable
}

func (l *Loader) restoreData(ctx context.Context) error {
	begin := time.Now()

	baseConn, err := l.toDB.GetBaseConn(ctx)
	if err != nil {
		return err
	}
	defer l.toDB.CloseBaseConn(baseConn)
	dbConn := &DBConn{
		cfg:      l.cfg,
		baseConn: baseConn,
		resetBaseConnFn: func(*tcontext.Context, *conn.BaseConn) (*conn.BaseConn, error) {
			return nil, terror.ErrDBBadConn.Generate("bad connection error restoreData")
		},
	}

	dispatchMap := make(map[string]*fileJob)

	// restore db in sort
	dbs := make([]string, 0, len(l.db2Tables))
	for db := range l.db2Tables {
		dbs = append(dbs, db)
	}

	tctx := l.logCtx.WithContext(ctx)

	for _, db := range dbs {
		tables := l.db2Tables[db]

		// create db
		dbFile := fmt.Sprintf("%s/%s-schema-create.sql", l.cfg.Dir, db)
		l.logCtx.L().Info("start to create schema", zap.String("schema file", dbFile))
		err = l.restoreSchema(ctx, dbConn, dbFile, db)
		if err != nil {
			return err
		}
		l.logCtx.L().Info("finish to create schema", zap.String("schema file", dbFile))

		tnames := make([]string, 0, len(tables))
		for t := range tables {
			tnames = append(tnames, t)
		}
		for _, table := range tnames {
			dataFiles := tables[table]
			tableFile := fmt.Sprintf("%s/%s.%s-schema.sql", l.cfg.Dir, db, table)
			if _, ok := l.tableInfos[tableName(db, table)]; !ok {
				l.tableInfos[tableName(db, table)], err = parseTable(tctx, l.tableRouter, db, table, tableFile)
				if err != nil {
					return terror.Annotatef(err, "parse table %s/%s", db, table)
				}
			}

			if l.checkPoint.IsTableFinished(db, table) {
				l.logCtx.L().Info("table has finished, skip it.", zap.String("schema", db), zap.String("table", table))
				continue
			}

			// create table
			l.logCtx.L().Info("start to create table", zap.String("table file", tableFile))
			err := l.restoreTable(ctx, dbConn, tableFile, db, table)
			if err != nil {
				return err
			}
			l.logCtx.L().Info("finish to create table", zap.String("table file", tableFile))

			restoringFiles := l.checkPoint.GetRestoringFileInfo(db, table)
			l.logCtx.L().Debug("restoring table data", zap.String("schema", db), zap.String("table", table), zap.Reflect("data files", restoringFiles))

			info := l.tableInfos[tableName(db, table)]
			for _, file := range dataFiles {
				select {
				case <-ctx.Done():
					l.logCtx.L().Warn("stop generate data file job", log.ShortError(ctx.Err()))
					return ctx.Err()
				default:
					// do nothing
				}

				l.logCtx.L().Debug("dispatch data file", zap.String("schema", db), zap.String("table", table), zap.String("data file", file))

				var offset int64
				posSet, ok := restoringFiles[file]
				if ok {
					offset = posSet[0]
				}

				j := &fileJob{
					schema:   db,
					table:    table,
					dataFile: file,
					offset:   offset,
					info:     info,
				}
				dispatchMap[fmt.Sprintf("%s_%s_%s", db, table, file)] = j
			}
		}
	}
	l.logCtx.L().Info("finish to create tables", zap.Duration("cost time", time.Since(begin)))

	// a simple and naive approach to dispatch files randomly based on the feature of golang map(range by random)
	for _, j := range dispatchMap {
		select {
		case <-ctx.Done():
			l.logCtx.L().Warn("stop dispatch data file job", log.ShortError(ctx.Err()))
			return ctx.Err()
		case l.fileJobQueue <- j:
		}
	}

	l.logCtx.L().Info("all data files have been dispatched, waiting for them finished")
	return nil
}

// checkpointID returns ID which used for checkpoint table
func (l *Loader) checkpointID() string {
	if len(l.cfg.SourceID) > 0 {
		return l.cfg.SourceID
	}
	dir, err := filepath.Abs(l.cfg.Dir)
	if err != nil {
		l.logCtx.L().Warn("get abs dir", zap.String("directory", l.cfg.Dir), log.ShortError(err))
		return l.cfg.Dir
	}
	return shortSha1(dir)
}

func (l *Loader) getMydumpMetadata() error {
	metafile := filepath.Join(l.cfg.LoaderConfig.Dir, "metadata")
	pos, err := utils.ParseMetaData(metafile)
	if err != nil {
		l.logCtx.L().Error("fail to parse dump metadata", log.ShortError(err))
		return err
	}

	l.metaBinlog.Set(pos.String())
	return nil
}
