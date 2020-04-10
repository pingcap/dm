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
	"path"
	"sync"
	"time"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/conn"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"

	"github.com/pingcap/failpoint"
	tmysql "github.com/pingcap/parser/mysql"
	"github.com/siddontang/go-mysql/mysql"
	"go.uber.org/zap"
)

/*
variants about checkpoint:
1. update global checkpoint for DDL/XID event from any stream (global and sharding streaming)
2. update table checkpoint for DDL/DML event from any stream (global and sharding streaming)
3. position of global/table checkpoint increases monotonically
4. global checkpoint <= min checkpoint of all unsolved sharding tables
5. max checkpoint of all tables >= global checkpoint
*/

var (
	globalCpSchema       = "" // global checkpoint's cp_schema
	globalCpTable        = "" // global checkpoint's cp_table
	maxCheckPointTimeout = "1m"
	minCheckpoint        = mysql.Position{Pos: 4}
)

// NOTE: now we sync from relay log, so not add GTID support yet
type binlogPoint struct {
	sync.RWMutex
	mysql.Position

	flushedPos mysql.Position // pos which flushed permanently
}

func newBinlogPoint(pos mysql.Position, flushedPos mysql.Position) *binlogPoint {
	return &binlogPoint{
		Position:   pos,
		flushedPos: flushedPos,
	}
}

func (b *binlogPoint) save(pos mysql.Position) error {
	b.Lock()
	defer b.Unlock()
	if pos.Compare(b.Position) < 0 {
		// support to save equal pos, but not older pos
		return terror.ErrCheckpointSaveInvalidPos.Generate(pos, b.Position)
	}
	b.Position = pos
	return nil
}

func (b *binlogPoint) flush() {
	b.Lock()
	defer b.Unlock()
	b.flushedPos = b.Position
}

func (b *binlogPoint) rollback() {
	b.Lock()
	defer b.Unlock()
	b.Position = b.flushedPos
}

func (b *binlogPoint) outOfDate() bool {
	b.RLock()
	defer b.RUnlock()
	return b.Position.Compare(b.flushedPos) > 0
}

// MySQLPos returns point as mysql.Position
func (b *binlogPoint) MySQLPos() mysql.Position {
	b.RLock()
	defer b.RUnlock()
	return b.Position
}

// FlushedMySQLPos returns flushed point as mysql.Position
func (b *binlogPoint) FlushedMySQLPos() mysql.Position {
	b.RLock()
	defer b.RUnlock()
	return b.flushedPos
}

func (b *binlogPoint) String() string {
	b.RLock()
	defer b.RUnlock()

	return fmt.Sprintf("%v(flushed %v)", b.Position, b.flushedPos)
}

// CheckPoint represents checkpoints status for syncer
// including global binlog's checkpoint and every table's checkpoint
// when save checkpoint, we must differ saving in memory from saving (flushing) to DB (or file) permanently
// for sharding merging, we must save checkpoint in memory to support skip when re-syncing for the special streamer
// but before all DDLs for a sharding group to be synced and executed, we should not save checkpoint permanently
// because, when restarting to continue the sync, all sharding DDLs must try-sync again
type CheckPoint interface {
	// Init initializes the CheckPoint
	Init(tctx *tcontext.Context) error

	// Close closes the CheckPoint
	Close()

	// ResetConn resets database connections owned by the Checkpoint
	ResetConn(tctx *tcontext.Context) error

	// Clear clears all checkpoints
	Clear(tctx *tcontext.Context) error

	// Load loads all checkpoints saved by CheckPoint
	Load(tctx *tcontext.Context) error

	// LoadMeta loads checkpoints from meta config item or file
	LoadMeta() error

	// SaveTablePoint saves checkpoint for specified table in memory
	SaveTablePoint(sourceSchema, sourceTable string, pos mysql.Position)

	// DeleteTablePoint deletes checkpoint for specified table in memory and storage
	DeleteTablePoint(tctx *tcontext.Context, sourceSchema, sourceTable string) error

	// IsNewerTablePoint checks whether job's checkpoint is newer than previous saved checkpoint
	IsNewerTablePoint(sourceSchema, sourceTable string, pos mysql.Position) bool

	// SaveGlobalPoint saves the global binlog stream's checkpoint
	// corresponding to Meta.Save
	SaveGlobalPoint(pos mysql.Position)

	// FlushGlobalPointsExcept flushes the global checkpoint and tables'
	// checkpoints except exceptTables, it also flushes SQLs with Args providing
	// by extraSQLs and extraArgs. Currently extraSQLs contain shard meta only.
	// @exceptTables: [[schema, table]... ]
	// corresponding to Meta.Flush
	FlushPointsExcept(tctx *tcontext.Context, exceptTables [][]string, extraSQLs []string, extraArgs [][]interface{}) error

	// GlobalPoint returns the global binlog stream's checkpoint
	// corresponding to to Meta.Pos
	GlobalPoint() mysql.Position

	// TablePoint returns all table's stream checkpoint
	TablePoint() map[string]map[string]mysql.Position

	// FlushedGlobalPoint returns the flushed global binlog stream's checkpoint
	// corresponding to to Meta.Pos
	FlushedGlobalPoint() mysql.Position

	// CheckGlobalPoint checks whether we should save global checkpoint
	// corresponding to Meta.Check
	CheckGlobalPoint() bool

	// Rollback rolls global checkpoint and all table checkpoints back to flushed checkpoints
	Rollback()

	// String return text of global position
	String() string
}

// RemoteCheckPoint implements CheckPoint
// which using target database to store info
// NOTE: now we sync from relay log, so not add GTID support yet
// it's not thread-safe
type RemoteCheckPoint struct {
	sync.RWMutex

	cfg *config.SubTaskConfig

	db     *conn.BaseDB
	dbConn *DBConn
	schema string // schema name, set through task config
	table  string // table name, now it's task name
	id     string // checkpoint ID, now it is `source-id`

	// source-schema -> source-table -> checkpoint
	// used to filter the synced binlog when re-syncing for sharding group
	points map[string]map[string]*binlogPoint

	// global binlog checkpoint
	// after restarted, we can continue to re-sync from this point
	// if there are sharding groups waiting for DDL syncing or in DMLs re-syncing
	//   this global checkpoint is min(next-binlog-pos, min(all-syncing-sharding-group-first-pos))
	// else
	//   this global checkpoint is next-binlog-pos
	globalPoint         *binlogPoint
	globalPointSaveTime time.Time

	logCtx *tcontext.Context
}

// NewRemoteCheckPoint creates a new RemoteCheckPoint
func NewRemoteCheckPoint(tctx *tcontext.Context, cfg *config.SubTaskConfig, id string) CheckPoint {
	cp := &RemoteCheckPoint{
		cfg:         cfg,
		schema:      cfg.MetaSchema,
		table:       fmt.Sprintf("%s_syncer_checkpoint", cfg.Name),
		id:          id,
		points:      make(map[string]map[string]*binlogPoint),
		globalPoint: newBinlogPoint(minCheckpoint, minCheckpoint),
		logCtx:      tcontext.Background().WithLogger(tctx.L().WithFields(zap.String("component", "remote checkpoint"))),
	}

	return cp
}

// Init implements CheckPoint.Init
func (cp *RemoteCheckPoint) Init(tctx *tcontext.Context) error {
	checkPointDB := cp.cfg.To
	checkPointDB.RawDBCfg = config.DefaultRawDBConfig().SetReadTimeout(maxCheckPointTimeout)
	db, dbConns, err := createConns(tctx, cp.cfg, checkPointDB, 1)
	if err != nil {
		return err
	}
	cp.db = db
	cp.dbConn = dbConns[0]

	return cp.prepare(tctx)
}

// Close implements CheckPoint.Close
func (cp *RemoteCheckPoint) Close() {
	closeBaseDB(cp.logCtx, cp.db)
}

// ResetConn implements CheckPoint.ResetConn
func (cp *RemoteCheckPoint) ResetConn(tctx *tcontext.Context) error {
	return cp.dbConn.resetConn(tctx)
}

// Clear implements CheckPoint.Clear
func (cp *RemoteCheckPoint) Clear(tctx *tcontext.Context) error {
	cp.Lock()
	defer cp.Unlock()

	// delete all checkpoints
	sql2 := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE `id` = '%s'", cp.schema, cp.table, cp.id)
	args := make([]interface{}, 0)
	_, err := cp.dbConn.executeSQL(tctx, []string{sql2}, [][]interface{}{args}...)
	if err != nil {
		return err
	}

	cp.globalPoint = newBinlogPoint(minCheckpoint, minCheckpoint)

	cp.points = make(map[string]map[string]*binlogPoint)

	return nil
}

// SaveTablePoint implements CheckPoint.SaveTablePoint
func (cp *RemoteCheckPoint) SaveTablePoint(sourceSchema, sourceTable string, pos mysql.Position) {
	cp.Lock()
	defer cp.Unlock()
	cp.saveTablePoint(sourceSchema, sourceTable, pos)
}

// saveTablePoint saves single table's checkpoint without mutex.Lock
func (cp *RemoteCheckPoint) saveTablePoint(sourceSchema, sourceTable string, pos mysql.Position) {
	if cp.globalPoint.Compare(pos) > 0 {
		panic(fmt.Sprintf("table checkpoint %+v less than global checkpoint %+v", pos, cp.globalPoint))
	}

	// we save table checkpoint while we meet DDL or DML
	cp.logCtx.L().Debug("save table checkpoint", zap.Stringer("position", pos), zap.String("schema", sourceSchema), zap.String("table", sourceTable))
	mSchema, ok := cp.points[sourceSchema]
	if !ok {
		mSchema = make(map[string]*binlogPoint)
		cp.points[sourceSchema] = mSchema
	}
	point, ok := mSchema[sourceTable]
	if !ok {
		mSchema[sourceTable] = newBinlogPoint(pos, minCheckpoint)
	} else {
		if err := point.save(pos); err != nil {
			cp.logCtx.L().Error("fail to save table point", zap.String("schema", sourceSchema), zap.String("table", sourceTable), log.ShortError(err))
		}
	}
}

// DeleteTablePoint implements CheckPoint.DeleteTablePoint
func (cp *RemoteCheckPoint) DeleteTablePoint(tctx *tcontext.Context, sourceSchema, sourceTable string) error {
	cp.Lock()
	defer cp.Unlock()
	mSchema, ok := cp.points[sourceSchema]
	if !ok {
		return nil
	}
	_, ok = mSchema[sourceTable]
	if !ok {
		return nil
	}

	cp.logCtx.L().Info("delete table checkpoint", zap.String("schema", sourceSchema), zap.String("table", sourceTable))
	// delete  checkpoint
	sql2 := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE `id` = '%s' AND `cp_schema` = '%s' AND `cp_table` = '%s'", cp.schema, cp.table, cp.id, sourceSchema, sourceTable)
	args := make([]interface{}, 0)
	_, err := cp.dbConn.executeSQL(tctx, []string{sql2}, [][]interface{}{args}...)
	if err != nil {
		return err
	}
	delete(mSchema, sourceTable)
	return nil
}

// IsNewerTablePoint implements CheckPoint.IsNewerTablePoint
func (cp *RemoteCheckPoint) IsNewerTablePoint(sourceSchema, sourceTable string, pos mysql.Position) bool {
	cp.RLock()
	defer cp.RUnlock()
	mSchema, ok := cp.points[sourceSchema]
	if !ok {
		return true
	}
	point, ok := mSchema[sourceTable]
	if !ok {
		return true
	}
	oldPos := point.MySQLPos()
	return pos.Compare(oldPos) > 0
}

// SaveGlobalPoint implements CheckPoint.SaveGlobalPoint
func (cp *RemoteCheckPoint) SaveGlobalPoint(pos mysql.Position) {
	cp.Lock()
	defer cp.Unlock()

	cp.logCtx.L().Debug("save global checkpoint", zap.Stringer("position", pos))
	if err := cp.globalPoint.save(pos); err != nil {
		cp.logCtx.L().Error("fail to save global checkpoint", log.ShortError(err))
	}
}

// FlushPointsExcept implements CheckPoint.FlushPointsExcept
func (cp *RemoteCheckPoint) FlushPointsExcept(tctx *tcontext.Context, exceptTables [][]string, extraSQLs []string, extraArgs [][]interface{}) error {
	cp.RLock()
	defer cp.RUnlock()

	// convert slice to map
	excepts := make(map[string]map[string]struct{})
	for _, schemaTable := range exceptTables {
		schema, table := schemaTable[0], schemaTable[1]
		m, ok := excepts[schema]
		if !ok {
			m = make(map[string]struct{})
			excepts[schema] = m
		}
		m[table] = struct{}{}
	}

	sqls := make([]string, 0, 100)
	args := make([][]interface{}, 0, 100)

	if cp.globalPoint.outOfDate() {
		posG := cp.GlobalPoint()
		sqlG, argG := cp.genUpdateSQL(globalCpSchema, globalCpTable, posG.Name, posG.Pos, true)
		sqls = append(sqls, sqlG)
		args = append(args, argG)
	}

	points := make([]*binlogPoint, 0, 100)

	for schema, mSchema := range cp.points {
		for table, point := range mSchema {
			if _, ok1 := excepts[schema]; ok1 {
				if _, ok2 := excepts[schema][table]; ok2 {
					continue
				}
			}
			if point.outOfDate() {
				pos := point.MySQLPos()
				sql2, arg := cp.genUpdateSQL(schema, table, pos.Name, pos.Pos, false)
				sqls = append(sqls, sql2)
				args = append(args, arg)

				points = append(points, point)
			}
		}
	}
	for i := range extraSQLs {
		sqls = append(sqls, extraSQLs[i])
		args = append(args, extraArgs[i])
	}

	_, err := cp.dbConn.executeSQL(tctx, sqls, args...)
	if err != nil {
		return err
	}

	cp.globalPoint.flush()
	for _, point := range points {
		point.flush()
	}

	cp.globalPointSaveTime = time.Now()
	return nil
}

// GlobalPoint implements CheckPoint.GlobalPoint
func (cp *RemoteCheckPoint) GlobalPoint() mysql.Position {
	return cp.globalPoint.MySQLPos()
}

// TablePoint implements CheckPoint.TablePoint
func (cp *RemoteCheckPoint) TablePoint() map[string]map[string]mysql.Position {
	cp.RLock()
	defer cp.RUnlock()

	tablePoint := make(map[string]map[string]mysql.Position)
	for schema, tables := range cp.points {
		tablePoint[schema] = make(map[string]mysql.Position)
		for table, point := range tables {
			tablePoint[schema][table] = point.MySQLPos()
		}
	}
	return tablePoint
}

// FlushedGlobalPoint implements CheckPoint.FlushedGlobalPoint
func (cp *RemoteCheckPoint) FlushedGlobalPoint() mysql.Position {
	return cp.globalPoint.FlushedMySQLPos()
}

// String implements CheckPoint.String
func (cp *RemoteCheckPoint) String() string {
	return cp.globalPoint.String()
}

// CheckGlobalPoint implements CheckPoint.CheckGlobalPoint
func (cp *RemoteCheckPoint) CheckGlobalPoint() bool {
	cp.RLock()
	defer cp.RUnlock()
	return time.Since(cp.globalPointSaveTime) >= time.Duration(cp.cfg.CheckpointFlushInterval)*time.Second
}

// Rollback implements CheckPoint.Rollback
func (cp *RemoteCheckPoint) Rollback() {
	cp.RLock()
	defer cp.RUnlock()
	cp.globalPoint.rollback()
	for schema, mSchema := range cp.points {
		for table, point := range mSchema {
			cp.logCtx.L().Info("rollback checkpoint", log.WrapStringerField("checkpoint", point), zap.String("schema", schema), zap.String("table", table))
			point.rollback()
		}
	}
}

func (cp *RemoteCheckPoint) prepare(tctx *tcontext.Context) error {
	if err := cp.createSchema(tctx); err != nil {
		return err
	}

	if err := cp.createTable(tctx); err != nil {
		return err
	}
	return nil
}

func (cp *RemoteCheckPoint) createSchema(tctx *tcontext.Context) error {
	sql2 := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS `%s`", cp.schema)
	args := make([]interface{}, 0)
	_, err := cp.dbConn.executeSQL(tctx, []string{sql2}, [][]interface{}{args}...)
	cp.logCtx.L().Info("create checkpoint schema", zap.String("statement", sql2))
	return err
}

func (cp *RemoteCheckPoint) createTable(tctx *tcontext.Context) error {
	tableName := fmt.Sprintf("`%s`.`%s`", cp.schema, cp.table)
	sql2 := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id VARCHAR(32) NOT NULL,
			cp_schema VARCHAR(128) NOT NULL,
			cp_table VARCHAR(128) NOT NULL,
			binlog_name VARCHAR(128),
			binlog_pos INT UNSIGNED,
			is_global BOOLEAN,
			create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			UNIQUE KEY uk_id_schema_table (id, cp_schema, cp_table)
		)`, tableName)
	args := make([]interface{}, 0)
	_, err := cp.dbConn.executeSQL(tctx, []string{sql2}, [][]interface{}{args}...)
	cp.logCtx.L().Info("create checkpoint table", zap.String("statement", sql2))
	return err
}

// Load implements CheckPoint.Load
func (cp *RemoteCheckPoint) Load(tctx *tcontext.Context) error {
	query := fmt.Sprintf("SELECT `cp_schema`, `cp_table`, `binlog_name`, `binlog_pos`, `is_global` FROM `%s`.`%s` WHERE `id`='%s'", cp.schema, cp.table, cp.id)
	rows, err := cp.dbConn.querySQL(tctx, query)
	defer func() {
		if rows != nil {
			rows.Close()
		}
	}()

	failpoint.Inject("LoadCheckpointFailed", func(val failpoint.Value) {
		err = tmysql.NewErr(uint16(val.(int)))
		log.L().Warn("Load failed", zap.String("failpoint", "LoadCheckpointFailed"), zap.Error(err))
	})

	if err != nil {
		return terror.WithScope(err, terror.ScopeDownstream)
	}

	// checkpoints in DB have higher priority
	// if don't want to use checkpoint in DB, set `remove-previous-checkpoint` to `true`
	var (
		cpSchema   string
		cpTable    string
		binlogName string
		binlogPos  uint32
		isGlobal   bool
	)
	for rows.Next() {
		err := rows.Scan(&cpSchema, &cpTable, &binlogName, &binlogPos, &isGlobal)
		if err != nil {
			return terror.WithScope(terror.DBErrorAdapt(err, terror.ErrDBDriverError), terror.ScopeDownstream)
		}
		pos := mysql.Position{
			Name: binlogName,
			Pos:  binlogPos,
		}
		if isGlobal {
			if pos.Compare(minCheckpoint) > 0 {
				cp.globalPoint = newBinlogPoint(pos, pos)
				cp.logCtx.L().Info("fetch global checkpoint from DB", log.WrapStringerField("global checkpoint", cp.globalPoint))
			}
			continue // skip global checkpoint
		}
		mSchema, ok := cp.points[cpSchema]
		if !ok {
			mSchema = make(map[string]*binlogPoint)
			cp.points[cpSchema] = mSchema
		}
		mSchema[cpTable] = newBinlogPoint(pos, pos)
	}
	return terror.WithScope(terror.DBErrorAdapt(rows.Err(), terror.ErrDBDriverError), terror.ScopeDownstream)
}

// LoadMeta implements CheckPoint.LoadMeta
func (cp *RemoteCheckPoint) LoadMeta() error {
	var (
		pos *mysql.Position
		err error
	)
	switch cp.cfg.Mode {
	case config.ModeAll:
		// NOTE: syncer must continue the syncing follow loader's tail, so we parse mydumper's output
		// refine when master / slave switching added and checkpoint mechanism refactored
		pos, err = cp.parseMetaData()
		if err != nil {
			return err
		}
	case config.ModeIncrement:
		// load meta from task config
		if cp.cfg.Meta == nil {
			cp.logCtx.L().Warn("don't set meta in increment task-mode")
			return nil
		}
		pos = &mysql.Position{
			Name: cp.cfg.Meta.BinLogName,
			Pos:  cp.cfg.Meta.BinLogPos,
		}
	default:
		// should not go here (syncer is only used in `all` or `incremental` mode)
		return terror.ErrCheckpointInvalidTaskMode.Generate(cp.cfg.Mode)
	}

	// if meta loaded, we will start syncing from meta's pos
	if pos != nil {
		cp.globalPoint = newBinlogPoint(*pos, *pos)
		cp.logCtx.L().Info("loaded checkpoints from meta", log.WrapStringerField("global checkpoint", cp.globalPoint))
	}

	return nil
}

// genUpdateSQL generates SQL and arguments for update checkpoint
func (cp *RemoteCheckPoint) genUpdateSQL(cpSchema, cpTable string, binlogName string, binlogPos uint32, isGlobal bool) (string, []interface{}) {
	// use `INSERT INTO ... ON DUPLICATE KEY UPDATE` rather than `REPLACE INTO`
	// to keep `create_time`, `update_time` correctly
	sql2 := fmt.Sprintf("INSERT INTO `%s`.`%s` (`id`, `cp_schema`, `cp_table`, `binlog_name`, `binlog_pos`, `is_global`) VALUES(?,?,?,?,?,?) ON DUPLICATE KEY UPDATE `binlog_name`=?, `binlog_pos`=?",
		cp.schema, cp.table)
	if isGlobal {
		cpSchema = globalCpSchema
		cpTable = globalCpTable
	}
	args := []interface{}{cp.id, cpSchema, cpTable, binlogName, binlogPos, isGlobal, binlogName, binlogPos}
	return sql2, args
}

func (cp *RemoteCheckPoint) parseMetaData() (*mysql.Position, error) {
	// `metadata` is mydumper's output meta file name
	filename := path.Join(cp.cfg.Dir, "metadata")
	cp.logCtx.L().Info("parsing metadata from file", zap.String("file", filename))
	return utils.ParseMetaData(filename)
}
