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
	"database/sql"
	"encoding/json"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/binlog"
	"github.com/pingcap/dm/pkg/conn"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/cputil"
	"github.com/pingcap/dm/pkg/dumpling"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/schema"
	"github.com/pingcap/dm/pkg/terror"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/model"
	tmysql "github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
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
)

type binlogPoint struct {
	sync.RWMutex

	location binlog.Location
	ti       *model.TableInfo

	flushedLocation binlog.Location // location which flushed permanently
	flushedTI       *model.TableInfo

	enableGTID bool
}

func newBinlogPoint(location, flushedLocation binlog.Location, ti, flushedTI *model.TableInfo, enableGTID bool) *binlogPoint {
	return &binlogPoint{
		location:        location,
		ti:              ti,
		flushedLocation: flushedLocation,
		flushedTI:       flushedTI,
		enableGTID:      enableGTID,
	}
}

func (b *binlogPoint) save(location binlog.Location, ti *model.TableInfo) error {
	b.Lock()
	defer b.Unlock()

	if binlog.CompareLocation(location, b.location, b.enableGTID) < 0 {
		// support to save equal location, but not older location
		return terror.ErrCheckpointSaveInvalidPos.Generate(location, b.location)
	}

	b.location = location
	b.ti = ti
	return nil
}

func (b *binlogPoint) flush() {
	b.Lock()
	defer b.Unlock()
	b.flushedLocation = b.location.Clone()
	b.flushedTI = b.ti
}

func (b *binlogPoint) rollback(schemaTracker *schema.Tracker, schema string) (isSchemaChanged bool) {
	b.Lock()
	defer b.Unlock()

	// set suffix to 0 when we meet error
	b.flushedLocation.ResetSuffix()
	b.location = b.flushedLocation.Clone()
	if b.ti == nil {
		return // for global checkpoint, no need to rollback the schema.
	}

	// NOTE: no `Equal` function for `model.TableInfo` exists now, so we compare `pointer` directly,
	// and after a new DDL applied to the schema, the returned pointer of `model.TableInfo` changed now.
	trackedTi, _ := schemaTracker.GetTable(schema, b.ti.Name.O) // ignore the returned error, only compare `trackerTi` is enough.
	// may three versions of schema exist:
	// - the one tracked in the TiDB-with-mockTiKV.
	// - the one in the checkpoint but not flushed.
	// - the one in the checkpoint and flushed.
	// if any of them are not equal, then we rollback them:
	// - set the one in the checkpoint but not flushed to the one flushed.
	// - set the one tracked to the one in the checkpoint by the caller of this method (both flushed and not flushed are the same now)
	if isSchemaChanged = (trackedTi != b.ti) || (b.ti != b.flushedTI); isSchemaChanged {
		b.ti = b.flushedTI
	}
	return
}

func (b *binlogPoint) outOfDate() bool {
	b.RLock()
	defer b.RUnlock()

	return binlog.CompareLocation(b.location, b.flushedLocation, b.enableGTID) > 0
}

// MySQLLocation returns point as binlog.Location
func (b *binlogPoint) MySQLLocation() binlog.Location {
	b.RLock()
	defer b.RUnlock()
	return b.location.Clone()
}

// FlushedMySQLLocation returns flushed point as binlog.Location
func (b *binlogPoint) FlushedMySQLLocation() binlog.Location {
	b.RLock()
	defer b.RUnlock()
	return b.flushedLocation.Clone()
}

// TableInfo returns the table schema associated at the current binlog position.
func (b *binlogPoint) TableInfo() *model.TableInfo {
	b.RLock()
	defer b.RUnlock()
	return b.ti
}

func (b *binlogPoint) String() string {
	b.RLock()
	defer b.RUnlock()

	return fmt.Sprintf("%v(flushed %v)", b.location, b.flushedLocation)
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
	Load(tctx *tcontext.Context, schemaTracker *schema.Tracker) error

	// LoadMeta loads checkpoints from meta config item or file
	LoadMeta() error

	// SaveTablePoint saves checkpoint for specified table in memory
	SaveTablePoint(sourceSchema, sourceTable string, point binlog.Location, ti *model.TableInfo)

	// DeleteTablePoint deletes checkpoint for specified table in memory and storage
	DeleteTablePoint(tctx *tcontext.Context, sourceSchema, sourceTable string) error

	// DeleteSchemaPoint deletes checkpoint for specified schema
	DeleteSchemaPoint(tctx *tcontext.Context, sourceSchema string) error

	// IsNewerTablePoint checks whether job's checkpoint is newer than previous saved checkpoint
	IsNewerTablePoint(sourceSchema, sourceTable string, point binlog.Location, gte bool) bool

	// SaveGlobalPoint saves the global binlog stream's checkpoint
	// corresponding to Meta.Save
	SaveGlobalPoint(point binlog.Location)

	// FlushGlobalPointsExcept flushes the global checkpoint and tables'
	// checkpoints except exceptTables, it also flushes SQLs with Args providing
	// by extraSQLs and extraArgs. Currently extraSQLs contain shard meta only.
	// @exceptTables: [[schema, table]... ]
	// corresponding to Meta.Flush
	FlushPointsExcept(tctx *tcontext.Context, exceptTables [][]string, extraSQLs []string, extraArgs [][]interface{}) error

	// GlobalPoint returns the global binlog stream's checkpoint
	// corresponding to Meta.Pos and Meta.GTID
	GlobalPoint() binlog.Location

	// TablePoint returns all table's stream checkpoint
	TablePoint() map[string]map[string]binlog.Location

	// FlushedGlobalPoint returns the flushed global binlog stream's checkpoint
	// corresponding to to Meta.Pos and gtid
	FlushedGlobalPoint() binlog.Location

	// CheckGlobalPoint checks whether we should save global checkpoint
	// corresponding to Meta.Check
	CheckGlobalPoint() bool

	// Rollback rolls global checkpoint and all table checkpoints back to flushed checkpoints
	Rollback(schemaTracker *schema.Tracker)

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

	db        *conn.BaseDB
	dbConn    *DBConn
	tableName string // qualified table name: schema is set through task config, table is task name
	id        string // checkpoint ID, now it is `source-id`

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
		tableName:   dbutil.TableName(cfg.MetaSchema, cputil.SyncerCheckpoint(cfg.Name)),
		id:          id,
		points:      make(map[string]map[string]*binlogPoint),
		globalPoint: newBinlogPoint(binlog.NewLocation(cfg.Flavor), binlog.NewLocation(cfg.Flavor), nil, nil, cfg.EnableGTID),
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
	_, err := cp.dbConn.executeSQL(
		tctx,
		[]string{`DELETE FROM ` + cp.tableName + ` WHERE id = ?`},
		[]interface{}{cp.id},
	)
	if err != nil {
		return err
	}

	cp.globalPoint = newBinlogPoint(binlog.NewLocation(cp.cfg.Flavor), binlog.NewLocation(cp.cfg.Flavor), nil, nil, cp.cfg.EnableGTID)
	cp.globalPointSaveTime = time.Time{}
	cp.points = make(map[string]map[string]*binlogPoint)

	return nil
}

// SaveTablePoint implements CheckPoint.SaveTablePoint
func (cp *RemoteCheckPoint) SaveTablePoint(sourceSchema, sourceTable string, point binlog.Location, ti *model.TableInfo) {
	cp.Lock()
	defer cp.Unlock()
	cp.saveTablePoint(sourceSchema, sourceTable, point.Clone(), ti)
}

// saveTablePoint saves single table's checkpoint without mutex.Lock
func (cp *RemoteCheckPoint) saveTablePoint(sourceSchema, sourceTable string, location binlog.Location, ti *model.TableInfo) {
	if binlog.CompareLocation(cp.globalPoint.location, location, cp.cfg.EnableGTID) > 0 {
		panic(fmt.Sprintf("table checkpoint %+v less than global checkpoint %+v", location, cp.globalPoint))
	}

	// we save table checkpoint while we meet DDL or DML
	cp.logCtx.L().Debug("save table checkpoint", zap.Stringer("location", location), zap.String("schema", sourceSchema), zap.String("table", sourceTable))
	mSchema, ok := cp.points[sourceSchema]
	if !ok {
		mSchema = make(map[string]*binlogPoint)
		cp.points[sourceSchema] = mSchema
	}
	point, ok := mSchema[sourceTable]
	if !ok {
		mSchema[sourceTable] = newBinlogPoint(location, binlog.NewLocation(cp.cfg.Flavor), ti, nil, cp.cfg.EnableGTID)
	} else if err := point.save(location, ti); err != nil {
		cp.logCtx.L().Error("fail to save table point", zap.String("schema", sourceSchema), zap.String("table", sourceTable), log.ShortError(err))
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
	_, err := cp.dbConn.executeSQL(
		tctx,
		[]string{`DELETE FROM ` + cp.tableName + ` WHERE id = ? AND cp_schema = ? AND cp_table = ?`},
		[]interface{}{cp.id, sourceSchema, sourceTable},
	)
	if err != nil {
		return err
	}
	delete(mSchema, sourceTable)
	return nil
}

// DeleteSchemaPoint implements CheckPoint.DeleteSchemaPoint
func (cp *RemoteCheckPoint) DeleteSchemaPoint(tctx *tcontext.Context, sourceSchema string) error {
	cp.Lock()
	defer cp.Unlock()
	_, ok := cp.points[sourceSchema]
	if !ok {
		return nil
	}

	cp.logCtx.L().Info("delete schema checkpoint", zap.String("schema", sourceSchema))
	_, err := cp.dbConn.executeSQL(
		tctx,
		[]string{`DELETE FROM ` + cp.tableName + ` WHERE id = ? AND cp_schema = ?`},
		[]interface{}{cp.id, sourceSchema},
	)
	if err != nil {
		return err
	}
	delete(cp.points, sourceSchema)
	return nil
}

// IsNewerTablePoint implements CheckPoint.IsNewerTablePoint.
// gte means greater than or equal, gte should judge by EnableGTID and the event type
// - when enable GTID and binlog is DML, go-mysql will only update GTID set in a XID event after the rows event, for example, the binlog events are:
//   - Query event, location is gset1
//   - Rows event, location is gset1
//   - XID event, location is gset2
//   after syncer handle query event, will save table point with gset1, and when handle rows event, will compare the rows's location with table checkpoint's location in `IsNewerTablePoint`, and these two location have same gset, so we should use `>=` to compare location in this case.
// - when enable GTID and binlog is DDL, different DDL have different GTID set, so if GTID set is euqal, it is a old table point, should use `>` to compare location in this case.
// - when not enable GTID, just compare the position, and only when grater than the old point is newer table point, should use `>` to compare location is this case.
func (cp *RemoteCheckPoint) IsNewerTablePoint(sourceSchema, sourceTable string, location binlog.Location, gte bool) bool {
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
	oldLocation := point.MySQLLocation()
	cp.logCtx.L().Debug("compare table location whether is newer", zap.Stringer("location", location), zap.Stringer("old location", oldLocation))

	if gte {
		return binlog.CompareLocation(location, oldLocation, cp.cfg.EnableGTID) >= 0
	}

	return binlog.CompareLocation(location, oldLocation, cp.cfg.EnableGTID) > 0
}

// SaveGlobalPoint implements CheckPoint.SaveGlobalPoint
func (cp *RemoteCheckPoint) SaveGlobalPoint(location binlog.Location) {
	cp.Lock()
	defer cp.Unlock()

	cp.logCtx.L().Debug("save global checkpoint", zap.Stringer("location", location))
	if err := cp.globalPoint.save(location.Clone(), nil); err != nil {
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

	if cp.globalPoint.outOfDate() || cp.globalPointSaveTime.IsZero() {
		locationG := cp.GlobalPoint()
		sqlG, argG := cp.genUpdateSQL(globalCpSchema, globalCpTable, locationG, nil, true)
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
				tiBytes, err := json.Marshal(point.ti)
				if err != nil {
					return terror.ErrSchemaTrackerCannotSerialize.Delegate(err, schema, table)
				}

				location := point.MySQLLocation()
				sql2, arg := cp.genUpdateSQL(schema, table, location, tiBytes, false)
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
func (cp *RemoteCheckPoint) GlobalPoint() binlog.Location {
	cp.RLock()
	defer cp.RUnlock()

	return cp.globalPoint.MySQLLocation()
}

// TablePoint implements CheckPoint.TablePoint
func (cp *RemoteCheckPoint) TablePoint() map[string]map[string]binlog.Location {
	cp.RLock()
	defer cp.RUnlock()

	tablePoint := make(map[string]map[string]binlog.Location)
	for schema, tables := range cp.points {
		tablePoint[schema] = make(map[string]binlog.Location)
		for table, point := range tables {
			tablePoint[schema][table] = point.MySQLLocation()
		}
	}
	return tablePoint
}

// FlushedGlobalPoint implements CheckPoint.FlushedGlobalPoint
func (cp *RemoteCheckPoint) FlushedGlobalPoint() binlog.Location {
	cp.RLock()
	defer cp.RUnlock()

	return cp.globalPoint.FlushedMySQLLocation()
}

// String implements CheckPoint.String
func (cp *RemoteCheckPoint) String() string {
	cp.RLock()
	defer cp.RUnlock()

	return cp.globalPoint.String()
}

// CheckGlobalPoint implements CheckPoint.CheckGlobalPoint
func (cp *RemoteCheckPoint) CheckGlobalPoint() bool {
	cp.RLock()
	defer cp.RUnlock()
	return time.Since(cp.globalPointSaveTime) >= time.Duration(cp.cfg.CheckpointFlushInterval)*time.Second
}

// Rollback implements CheckPoint.Rollback
func (cp *RemoteCheckPoint) Rollback(schemaTracker *schema.Tracker) {
	cp.RLock()
	defer cp.RUnlock()
	cp.globalPoint.rollback(schemaTracker, "")
	for schema, mSchema := range cp.points {
		for table, point := range mSchema {
			logger := cp.logCtx.L().WithFields(zap.String("schema", schema), zap.String("table", table))
			logger.Debug("try to rollback checkpoint", log.WrapStringerField("checkpoint", point))
			from := point.MySQLLocation()
			if point.rollback(schemaTracker, schema) {
				logger.Info("rollback checkpoint", zap.Stringer("from", from), zap.Stringer("to", point.FlushedMySQLLocation()))
				// schema changed
				if err := schemaTracker.DropTable(schema, table); err != nil {
					logger.Warn("failed to drop table from schema tracker", log.ShortError(err))
				}
				if point.ti != nil {
					// TODO: Figure out how to recover from errors.
					if err := schemaTracker.CreateSchemaIfNotExists(schema); err != nil {
						logger.Error("failed to rollback schema on schema tracker: cannot create schema", log.ShortError(err))
					}
					if err := schemaTracker.CreateTableIfNotExists(schema, table, point.ti); err != nil {
						logger.Error("failed to rollback schema on schema tracker: cannot create table", log.ShortError(err))
					}
				}
			}
		}
	}

	// drop any tables in the tracker if no corresponding checkpoint exists.
	for _, schema := range schemaTracker.AllSchemas() {
		_, ok1 := cp.points[schema.Name.O]
		for _, table := range schema.Tables {
			var ok2 bool
			if ok1 {
				_, ok2 = cp.points[schema.Name.O][table.Name.O]
			}
			if !ok2 {
				err := schemaTracker.DropTable(schema.Name.O, table.Name.O)
				cp.logCtx.L().Info("drop table in schema tracker because no checkpoint exists", zap.String("schema", schema.Name.O), zap.String("table", table.Name.O), log.ShortError(err))
			}
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
	// TODO(lance6716): change ColumnName to IdentName or something
	sql2 := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", dbutil.ColumnName(cp.cfg.MetaSchema))
	args := make([]interface{}, 0)
	_, err := cp.dbConn.executeSQL(tctx, []string{sql2}, [][]interface{}{args}...)
	cp.logCtx.L().Info("create checkpoint schema", zap.String("statement", sql2))
	return err
}

func (cp *RemoteCheckPoint) createTable(tctx *tcontext.Context) error {
	sqls := []string{
		`CREATE TABLE IF NOT EXISTS ` + cp.tableName + ` (
			id VARCHAR(32) NOT NULL,
			cp_schema VARCHAR(128) NOT NULL,
			cp_table VARCHAR(128) NOT NULL,
			binlog_name VARCHAR(128),
			binlog_pos INT UNSIGNED,
			binlog_gtid TEXT,
			table_info JSON NOT NULL,
			is_global BOOLEAN,
			create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			UNIQUE KEY uk_id_schema_table (id, cp_schema, cp_table)
		)`,
	}
	_, err := cp.dbConn.executeSQL(tctx, sqls)
	cp.logCtx.L().Info("create checkpoint table", zap.Strings("statements", sqls))
	return err
}

// Load implements CheckPoint.Load
func (cp *RemoteCheckPoint) Load(tctx *tcontext.Context, schemaTracker *schema.Tracker) error {
	cp.Lock()
	defer cp.Unlock()

	query := `SELECT cp_schema, cp_table, binlog_name, binlog_pos, binlog_gtid, table_info, is_global FROM ` + cp.tableName + ` WHERE id = ?`
	rows, err := cp.dbConn.querySQL(tctx, query, cp.id)
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
		cpSchema      string
		cpTable       string
		binlogName    string
		binlogPos     uint32
		binlogGTIDSet sql.NullString
		tiBytes       []byte
		isGlobal      bool
	)
	for rows.Next() {
		err := rows.Scan(&cpSchema, &cpTable, &binlogName, &binlogPos, &binlogGTIDSet, &tiBytes, &isGlobal)
		if err != nil {
			return terror.WithScope(terror.DBErrorAdapt(err, terror.ErrDBDriverError), terror.ScopeDownstream)
		}

		gset, err := gtid.ParserGTID(cp.cfg.Flavor, binlogGTIDSet.String) // default to "".
		if err != nil {
			return err
		}

		location := binlog.Location{
			Position: mysql.Position{
				Name: binlogName,
				Pos:  binlogPos,
			},
			GTIDSet: gset,
		}
		if isGlobal {
			if binlog.CompareLocation(location, binlog.NewLocation(cp.cfg.Flavor), cp.cfg.EnableGTID) > 0 {
				cp.globalPoint = newBinlogPoint(location.Clone(), location.Clone(), nil, nil, cp.cfg.EnableGTID)
				cp.logCtx.L().Info("fetch global checkpoint from DB", log.WrapStringerField("global checkpoint", cp.globalPoint))
			}
			continue // skip global checkpoint
		}

		var ti model.TableInfo
		if !bytes.Equal(tiBytes, []byte("null")) {
			// only create table if `table_info` is not `null`.
			if err = json.Unmarshal(tiBytes, &ti); err != nil {
				return terror.ErrSchemaTrackerInvalidJSON.Delegate(err, cpSchema, cpTable)
			}

			if schemaTracker != nil {
				if err = schemaTracker.CreateSchemaIfNotExists(cpSchema); err != nil {
					return terror.ErrSchemaTrackerCannotCreateSchema.Delegate(err, cpSchema)
				}
				if err = schemaTracker.CreateTableIfNotExists(cpSchema, cpTable, &ti); err != nil {
					return terror.ErrSchemaTrackerCannotCreateTable.Delegate(err, cpSchema, cpTable)
				}
			}
		}

		mSchema, ok := cp.points[cpSchema]
		if !ok {
			mSchema = make(map[string]*binlogPoint)
			cp.points[cpSchema] = mSchema
		}
		mSchema[cpTable] = newBinlogPoint(location.Clone(), location.Clone(), &ti, &ti, cp.cfg.EnableGTID)
	}

	return terror.WithScope(terror.DBErrorAdapt(rows.Err(), terror.ErrDBDriverError), terror.ScopeDownstream)
}

// LoadMeta implements CheckPoint.LoadMeta
func (cp *RemoteCheckPoint) LoadMeta() error {
	cp.Lock()
	defer cp.Unlock()

	var (
		location        *binlog.Location
		safeModeExitLoc *binlog.Location
		err             error
	)
	switch cp.cfg.Mode {
	case config.ModeAll:
		// NOTE: syncer must continue the syncing follow loader's tail, so we parse mydumper's output
		// refine when master / slave switching added and checkpoint mechanism refactored
		location, safeModeExitLoc, err = cp.parseMetaData()
		if err != nil {
			return err
		}
	case config.ModeIncrement:
		// load meta from task config
		if cp.cfg.Meta == nil {
			cp.logCtx.L().Warn("don't set meta in increment task-mode")
			cp.globalPoint = newBinlogPoint(binlog.NewLocation(cp.cfg.Flavor), binlog.NewLocation(cp.cfg.Flavor), nil, nil, cp.cfg.EnableGTID)
			return nil
		}
		gset, err := gtid.ParserGTID(cp.cfg.Flavor, cp.cfg.Meta.BinLogGTID)
		if err != nil {
			return err
		}

		location = &binlog.Location{
			Position: mysql.Position{
				Name: cp.cfg.Meta.BinLogName,
				Pos:  cp.cfg.Meta.BinLogPos,
			},
			GTIDSet: gset,
		}
	default:
		// should not go here (syncer is only used in `all` or `incremental` mode)
		return terror.ErrCheckpointInvalidTaskMode.Generate(cp.cfg.Mode)
	}

	// if meta loaded, we will start syncing from meta's pos
	if location != nil {
		cp.globalPoint = newBinlogPoint(location.Clone(), location.Clone(), nil, nil, cp.cfg.EnableGTID)
		cp.logCtx.L().Info("loaded checkpoints from meta", log.WrapStringerField("global checkpoint", cp.globalPoint))
	}
	if safeModeExitLoc != nil {
		cp.cfg.SafeModeExitLoc = safeModeExitLoc
		cp.logCtx.L().Info("set SafeModeExitLoc from meta", zap.Stringer("SafeModeExitLoc", safeModeExitLoc))
	}

	return nil
}

// genUpdateSQL generates SQL and arguments for update checkpoint
func (cp *RemoteCheckPoint) genUpdateSQL(cpSchema, cpTable string, location binlog.Location, tiBytes []byte, isGlobal bool) (string, []interface{}) {
	// use `INSERT INTO ... ON DUPLICATE KEY UPDATE` rather than `REPLACE INTO`
	// to keep `create_time`, `update_time` correctly
	sql2 := `INSERT INTO ` + cp.tableName + `
		(id, cp_schema, cp_table, binlog_name, binlog_pos, binlog_gtid, table_info, is_global) VALUES
		(?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			binlog_name = VALUES(binlog_name),
			binlog_pos = VALUES(binlog_pos),
			binlog_gtid = VALUES(binlog_gtid),
			table_info = VALUES(table_info),
			is_global = VALUES(is_global);
	`

	if isGlobal {
		cpSchema = globalCpSchema
		cpTable = globalCpTable
	}

	if len(tiBytes) == 0 {
		tiBytes = []byte("null")
	}

	args := []interface{}{cp.id, cpSchema, cpTable, location.Position.Name, location.Position.Pos, location.GTIDSetStr(), tiBytes, isGlobal}
	return sql2, args
}

func (cp *RemoteCheckPoint) parseMetaData() (*binlog.Location, *binlog.Location, error) {
	// `metadata` is mydumper's output meta file name
	filename := path.Join(cp.cfg.Dir, "metadata")
	cp.logCtx.L().Info("parsing metadata from file", zap.String("file", filename))

	return dumpling.ParseMetaData(filename, cp.cfg.Flavor)
}
