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
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/conn"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"

	"go.uber.org/zap"
)

// CheckPoint represents checkpoint status
type CheckPoint interface {
	// Load loads all checkpoints recorded before.
	// because of no checkpoints updated in memory when error occurred
	// when resuming, Load will be called again to load checkpoints
	Load() error

	// GetRestoringFileInfo get restoring data files for table
	GetRestoringFileInfo(db, table string) map[string][]int64

	// GetAllRestoringFileInfo return all restoring files position
	GetAllRestoringFileInfo() map[string][]int64

	// IsTableFinished query if table has finished
	IsTableFinished(db, table string) bool

	// CalcProgress calculate which table has finished and which table partial restored
	CalcProgress(allFiles map[string]Tables2DataFiles) error

	// Init initialize checkpoint data in tidb
	Init(filename string, endpos int64) error

	// Close closes the CheckPoint
	Close()

	// Clear clears all recorded checkpoints
	Clear() error

	// Count returns recorded checkpoints' count
	Count() (int, error)

	// GenSQL generates sql to update checkpoint to DB
	GenSQL(filename string, offset int64) string
}

// RemoteCheckPoint implements CheckPoint by saving status in remote database system, mostly in TiDB.
type RemoteCheckPoint struct {
	db             *conn.BaseDB
	conn           *WorkerConn // NOTE: use dbutil in tidb-tools later
	id             string
	schema         string
	table          string
	restoringFiles map[string]map[string]FilePosSet
	finishedTables map[string]struct{}
	tctx           *tcontext.Context
}

func newRemoteCheckPoint(tctx *tcontext.Context, cfg *config.SubTaskConfig, id string) (CheckPoint, error) {
	db, conn, err := createConn(tctx.Context(), cfg)
	if err != nil {
		return nil, err
	}

	newtctx := tctx.WithLogger(tctx.L().WithFields(zap.String("component", "remote checkpoint")))

	cp := &RemoteCheckPoint{
		db:             db,
		conn:           conn,
		id:             id,
		restoringFiles: make(map[string]map[string]FilePosSet),
		finishedTables: make(map[string]struct{}),
		schema:         cfg.MetaSchema,
		table:          fmt.Sprintf("%s_loader_checkpoint", cfg.Name),
		tctx:           newtctx,
	}

	err = cp.prepare()
	if err != nil {
		return nil, err
	}

	return cp, nil
}

func (cp *RemoteCheckPoint) prepare() error {
	// create schema
	if err := cp.createSchema(); err != nil {
		return err
	}
	// create table
	if err := cp.createTable(); err != nil {
		return err
	}
	return nil
}

func (cp *RemoteCheckPoint) createSchema() error {
	sql2 := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS `%s`", cp.schema)
	err := cp.conn.executeSQL(cp.tctx, []string{sql2})
	return terror.WithScope(err, terror.ScopeDownstream)
}

func (cp *RemoteCheckPoint) createTable() error {
	tableName := fmt.Sprintf("`%s`.`%s`", cp.schema, cp.table)
	createTable := `CREATE TABLE IF NOT EXISTS %s (
		id char(32) NOT NULL,
		filename varchar(255) NOT NULL,
		cp_schema varchar(128) NOT NULL,
		cp_table varchar(128) NOT NULL,
		offset bigint NOT NULL,
		end_pos bigint NOT NULL,
		create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		UNIQUE KEY uk_id_f (id,filename)
	);
`
	sql2 := fmt.Sprintf(createTable, tableName)
	err := cp.conn.executeSQL(cp.tctx, []string{sql2})
	return terror.WithScope(err, terror.ScopeDownstream)
}

// Load implements CheckPoint.Load
func (cp *RemoteCheckPoint) Load() error {
	begin := time.Now()
	defer func() {
		cp.tctx.L().Info("load checkpoint", zap.Duration("cost time", time.Since(begin)))
	}()

	query := fmt.Sprintf("SELECT `filename`,`cp_schema`,`cp_table`,`offset`,`end_pos` from `%s`.`%s` where `id`=?", cp.schema, cp.table)
	rows, err := cp.conn.querySQL(cp.tctx, query, cp.id)
	if err != nil {
		return terror.WithScope(err, terror.ScopeDownstream)
	}
	defer rows.Close()

	var (
		filename string
		schema   string
		table    string
		offset   int64
		endPos   int64
	)

	cp.restoringFiles = make(map[string]map[string]FilePosSet) // reset to empty
	for rows.Next() {
		err := rows.Scan(&filename, &schema, &table, &offset, &endPos)
		if err != nil {
			return terror.WithScope(terror.DBErrorAdapt(err, terror.ErrDBDriverError), terror.ScopeDownstream)
		}

		if _, ok := cp.restoringFiles[schema]; !ok {
			cp.restoringFiles[schema] = make(map[string]FilePosSet)
		}
		tables := cp.restoringFiles[schema]
		if _, ok := tables[table]; !ok {
			tables[table] = make(map[string][]int64)
		}
		restoringFiles := tables[table]
		restoringFiles[filename] = []int64{offset, endPos}
	}

	return terror.WithScope(terror.DBErrorAdapt(rows.Err(), terror.ErrDBDriverError), terror.ScopeDownstream)
}

// GetRestoringFileInfo implements CheckPoint.GetRestoringFileInfo
func (cp *RemoteCheckPoint) GetRestoringFileInfo(db, table string) map[string][]int64 {
	if tables, ok := cp.restoringFiles[db]; ok {
		if restoringFiles, ok := tables[table]; ok {
			return restoringFiles
		}
	}
	return make(map[string][]int64)
}

// GetAllRestoringFileInfo implements CheckPoint.GetAllRestoringFileInfo
func (cp *RemoteCheckPoint) GetAllRestoringFileInfo() map[string][]int64 {
	results := make(map[string][]int64)
	for _, tables := range cp.restoringFiles {
		for _, files := range tables {
			for file, pos := range files {
				results[file] = pos
			}
		}
	}
	return results
}

// IsTableFinished implements CheckPoint.IsTableFinished
func (cp *RemoteCheckPoint) IsTableFinished(db, table string) bool {
	key := strings.Join([]string{db, table}, ".")
	if _, ok := cp.finishedTables[key]; ok {
		return true
	}
	return false
}

// CalcProgress implements CheckPoint.CalcProgress
func (cp *RemoteCheckPoint) CalcProgress(allFiles map[string]Tables2DataFiles) error {
	cp.finishedTables = make(map[string]struct{}) // reset to empty
	for db, tables := range cp.restoringFiles {
		dbTables, ok := allFiles[db]
		if !ok {
			return terror.ErrCheckpointDBNotExistInFile.Generate(db)
		}

		for table, restoringFiles := range tables {
			files, ok := dbTables[table]
			if !ok {
				return terror.ErrCheckpointTableNotExistInFile.Generate(table, db)
			}

			restoringCount := len(restoringFiles)
			totalCount := len(files)

			t := strings.Join([]string{db, table}, ".")
			if restoringCount == totalCount {
				// compare offset.
				if cp.allFilesFinished(restoringFiles) {
					cp.finishedTables[t] = struct{}{}
				}
			} else if restoringCount > totalCount {
				return terror.ErrCheckpointRestoreCountGreater.Generate(table)
			}
		}
	}

	cp.tctx.L().Info("calculate checkpoint finished.", zap.Reflect("finished tables", cp.finishedTables))
	return nil
}

func (cp *RemoteCheckPoint) allFilesFinished(files map[string][]int64) bool {
	for file, pos := range files {
		if len(pos) != 2 {
			cp.tctx.L().Error("unexpected checkpoint record", zap.String("data file", file), zap.Int64s("position", pos))
			return false
		}
		if pos[0] != pos[1] {
			return false
		}
	}
	return true
}

// Init implements CheckPoint.Init
func (cp *RemoteCheckPoint) Init(filename string, endPos int64) error {
	idx := strings.Index(filename, ".sql")
	if idx < 0 {
		return terror.ErrCheckpointInvalidTableFile.Generate(filename)
	}
	fname := filename[:idx]
	fields := strings.Split(fname, ".")
	if len(fields) != 2 && len(fields) != 3 {
		return terror.ErrCheckpointInvalidTableFile.Generate(filename)
	}

	// fields[0] -> db name, fields[1] -> table name
	sql2 := fmt.Sprintf("INSERT INTO `%s`.`%s` (`id`, `filename`, `cp_schema`, `cp_table`, `offset`, `end_pos`) VALUES(?,?,?,?,?,?)", cp.schema, cp.table)
	cp.tctx.L().Debug("initial checkpoint record",
		zap.String("sql", sql2),
		zap.String("id", cp.id),
		zap.String("filename", filename),
		zap.String("schema", fields[0]),
		zap.String("table", fields[1]),
		zap.Int64("offset", 0),
		zap.Int64("end position", endPos))
	args := []interface{}{cp.id, filename, fields[0], fields[1], 0, endPos}
	err := cp.conn.executeSQL(cp.tctx, []string{sql2}, args)
	if err != nil {
		if isErrDupEntry(err) {
			cp.tctx.L().Info("checkpoint record already exists, skip it.", zap.String("id", cp.id), zap.String("filename", filename))
			return nil
		}
		return terror.WithScope(terror.Annotate(err, "initialize checkpoint"), terror.ScopeDownstream)
	}
	return nil
}

// Close implements CheckPoint.Close
func (cp *RemoteCheckPoint) Close() {
	err := cp.conn.Close()
	if err != nil {
		cp.tctx.L().Error("close checkpoint connection", log.ShortError(err))
	}
	err = cp.db.Close()
	if err != nil {
		cp.tctx.L().Error("close checkpoint db", log.ShortError(err))
	}
}

// GenSQL implements CheckPoint.GenSQL
func (cp *RemoteCheckPoint) GenSQL(filename string, offset int64) string {
	sql := fmt.Sprintf("UPDATE `%s`.`%s` SET `offset`=%d WHERE `id` ='%s' AND `filename`='%s';",
		cp.schema, cp.table, offset, cp.id, filename)
	return sql
}

// Clear implements CheckPoint.Clear
func (cp *RemoteCheckPoint) Clear() error {
	sql2 := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE `id` = '%s'", cp.schema, cp.table, cp.id)
	err := cp.conn.executeSQL(cp.tctx, []string{sql2})
	return terror.WithScope(err, terror.ScopeDownstream)
}

// Count implements CheckPoint.Count
func (cp *RemoteCheckPoint) Count() (int, error) {
	query := fmt.Sprintf("SELECT COUNT(id) FROM `%s`.`%s` WHERE `id` = ?", cp.schema, cp.table)
	rows, err := cp.conn.querySQL(cp.tctx, query, cp.id)
	if err != nil {
		return 0, terror.WithScope(err, terror.ScopeDownstream)
	}
	defer rows.Close()
	var count = 0
	for rows.Next() {
		err = rows.Scan(&count)
		if err != nil {
			return 0, terror.WithScope(terror.DBErrorAdapt(err, terror.ErrDBDriverError), terror.ScopeDownstream)
		}
	}
	if rows.Err() != nil {
		return 0, terror.WithScope(terror.DBErrorAdapt(rows.Err(), terror.ErrDBDriverError), terror.ScopeDownstream)
	}
	cp.tctx.L().Debug("checkpoint record", zap.Int("count", count))
	return count, nil
}

func (cp *RemoteCheckPoint) String() string {
	if err := cp.Load(); err != nil {
		return err.Error()
	}

	result := make(map[string][]int64)
	for _, tables := range cp.restoringFiles {
		for _, files := range tables {
			for file, set := range files {
				result[file] = set
			}
		}
	}
	bytes, err := json.Marshal(result)
	if err != nil {
		return err.Error()
	}
	return string(bytes)
}
