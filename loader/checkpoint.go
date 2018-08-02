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

package loader

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-enterprise-tools/dm/config"
)

// CheckPoint represents checkpoint status
type CheckPoint interface {
	GetRestoringFileInfo(db, table string) map[string][]int64
	GetAllRestoringFileInfo() map[string][]int64
	IsTableFinished(db, table string) bool
	CalcProgress(allFiles map[string]Tables2DataFiles) error
	Init(filename string, endpos int64) error
	GenSQL(filename string, offset int64) string
	MarkAsDone(filename string) error
}

// RemoteCheckPoint implements CheckPoint by saving status in remote database system, mostly in TiDB.
type RemoteCheckPoint struct {
	db             *sql.DB
	id             string
	schema         string
	table          string
	restoringFiles map[string]map[string]FilePosSet
	finishedTables map[string]struct{}
}

func newRemoteCheckPoint(cfg *config.SubTaskConfig) (CheckPoint, error) {
	db, err := createConn(cfg.To)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if cfg.CheckPointSchema == "" {
		cfg.CheckPointSchema = "tidb_loader"
	}

	dir, err := filepath.Abs(cfg.Dir)
	if err != nil {
		return nil, errors.Annotatef(err, "get abs of dir:", cfg.Dir)
	}

	cp := &RemoteCheckPoint{
		db:             db.db,
		restoringFiles: make(map[string]map[string]FilePosSet),
		finishedTables: make(map[string]struct{}),
		schema:         cfg.CheckPointSchema,
		table:          "checkpoint",
		id:             shortSha1(dir),
	}

	begin := time.Now()
	if err := cp.load(); err != nil {
		if isErrTableNotExists(err) {
			err = cp.prepare()
			if err == nil {
				return cp, nil
			}
		}
		return nil, errors.Annotatef(err, "recover from checkpoint failed")
	}
	log.Infof("[loader] load checkpoint takes %f seconds", time.Since(begin).Seconds())

	return cp, nil
}

func (cp *RemoteCheckPoint) prepare() error {
	// create schema
	if err := createSchema(cp.db, cp.schema); err != nil {
		if !isErrDBExists(err) {
			return errors.Trace(err)
		}
	}
	// create tables
	if err := createCheckPointTable(cp.db, cp.schema, cp.table); err != nil {
		if !isErrTableExists(err) {
			return errors.Trace(err)
		}
	}
	return nil
}

func (cp *RemoteCheckPoint) load() error {
	if err := cp.loadCheckPoint(); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (cp *RemoteCheckPoint) loadCheckPoint() error {
	query := fmt.Sprintf("SELECT `filename`,`cp_schema`,`cp_table`,`offset`,`end_pos` from `%s`.`%s` where `id`='%s'", cp.schema, cp.table, cp.id)
	rows, err := querySQL(cp.db, query)
	if err != nil {
		return errors.Trace(err)
	}

	var (
		filename string
		schema   string
		table    string
		offset   int64
		endPos   int64
	)

	for rows.Next() {
		err := rows.Scan(&filename, &schema, &table, &offset, &endPos)
		if err != nil {
			return errors.Trace(err)
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

	return nil
}

// GetRestoringFileInfo get restoring data files for table
func (cp *RemoteCheckPoint) GetRestoringFileInfo(db, table string) map[string][]int64 {
	if tables, ok := cp.restoringFiles[db]; ok {
		if restoringFiles, ok := tables[table]; ok {
			return restoringFiles
		}
	}
	return make(map[string][]int64)
}

// GetAllRestoringFileInfo return all restoring files position.
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

// IsTableFinished query if table finished.
func (cp *RemoteCheckPoint) IsTableFinished(db, table string) bool {
	key := strings.Join([]string{db, table}, ".")
	if _, ok := cp.finishedTables[key]; ok {
		return true
	}
	return false
}

// CalcProgress calculate which table has finished and which table partial restored.
func (cp *RemoteCheckPoint) CalcProgress(allFiles map[string]Tables2DataFiles) error {
	for db, tables := range cp.restoringFiles {
		dbTables, ok := allFiles[db]
		if !ok {
			return errors.Errorf("db (%s) not exist in data files, but in checkpoint.", db)
		}

		for table, restoringFiles := range tables {
			files, ok := dbTables[table]
			if !ok {
				return errors.Errorf("table (%s) not exist in db (%s) in data files, but in checkpoint", table, db)
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
				return errors.Errorf("restoring count greater than total count for table[%v]", table)
			}
		}
	}

	log.Infof("calc checkpoint finished. finished tables (%v)", cp.finishedTables)
	return nil
}

func (cp *RemoteCheckPoint) allFilesFinished(files map[string][]int64) bool {
	for file, pos := range files {
		if len(pos) != 2 {
			log.Errorf("unexpected position data: %s %v", file, pos)
			return false
		}
		if pos[0] != pos[1] {
			return false
		}
	}
	return true
}

// Init initialize checkpoint data in tidb
func (cp *RemoteCheckPoint) Init(filename string, endPos int64) error {
	return initCheckpoint(cp.db, cp.id, cp.schema, cp.table, filename, 0, endPos)
}

// GenSQL generates sql to be executed in tidb
func (cp *RemoteCheckPoint) GenSQL(filename string, offset int64) string {
	sql := fmt.Sprintf("UPDATE `%s`.`%s` SET `offset`=%d WHERE `id` ='%s' AND `filename`='%s';",
		cp.schema, cp.table, offset, cp.id, filename)
	return sql
}

// MarkAsDone will delete the checkpoint record.
func (cp *RemoteCheckPoint) MarkAsDone(filename string) error {
	done, err := cp.checkFileDone(filename)
	if err != nil {
		if errors.Cause(err) == sql.ErrNoRows {
			log.Warnf("[loader] not found checkpoint for %s", filename)
			return nil
		}
		return errors.Trace(err)
	}

	if !done {
		log.Warnf("[loader] datafile %s has finished but the checkpoint tells it's not finished", filename)
		return nil
	}

	sql := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE `id`='%s' AND `filename`='%s';", cp.schema, cp.table, cp.id, filename)
	_, err = cp.db.Exec(sql)
	return errors.Trace(err)
}

// CheckFileDone checks whether the datafile has restored successful or not.
func (cp *RemoteCheckPoint) checkFileDone(filename string) (bool, error) {
	sql := fmt.Sprintf("SELECT `offset`, `end_pos` FROM `%s`.`%s` WHERE `id`='%s' AND `filename`='%s'", cp.schema, cp.table, cp.id, filename)

	var offset, endPos int64
	err := cp.db.QueryRow(sql).Scan(&offset, &endPos)
	if err != nil {
		return false, errors.Annotatef(err, "sql %s", sql)
	}
	return offset == endPos, nil
}

func (cp *RemoteCheckPoint) String() string {
	if err := cp.load(); err != nil {
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
