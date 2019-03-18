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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb-tools/pkg/filter"

	"github.com/pingcap/dm/dm/config"
)

var (
	// OnlineDDLSchemes is scheme name => online ddl handler
	OnlineDDLSchemes = map[string]func(*config.SubTaskConfig) (OnlinePlugin, error){
		config.PT:    NewPT,
		config.GHOST: NewGhost,
	}
)

// OnlinePlugin handles online ddl solutions like pt, gh-ost
type OnlinePlugin interface {
	// Applys does:
	// * detect online ddl
	// * record changes
	// * apply online ddl on real table
	// returns sqls, replaced/self schema, repliaced/slef table, error
	Apply(tables []*filter.Table, statement string, stmt ast.StmtNode) ([]string, string, string, error)
	// Finish would delete online ddl from memory and storage
	Finish(schema, table string) error
	// TableType returns ghhost/real table
	TableType(table string) TableType
	// RealName returns real table name that removed ghost suffix and handled by table router
	RealName(schema, table string) (string, string)
	// Clear clears all online information
	Clear() error
	// Close closes online ddl plugin
	Close()
}

// TableType is type of table
type TableType string

const (
	realTable  TableType = "real table"
	ghostTable TableType = "ghost table"
	trashTable TableType = "trash table" // means we should ignore these tables
)

// GhostDDLInfo stores ghost information and ddls
type GhostDDLInfo struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`

	DDLs []string `json:"ddls"`
}

// OnlineDDLStorage stores sharding group online ddls information
type OnlineDDLStorage struct {
	sync.RWMutex

	cfg *config.SubTaskConfig

	db     *Conn
	schema string // schema name, set through task config
	table  string // table name, now it's task name
	id     string // now it is `server-id` used as MySQL slave

	// map ghost schema => [ghost table => ghost ddl info, ...]
	ddls map[string]map[string]*GhostDDLInfo
}

// NewOnlineDDLStorage creates a new online ddl storager
func NewOnlineDDLStorage(cfg *config.SubTaskConfig) *OnlineDDLStorage {
	s := &OnlineDDLStorage{
		cfg:    cfg,
		schema: cfg.MetaSchema,
		table:  fmt.Sprintf("%s_onlineddl", cfg.Name),
		id:     strconv.Itoa(cfg.ServerID),
		ddls:   make(map[string]map[string]*GhostDDLInfo),
	}

	return s
}

// Init initials online handler
func (s *OnlineDDLStorage) Init() error {
	db, err := createDB(s.cfg, s.cfg.To, maxCheckPointTimeout)
	if err != nil {
		return errors.Trace(err)
	}
	s.db = db

	err = s.prepare()
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(s.Load())
}

// Load loads information from storage
func (s *OnlineDDLStorage) Load() error {
	s.Lock()
	defer s.Unlock()

	query := fmt.Sprintf("SELECT `ghost_schema`, `ghost_table`, `ddls` FROM `%s`.`%s` WHERE `id`='%s'", s.schema, s.table, s.id)
	rows, err := s.db.querySQL(query, maxRetryCount)
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	var (
		schema string
		table  string
		ddls   string
	)
	for rows.Next() {
		err := rows.Scan(&schema, &table, &ddls)
		if err != nil {
			return errors.Trace(err)
		}

		mSchema, ok := s.ddls[schema]
		if !ok {
			mSchema = make(map[string]*GhostDDLInfo)
			s.ddls[schema] = mSchema
		}

		mSchema[table] = &GhostDDLInfo{}
		err = json.Unmarshal([]byte(ddls), mSchema[table])
		if err != nil {
			return errors.Trace(err)
		}
	}

	return errors.Trace(rows.Err())
}

// Get returns ddls by given schema/table
func (s *OnlineDDLStorage) Get(ghostSchema, ghostTable string) *GhostDDLInfo {
	s.RLock()
	defer s.RUnlock()

	mSchema, ok := s.ddls[ghostSchema]
	if !ok {
		return nil
	}

	clone := new(GhostDDLInfo)
	*clone = *mSchema[ghostTable]

	return clone
}

// Save saves online ddl information
func (s *OnlineDDLStorage) Save(ghostSchema, ghostTable, realSchema, realTable, ddl string) error {
	s.Lock()
	defer s.Unlock()

	mSchema, ok := s.ddls[ghostSchema]
	if !ok {
		mSchema = make(map[string]*GhostDDLInfo)
		s.ddls[ghostSchema] = mSchema
	}

	info, ok := mSchema[ghostTable]
	if !ok {
		info = &GhostDDLInfo{
			Schema: realSchema,
			Table:  realTable,
		}
		mSchema[ghostTable] = info
	}

	// maybe we meed more checks for it

	info.DDLs = append(info.DDLs, ddl)
	ddlsBytes, err := json.Marshal(mSchema[ghostTable])
	if err != nil {
		return errors.Trace(err)
	}

	query := fmt.Sprintf("REPLACE INTO `%s`.`%s`(`id`,`ghost_schema`, `ghost_table`, `ddls`) VALUES ('%s', '%s', '%s', '%s')", s.schema, s.table, s.id, ghostSchema, ghostTable, escapeSingleQuote(string(ddlsBytes)))
	err = s.db.executeSQL([]string{query}, [][]interface{}{nil}, maxRetryCount)
	return errors.Trace(err)
}

// Delete deletes online ddl informations
func (s *OnlineDDLStorage) Delete(ghostSchema, ghostTable string) error {
	s.Lock()
	defer s.Unlock()

	mSchema, ok := s.ddls[ghostSchema]
	if !ok {
		return nil
	}

	// delete all checkpoints
	sql := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE `id` = '%s' and `ghost_schema` = '%s' and `ghost_table` = '%s'", s.schema, s.table, s.id, ghostSchema, ghostTable)
	err := s.db.executeSQL([]string{sql}, [][]interface{}{nil}, maxRetryCount)
	if err != nil {
		return errors.Trace(err)
	}

	delete(mSchema, ghostTable)
	return nil
}

// Clear clears online ddl information from storage
func (s *OnlineDDLStorage) Clear() error {
	s.Lock()
	defer s.Unlock()

	// delete all checkpoints
	sql := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE `id` = '%s'", s.schema, s.table, s.id)
	err := s.db.executeSQL([]string{sql}, [][]interface{}{nil}, maxRetryCount)
	if err != nil {
		return errors.Trace(err)
	}

	s.ddls = make(map[string]map[string]*GhostDDLInfo)
	return nil
}

// Close closes database connection
func (s *OnlineDDLStorage) Close() {
	s.Lock()
	defer s.Unlock()

	closeDBs(s.db)
}

func (s *OnlineDDLStorage) prepare() error {
	if err := s.createSchema(); err != nil {
		return errors.Trace(err)
	}

	if err := s.createTable(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *OnlineDDLStorage) createSchema() error {
	sql := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS `%s`", s.schema)
	err := s.db.executeSQL([]string{sql}, [][]interface{}{nil}, maxRetryCount)
	return errors.Trace(err)
}

func (s *OnlineDDLStorage) createTable() error {
	tableName := fmt.Sprintf("`%s`.`%s`", s.schema, s.table)
	sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id VARCHAR(32) NOT NULL,
			ghost_schema VARCHAR(128) NOT NULL,
			ghost_table VARCHAR(128) NOT NULL,
			ddls text,
			update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			UNIQUE KEY uk_id_schema_table (id, ghost_schema, ghost_table)
		)`, tableName)
	err := s.db.executeSQL([]string{sql}, [][]interface{}{nil}, maxRetryCount)
	return errors.Trace(err)
}

func escapeSingleQuote(str string) string {
	return strings.Replace(str, "'", "''", -1)
}
