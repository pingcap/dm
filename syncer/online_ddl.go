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
	"strings"
	"sync"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/conn"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/terror"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"go.uber.org/zap"
)

var (
	// OnlineDDLSchemes is scheme name => online ddl handler
	OnlineDDLSchemes = map[string]func(*tcontext.Context, *config.SubTaskConfig) (OnlinePlugin, error){
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
	Apply(tctx *tcontext.Context, tables []*filter.Table, statement string, stmt ast.StmtNode) ([]string, string, string, error)
	// Finish would delete online ddl from memory and storage
	Finish(tctx *tcontext.Context, schema, table string) error
	// TableType returns ghhost/real table
	TableType(table string) TableType
	// RealName returns real table name that removed ghost suffix and handled by table router
	RealName(schema, table string) (string, string)
	// ResetConn reset db connection
	ResetConn(tctx *tcontext.Context) error
	// Clear clears all online information
	Clear(tctx *tcontext.Context) error
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

	db     *conn.BaseDB
	dbConn *DBConn
	schema string // schema name, set through task config
	table  string // table name, now it's task name
	id     string // the source ID of the upstream MySQL/MariaDB replica.

	// map ghost schema => [ghost table => ghost ddl info, ...]
	ddls map[string]map[string]*GhostDDLInfo

	logCtx *tcontext.Context
}

// NewOnlineDDLStorage creates a new online ddl storager
func NewOnlineDDLStorage(logCtx *tcontext.Context, cfg *config.SubTaskConfig) *OnlineDDLStorage {
	s := &OnlineDDLStorage{
		cfg:    cfg,
		schema: cfg.MetaSchema,
		table:  fmt.Sprintf("%s_onlineddl", cfg.Name),
		id:     cfg.SourceID,
		ddls:   make(map[string]map[string]*GhostDDLInfo),
		logCtx: logCtx,
	}

	return s
}

// Init initials online handler
func (s *OnlineDDLStorage) Init(tctx *tcontext.Context) error {
	onlineDB := s.cfg.To
	onlineDB.RawDBCfg = config.DefaultRawDBConfig().SetReadTimeout(maxCheckPointTimeout)
	db, dbConns, err := createConns(tctx, s.cfg, onlineDB, 1)
	if err != nil {
		return terror.WithScope(err, terror.ScopeDownstream)
	}
	s.db = db
	s.dbConn = dbConns[0]

	err = s.prepare(tctx)
	if err != nil {
		return err
	}

	return s.Load(tctx)
}

// Load loads information from storage
func (s *OnlineDDLStorage) Load(tctx *tcontext.Context) error {
	s.Lock()
	defer s.Unlock()

	query := fmt.Sprintf("SELECT `ghost_schema`, `ghost_table`, `ddls` FROM `%s`.`%s` WHERE `id`='%s'", s.schema, s.table, s.id)
	rows, err := s.dbConn.querySQL(tctx, query)
	if err != nil {
		return terror.WithScope(err, terror.ScopeDownstream)
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
			return terror.WithScope(terror.DBErrorAdapt(err, terror.ErrDBDriverError), terror.ScopeDownstream)
		}

		mSchema, ok := s.ddls[schema]
		if !ok {
			mSchema = make(map[string]*GhostDDLInfo)
			s.ddls[schema] = mSchema
		}

		mSchema[table] = &GhostDDLInfo{}
		err = json.Unmarshal([]byte(ddls), mSchema[table])
		if err != nil {
			return terror.ErrSyncerUnitOnlineDDLInvalidMeta.Delegate(err)
		}
	}

	return terror.WithScope(terror.DBErrorAdapt(rows.Err(), terror.ErrDBDriverError), terror.ScopeDownstream)
}

// Get returns ddls by given schema/table
func (s *OnlineDDLStorage) Get(ghostSchema, ghostTable string) *GhostDDLInfo {
	s.RLock()
	defer s.RUnlock()

	mSchema, ok := s.ddls[ghostSchema]
	if !ok {
		return nil
	}

	if mSchema == nil || mSchema[ghostTable] == nil {
		return nil
	}

	clone := new(GhostDDLInfo)
	*clone = *mSchema[ghostTable]

	return clone
}

// Save saves online ddl information
func (s *OnlineDDLStorage) Save(tctx *tcontext.Context, ghostSchema, ghostTable, realSchema, realTable, ddl string) error {
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

	if len(info.DDLs) != 0 && info.DDLs[len(info.DDLs)-1] == ddl {
		tctx.L().Warn("online ddl may be saved before, just ignore it", zap.String("ddl", ddl))
		return nil
	}
	info.DDLs = append(info.DDLs, ddl)
	ddlsBytes, err := json.Marshal(mSchema[ghostTable])
	if err != nil {
		return terror.ErrSyncerUnitOnlineDDLInvalidMeta.Delegate(err)
	}

	query := fmt.Sprintf("REPLACE INTO `%s`.`%s`(`id`,`ghost_schema`, `ghost_table`, `ddls`) VALUES ('%s', '%s', '%s', '%s')", s.schema, s.table, s.id, ghostSchema, ghostTable, escapeSingleQuote(string(ddlsBytes)))
	_, err = s.dbConn.executeSQL(tctx, []string{query})
	return terror.WithScope(err, terror.ScopeDownstream)
}

// Delete deletes online ddl informations
func (s *OnlineDDLStorage) Delete(tctx *tcontext.Context, ghostSchema, ghostTable string) error {
	s.Lock()
	defer s.Unlock()

	mSchema, ok := s.ddls[ghostSchema]
	if !ok {
		return nil
	}

	// delete all checkpoints
	sql := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE `id` = '%s' and `ghost_schema` = '%s' and `ghost_table` = '%s'", s.schema, s.table, s.id, ghostSchema, ghostTable)
	_, err := s.dbConn.executeSQL(tctx, []string{sql})
	if err != nil {
		return terror.WithScope(err, terror.ScopeDownstream)
	}

	delete(mSchema, ghostTable)
	return nil
}

// Clear clears online ddl information from storage
func (s *OnlineDDLStorage) Clear(tctx *tcontext.Context) error {
	s.Lock()
	defer s.Unlock()

	// delete all checkpoints
	sql := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE `id` = '%s'", s.schema, s.table, s.id)
	_, err := s.dbConn.executeSQL(tctx, []string{sql})
	if err != nil {
		return terror.WithScope(err, terror.ScopeDownstream)
	}

	s.ddls = make(map[string]map[string]*GhostDDLInfo)
	return nil
}

// ResetConn implements OnlinePlugin.ResetConn
func (s *OnlineDDLStorage) ResetConn(tctx *tcontext.Context) error {
	return s.dbConn.resetConn(tctx)
}

// Close closes database connection
func (s *OnlineDDLStorage) Close() {
	s.Lock()
	defer s.Unlock()

	closeBaseDB(s.logCtx, s.db)
}

func (s *OnlineDDLStorage) prepare(tctx *tcontext.Context) error {
	if err := s.createSchema(tctx); err != nil {
		return err
	}

	if err := s.createTable(tctx); err != nil {
		return err
	}
	return nil
}

func (s *OnlineDDLStorage) createSchema(tctx *tcontext.Context) error {
	sql := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS `%s`", s.schema)
	_, err := s.dbConn.executeSQL(tctx, []string{sql})
	return terror.WithScope(err, terror.ScopeDownstream)
}

func (s *OnlineDDLStorage) createTable(tctx *tcontext.Context) error {
	tableName := fmt.Sprintf("`%s`.`%s`", s.schema, s.table)
	sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id VARCHAR(32) NOT NULL,
			ghost_schema VARCHAR(128) NOT NULL,
			ghost_table VARCHAR(128) NOT NULL,
			ddls text,
			update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			UNIQUE KEY uk_id_schema_table (id, ghost_schema, ghost_table)
		)`, tableName)
	_, err := s.dbConn.executeSQL(tctx, []string{sql})
	return terror.WithScope(err, terror.ScopeDownstream)
}

func escapeSingleQuote(str string) string {
	return strings.Replace(str, "'", "''", -1)
}
