package syncer

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-enterprise-tools/dm/config"
	"github.com/pingcap/tidb/ast"
)

var (
	// OnlineDDLSchemes is scheme name => online ddl handler
	OnlineDDLSchemes = map[string]func(*config.SubTaskConfig) (OnlinePlugin, error){
		config.PT: NewPT,
	}
)

// OnlinePlugin handles online ddl solutions like pt, gh-ost
type OnlinePlugin interface {
	// Applys does:
	// * detect online ddl
	// * record changes
	// * apply online ddl on real table
	// returns sqls, replaced/self schema, repliaced/slef table, error
	Apply(schema, table, statement string, stmt ast.StmtNode) ([]string, string, string, error)
	// InOnlineDDL returns true if an online ddl is unresolved
	InOnlineDDL(schema, table string) bool
	// Finish would delete online ddl from memory and storage
	Finish(schema, table string) error
	// TableType returns ghhost/real table
	TableType(table string) TableType
	// RealName returns real table name that removed ghost suffix and handled by table router
	RealName(schema, table string) (string, string)
	// GhostName returns ghost table name of a table
	GhostName(schema, table string) (string, string)
	// SchemaName returns scheme name (gh-ost/pt)
	SchemeName() string
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

	query := fmt.Sprintf("SELECT `ol_schema`, `ol_table`, `ddls` FROM `%s`.`%s` WHERE `id`='%s'", s.schema, s.table, s.id)
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
func (s *OnlineDDLStorage) Get(schema, table string) *GhostDDLInfo {
	s.RLock()
	defer s.RUnlock()

	mSchema, ok := s.ddls[schema]
	if !ok {
		return nil
	}

	return mSchema[table]
}

// Save saves online ddl information
func (s *OnlineDDLStorage) Save(schema, table, ghostSchema, ghostTable, ddl string) error {
	s.Lock()
	defer s.Unlock()

	mSchema, ok := s.ddls[schema]
	if !ok {
		mSchema = make(map[string]*GhostDDLInfo)
		s.ddls[schema] = mSchema
	}

	info, ok := mSchema[table]
	if !ok {
		info = &GhostDDLInfo{
			Schema: ghostSchema,
			Table:  ghostTable,
		}
		mSchema[table] = info
	} else if info.Schema != ghostSchema || info.Table != ghostTable {
		// this is a risky operation
		// we assume user can  execute only one online ddl changes in same time, so latest online ddl can overwrite older one
		log.Warningf("replace %s.%s online ddl info by %s.%s", info.Schema, info.Table, ghostSchema, ghostTable)
		info = &GhostDDLInfo{
			Schema: ghostSchema,
			Table:  ghostTable,
		}
		mSchema[table] = info
	}

	info.DDLs = append(info.DDLs, ddl)
	ddlsBytes, err := json.Marshal(mSchema[table])
	if err != nil {
		return errors.Trace(err)
	}

	query := fmt.Sprintf("REPLACE INTO `%s`.`%s`(`id`,`ol_schema`, `ol_table`, `ddls`) VALUES ('%s', '%s', '%s', '%s')", s.schema, s.table, s.id, schema, table, string(ddlsBytes))
	err = s.db.executeSQL([]string{query}, [][]interface{}{nil}, maxRetryCount)
	return errors.Trace(err)
}

// Delete deletes online ddl informations
func (s *OnlineDDLStorage) Delete(schema, table string) error {
	s.Lock()
	defer s.Unlock()

	mSchema, ok := s.ddls[schema]
	if !ok {
		return nil
	}

	// delete all checkpoints
	sql := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE `id` = '%s' and `ol_schema` = '%s' and `ol_table` = '%s'", s.schema, s.table, s.id, schema, table)
	err := s.db.executeSQL([]string{sql}, [][]interface{}{nil}, maxRetryCount)
	if err != nil {
		return errors.Trace(err)
	}

	delete(mSchema, table)
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
			ol_schema VARCHAR(128) NOT NULL,
			ol_table VARCHAR(128) NOT NULL,
			ddls text,
			update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			UNIQUE KEY uk_id_schema_table (id, ol_schema, ol_table)
		)`, tableName)
	err := s.db.executeSQL([]string{sql}, [][]interface{}{nil}, maxRetryCount)
	return errors.Trace(err)
}
