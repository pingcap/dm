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

package schema

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb-tools/pkg/filter"
	tidbConfig "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/conn"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/utils"
)

const (
	// TiDBClusteredIndex is the variable name for clustered index.
	TiDBClusteredIndex = "tidb_enable_clustered_index"
)

var (
	// don't read clustered index variable from downstream because it may changed during syncing
	// we always using OFF tidb_enable_clustered_index unless user set it in config.
	downstreamVars    = []string{"sql_mode", "tidb_skip_utf8_check"}
	defaultGlobalVars = map[string]string{
		TiDBClusteredIndex: "OFF",
	}
)

// Tracker is used to track schema locally.
type Tracker struct {
	store           kv.Storage
	dom             *domain.Domain
	se              session.Session
	downstreamTrack map[string]*ast.CreateTableStmt // downstream tracker tableid -> createTableStmt
}

// ToIndexes is downstream pk/uk info.
// type ToIndexes struct {
// 	tableID string
// 	pks     []string // include multiple primary key
// 	uks     []string // uk/uks
// 	// uksIsNull  []bool   // uk/uks is null?
// }

// NewTracker creates a new tracker. `sessionCfg` will be set as tracker's session variables if specified, or retrieve
// some variable from downstream TiDB using `tidbConn`.
// NOTE **sessionCfg is a reference to caller**.
func NewTracker(ctx context.Context, task string, sessionCfg map[string]string, tidbConn *conn.BaseConn) (*Tracker, error) {
	// NOTE: tidb uses a **global** config so can't isolate tracker's config from each other. If that isolation is needed,
	// we might SetGlobalConfig before every call to tracker, or use some patch like https://github.com/bouk/monkey
	tidbConfig.UpdateGlobal(func(conf *tidbConfig.Config) {
		// bypass wait time of https://github.com/pingcap/tidb/pull/20550
		conf.TiKVClient.AsyncCommit.SafeWindow = 0
		conf.TiKVClient.AsyncCommit.AllowedClockDrift = 0
	})

	if len(sessionCfg) == 0 {
		sessionCfg = make(map[string]string)
	}

	tctx := tcontext.NewContext(ctx, log.With(zap.String("component", "schema-tracker"), zap.String("task", task)))
	// get variables if user doesn't specify
	// all cfg in downstreamVars should be lower case
	for _, k := range downstreamVars {
		if _, ok := sessionCfg[k]; !ok {
			var ignoredColumn interface{}
			rows, err2 := tidbConn.QuerySQL(tctx, fmt.Sprintf("SHOW VARIABLES LIKE '%s'", k))
			if err2 != nil {
				return nil, err2
			}
			if rows.Next() {
				var value string
				if err3 := rows.Scan(&ignoredColumn, &value); err3 != nil {
					return nil, err3
				}
				sessionCfg[k] = value
			}
			// nolint:sqlclosecheck
			if err2 = rows.Close(); err2 != nil {
				return nil, err2
			}
			if err2 = rows.Err(); err2 != nil {
				return nil, err2
			}
		}
	}

	store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.EmbedUnistore))
	if err != nil {
		return nil, err
	}

	// avoid data race and of course no use in DM
	domain.RunAutoAnalyze = false
	session.DisableStats4Test()

	dom, err := session.BootstrapSession(store)
	if err != nil {
		return nil, err
	}

	se, err := session.CreateSession(store)
	if err != nil {
		return nil, err
	}

	globalVarsToSet := make(map[string]string, len(defaultGlobalVars))
	for k, v := range defaultGlobalVars {
		// user's config has highest priority
		if _, ok := sessionCfg[k]; !ok {
			globalVarsToSet[k] = v
		}
	}

	for k, v := range sessionCfg {
		err = se.GetSessionVars().SetSystemVarWithRelaxedValidation(k, v)
		if err != nil {
			// when user set some unsupported variable, we just ignore it
			if terror.ErrorEqual(err, variable.ErrUnknownSystemVar) {
				log.L().Warn("can not set this variable", zap.Error(err))
				continue
			}
			return nil, err
		}
	}
	for k, v := range globalVarsToSet {
		err = se.GetSessionVars().SetSystemVarWithRelaxedValidation(k, v)
		if err != nil {
			return nil, err
		}
	}

	// TiDB will unconditionally create an empty "test" schema.
	// This interferes with MySQL/MariaDB upstream which such schema does not
	// exist by default. So we need to drop it first.
	err = dom.DDL().DropSchema(se, model.NewCIStr("test"))
	if err != nil {
		return nil, err
	}

	return &Tracker{
		store: store,
		dom:   dom,
		se:    se,
	}, nil
}

// Exec runs an SQL (DDL) statement.
func (tr *Tracker) Exec(ctx context.Context, db string, sql string) error {
	tr.se.GetSessionVars().CurrentDB = db
	_, err := tr.se.Execute(ctx, sql)
	return err
}

// GetTableInfo returns the schema associated with the table.
func (tr *Tracker) GetTableInfo(table *filter.Table) (*model.TableInfo, error) {
	dbName := model.NewCIStr(table.Schema)
	tableName := model.NewCIStr(table.Name)
	t, err := tr.dom.InfoSchema().TableByName(dbName, tableName)
	if err != nil {
		return nil, err
	}
	return t.Meta(), nil
}

// GetCreateTable returns the `CREATE TABLE` statement of the table.
func (tr *Tracker) GetCreateTable(ctx context.Context, table *filter.Table) (string, error) {
	// use `SHOW CREATE TABLE` now, another method maybe `executor.ConstructResultOfShowCreateTable`.
	rs, err := tr.se.Execute(ctx, fmt.Sprintf("SHOW CREATE TABLE %s", table.String()))
	if err != nil {
		return "", err
	} else if len(rs) != 1 {
		return "", nil // this should not happen.
	}
	// nolint:errcheck
	defer rs[0].Close()

	req := rs[0].NewChunk()
	err = rs[0].Next(ctx, req)
	if err != nil {
		return "", err
	}
	if req.NumRows() == 0 {
		return "", nil // this should not happen.
	}

	row := req.GetRow(0)
	str := row.GetString(1) // the first column is the table name.
	// returned as single line.
	str = strings.ReplaceAll(str, "\n", "")
	str = strings.ReplaceAll(str, "  ", " ")
	return str, nil
}

// AllSchemas returns all schemas visible to the tracker (excluding system tables).
func (tr *Tracker) AllSchemas() []*model.DBInfo {
	allSchemas := tr.dom.InfoSchema().AllSchemas()
	filteredSchemas := make([]*model.DBInfo, 0, len(allSchemas)-3)
	for _, db := range allSchemas {
		if !filter.IsSystemSchema(db.Name.L) {
			filteredSchemas = append(filteredSchemas, db)
		}
	}
	return filteredSchemas
}

// GetSingleColumnIndices returns indices of input column if input column only has single-column indices
// returns nil if input column has no indices, or has multi-column indices.
func (tr *Tracker) GetSingleColumnIndices(db, tbl, col string) ([]*model.IndexInfo, error) {
	col = strings.ToLower(col)
	t, err := tr.dom.InfoSchema().TableByName(model.NewCIStr(db), model.NewCIStr(tbl))
	if err != nil {
		return nil, err
	}

	var idxInfos []*model.IndexInfo
	for _, idx := range t.Indices() {
		m := idx.Meta()
		for _, col2 := range m.Columns {
			// found an index covers input column
			if col2.Name.L == col {
				if len(m.Columns) == 1 {
					idxInfos = append(idxInfos, m)
				} else {
					// temporary use errors.New, won't propagate further
					return nil, errors.New("found multi-column index")
				}
			}
		}
	}
	return idxInfos, nil
}

// IsTableNotExists checks if err means the database or table does not exist.
func IsTableNotExists(err error) bool {
	return infoschema.ErrTableNotExists.Equal(err) || infoschema.ErrDatabaseNotExists.Equal(err)
}

// Reset drops all tables inserted into this tracker.
func (tr *Tracker) Reset() error {
	allDBs := tr.dom.InfoSchema().AllSchemaNames()
	ddl := tr.dom.DDL()
	for _, db := range allDBs {
		dbName := model.NewCIStr(db)
		if filter.IsSystemSchema(dbName.L) {
			continue
		}
		if err := ddl.DropSchema(tr.se, dbName); err != nil {
			return err
		}
	}
	return nil
}

// Close close a tracker.
func (tr *Tracker) Close() error {
	tr.se.Close()
	tr.dom.Close()
	return tr.store.Close()
}

// DropTable drops a table from this tracker.
func (tr *Tracker) DropTable(table *filter.Table) error {
	tableIdent := ast.Ident{
		Schema: model.NewCIStr(table.Schema),
		Name:   model.NewCIStr(table.Name),
	}
	return tr.dom.DDL().DropTable(tr.se, tableIdent)
}

// DropIndex drops an index from this tracker.
func (tr *Tracker) DropIndex(table *filter.Table, index string) error {
	tableIdent := ast.Ident{
		Schema: model.NewCIStr(table.Schema),
		Name:   model.NewCIStr(table.Name),
	}
	return tr.dom.DDL().DropIndex(tr.se, tableIdent, model.NewCIStr(index), true)
}

// CreateSchemaIfNotExists creates a SCHEMA of the given name if it did not exist.
func (tr *Tracker) CreateSchemaIfNotExists(db string) error {
	dbName := model.NewCIStr(db)
	if tr.dom.InfoSchema().SchemaExists(dbName) {
		return nil
	}
	return tr.dom.DDL().CreateSchema(tr.se, dbName, nil)
}

// cloneTableInfo creates a clone of the TableInfo.
func cloneTableInfo(ti *model.TableInfo) *model.TableInfo {
	ret := ti.Clone()
	ret.Lock = nil
	// FIXME pingcap/parser's Clone() doesn't clone Partition yet
	if ret.Partition != nil {
		pi := *ret.Partition
		pi.Definitions = append([]model.PartitionDefinition(nil), ret.Partition.Definitions...)
		ret.Partition = &pi
	}
	return ret
}

// CreateTableIfNotExists creates a TABLE of the given name if it did not exist.
func (tr *Tracker) CreateTableIfNotExists(table *filter.Table, ti *model.TableInfo) error {
	schemaName := model.NewCIStr(table.Schema)
	tableName := model.NewCIStr(table.Name)
	ti = cloneTableInfo(ti)
	ti.Name = tableName
	return tr.dom.DDL().CreateTableWithInfo(tr.se, schemaName, ti, ddl.OnExistIgnore, false)
}

// GetSystemVar gets a variable from schema tracker.
func (tr *Tracker) GetSystemVar(name string) (string, bool) {
	return tr.se.GetSessionVars().GetSystemVar(name)
}

// GetDownStreamIndexInfo gets downstream PK/UK(not null) Index.
// note. this function will init downstreamTrack's table info.
func (tr *Tracker) GetDownStreamIndexInfo(tableID string, originTi *model.TableInfo, tctx *tcontext.Context, task string, tidbConn *conn.BaseConn) (*model.IndexInfo, error) {
	if tr.downstreamTrack == nil {
		tr.downstreamTrack = make(map[string]*ast.CreateTableStmt)
	}
	createTableStmt := tr.downstreamTrack[tableID]
	if createTableStmt == nil {

		log.L().Info(fmt.Sprintf("DownStream schema tracker init: %s", tableID))

		rows, err := tidbConn.QuerySQL(tctx, fmt.Sprintf("SHOW CREATE TABLE %s", tableID))
		if err != nil {
			return nil, err
		}

		var tableName string
		var createStr string

		for rows.Next() {
			if err3 := rows.Scan(&tableName, &createStr); err3 != nil {
				return nil, err3
			}
			// parse create table stmt.
			parser := parser.New()

			stmtNode, err := parser.ParseOneStmt(createStr, "", "")
			if err != nil {
				return nil, err
			}
			createTableStmt = stmtNode.(*ast.CreateTableStmt)
		}

		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		tr.downstreamTrack[tableID] = createTableStmt
	}

	// get PK/UK from  Constraints.
	var index *model.IndexInfo
	for _, constraint := range createTableStmt.Constraints {
		var keys []*ast.IndexPartSpecification

		switch constraint.Tp {
		case ast.ConstraintPrimaryKey: // pk.
			keys = constraint.Keys
		case ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex: // unique,unique key,unique index.

			if index == nil {
				keys = constraint.Keys
			} else {
				// if index has been found, uk should be jump.
				continue
			}
		default:
			continue
		}

		if keys != nil {
			columns := make([]*model.IndexColumn, 0, len(keys))
			isAllNotNull := true // pk is true, uk should check.
			for _, key := range keys {

				// UK should check not null.
				if constraint.Tp != ast.ConstraintPrimaryKey {

					for _, column := range createTableStmt.Cols {
						if key.Column.Name.String() == column.Name.String() {
							hasNotNull := false
							for _, option := range column.Options {
								if option.Tp == ast.ColumnOptionNotNull {
									hasNotNull = true
									break
								}
							}
							if !hasNotNull {
								isAllNotNull = false
							}
							break
						}
					}
				}

				if !isAllNotNull {
					break
				}

				if orginColumn := model.FindColumnInfo(originTi.Columns, key.Column.Name.O); orginColumn != nil {
					column := &model.IndexColumn{
						Name:   key.Column.Name,
						Offset: orginColumn.Offset,
						Length: key.Length,
					}
					columns = append(columns, column)
				}
			}

			if !isAllNotNull {
				continue
			}

			if len(columns) != 0 {
				if constraint.Tp == ast.ConstraintPrimaryKey {
					index = &model.IndexInfo{
						Table:   createTableStmt.Table.Name,
						Unique:  true,
						Primary: true,
						State:   model.StatePublic,
						Tp:      model.IndexTypeBtree,
						Columns: columns,
					}
					log.L().Debug(fmt.Sprintf("Find DownStream table %s pk %s", tableID, constraint.Name))
					return index, nil // pk > uk.
				}
				// uk should continiue to find pk.
				index = &model.IndexInfo{
					Table:   createTableStmt.Table.Name,
					Unique:  true,
					Primary: false,
					State:   model.StatePublic,
					Tp:      model.IndexTypeBtree,
					Columns: columns,
				}
				log.L().Debug(fmt.Sprintf("Find DownStream table %s uk(not null) %s", tableID, constraint.Name))
			}
		}
	}

	if index == nil {
		log.L().Debug(fmt.Sprintf("DownStream table %s has no pk/uk(not null)!", tableID))
	}

	return index, nil
}

// GetAvailableDownStreanUKIndexInfo gets available downstream UK whose data is not null.
// note. this function will not init downstreamTrack.
func (tr *Tracker) GetAvailableDownStreanUKIndexInfo(tableID string, originTi *model.TableInfo, data []interface{}) *model.IndexInfo {
	if tr.downstreamTrack == nil || tr.downstreamTrack[tableID] == nil {
		return nil
	}
	createTableStmt := tr.downstreamTrack[tableID]
	for _, constraint := range createTableStmt.Constraints {

		if constraint.Tp == ast.ConstraintUniq || constraint.Tp == ast.ConstraintUniqKey || constraint.Tp == ast.ConstraintUniqIndex {
			columns := make([]*model.IndexColumn, 0, len(constraint.Keys))
			for _, key := range constraint.Keys {
				if orginColumn := model.FindColumnInfo(originTi.Columns, key.Column.Name.O); orginColumn != nil {
					// check data is null.
					if columnData := data[orginColumn.Offset]; columnData != nil {
						column := &model.IndexColumn{
							Name:   key.Column.Name,
							Offset: orginColumn.Offset,
							Length: key.Length,
						}
						columns = append(columns, column)
					}
				}
			}
			if len(constraint.Keys) == len(columns) {
				log.L().Debug(fmt.Sprintf("Find DownStream table %s uk(data not null) %s", tableID, constraint.Name))
				return &model.IndexInfo{
					Table:   createTableStmt.Table.Name,
					Unique:  true,
					Primary: false,
					State:   model.StatePublic,
					Tp:      model.IndexTypeBtree,
					Columns: columns,
				}
			}
		}
	}
	log.L().Debug(fmt.Sprintf("DownStream table %s has no pk/uk(even data not null)!", tableID))
	return nil
}

// ReTrackDownStreamIndex just remove schema or table in downstreamTrack.
func (tr *Tracker) ReTrackDownStreamIndex(targetTables []*filter.Table) {
	if tr.downstreamTrack == nil || targetTables == nil {
		return
	}

	for i := 0; i < len(targetTables); i++ {
		tableID := utils.GenTableID(targetTables[i])
		if tr.downstreamTrack[tableID] == nil {
			// handle just have schema
			if targetTables[i].Schema != "" && targetTables[i].Name == "" {
				for k := range tr.downstreamTrack {
					if strings.HasPrefix(k, tableID+".") {
						delete(tr.downstreamTrack, k)
					}
				}
				log.L().Info(fmt.Sprintf("Remove downStream schema tracker %s ", targetTables[i].Schema))
			}
		} else {
			delete(tr.downstreamTrack, tableID)
			log.L().Info(fmt.Sprintf("Remove downStream schema tracker %s ", tableID))
		}
	}
}
