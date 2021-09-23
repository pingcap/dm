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
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/filter"
	tidbConfig "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/types"
	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/conn"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/log"
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
	store     kv.Storage
	dom       *domain.Domain
	se        session.Session
	toIndexes map[string]map[string]*ToIndexes
}

// ToIndexes is downstream pk/uk info.
type ToIndexes struct {
	schemaName string
	tableName  string
	pks        []string // include multiple primary key
	uks        []string // uk/uks
	// uksIsNull  []bool   // uk/uks is null?
}

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

// GetTable returns the schema associated with the table.
func (tr *Tracker) GetTable(db, table string) (*model.TableInfo, error) {
	t, err := tr.dom.InfoSchema().TableByName(model.NewCIStr(db), model.NewCIStr(table))
	if err != nil {
		return nil, err
	}
	return t.Meta(), nil
}

// GetCreateTable returns the `CREATE TABLE` statement of the table.
func (tr *Tracker) GetCreateTable(ctx context.Context, db, table string) (string, error) {
	name := dbutil.TableName(db, table)
	// use `SHOW CREATE TABLE` now, another method maybe `executor.ConstructResultOfShowCreateTable`.
	rs, err := tr.se.Execute(ctx, fmt.Sprintf("SHOW CREATE TABLE %s", name))
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
func (tr *Tracker) DropTable(db, table string) error {
	return tr.dom.DDL().DropTable(tr.se, ast.Ident{Schema: model.NewCIStr(db), Name: model.NewCIStr(table)})
}

// DropIndex drops an index from this tracker.
func (tr *Tracker) DropIndex(db, table, index string) error {
	return tr.dom.DDL().DropIndex(tr.se, ast.Ident{Schema: model.NewCIStr(db), Name: model.NewCIStr(table)}, model.NewCIStr(index), true)
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
func (tr *Tracker) CreateTableIfNotExists(db, table string, ti *model.TableInfo) error {
	dbName := model.NewCIStr(db)
	tableName := model.NewCIStr(table)
	ti = cloneTableInfo(ti)
	ti.Name = tableName
	return tr.dom.DDL().CreateTableWithInfo(tr.se, dbName, ti, ddl.OnExistIgnore, false)
}

// GetSystemVar gets a variable from schema tracker.
func (tr *Tracker) GetSystemVar(name string) (string, bool) {
	return tr.se.GetSessionVars().GetSystemVar(name)
}

// GetToIndexInfo gets downstream PK Index.
// note. this function will init toIndexes.
func (tr *Tracker) GetToIndexInfo(db, table string, originTi *model.TableInfo, tctx *tcontext.Context, task string, tidbConn *conn.BaseConn) (*model.IndexInfo, error) {
	if tr.toIndexes == nil {
		tr.toIndexes = make(map[string]map[string]*ToIndexes)
	}
	if dbindexes := tr.toIndexes[db]; dbindexes == nil {
		dbindexes = make(map[string]*ToIndexes)
		tr.toIndexes[db] = dbindexes
	}
	index := tr.toIndexes[db][table]
	if index == nil {
		log.L().Info(fmt.Sprintf("DownStream schema tracker init: %s.%s", db, table))
		index = &ToIndexes{
			schemaName: db,
			tableName:  table,
			pks:        make([]string, 0),
			uks:        make([]string, 0),
			// uksIsNull:  make([]bool, 0),
		}
		// tctx := tcontext.NewContext(ctx, log.With(zap.String("component", "schema-tracker"), zap.String("task", task)))
		rows, err := tidbConn.QuerySQL(tctx, fmt.Sprintf("SHOW INDEX FROM %s FROM %s", table, db))
		if err != nil {
			return nil, err
		}

		cols, err := rows.Columns()
		if err != nil {
			return nil, err
		}
		// the column of show statement is too many, so make dynamic values for scan
		values := make([][]byte, len(cols))
		scans := make([]interface{}, len(cols))
		for i := range values {
			scans[i] = &values[i]
		}

		for rows.Next() {
			if err3 := rows.Scan(scans...); err3 != nil {
				return nil, err3
			}

			// Key_name -- 2, Column_name -- 4, Null -- 9
			nonUnique := string(values[1]) // 0 is UK
			keyName := string(values[2])   // pk is PRIMARY
			columName := string(values[4])
			// isNull := string(values[9]) // Null is YES

			if strings.EqualFold(keyName, "PRIMARY") {
				// handle multiple pk
				index.pks = append(index.pks, columName)
				log.L().Info(fmt.Sprintf("DownStream schema tracker %s.%s Find PK %s", db, table, columName))
			} else if strings.EqualFold(nonUnique, "0") {
				index.uks = append(index.uks, columName)
				log.L().Info(fmt.Sprintf("DownStream schema tracker %s.%s Find UK %s ", db, table, columName))
			}
		}
		// nolint:sqlclosecheck
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		tr.toIndexes[db][table] = index
	}

	// construct model.IndexInfo, PK > not null UK
	if len(index.pks) != 0 {
		// handle multiple pk
		columns := make([]*model.IndexColumn, 0, len(index.pks))
		for _, pk := range index.pks {
			if orginColumn := model.FindColumnInfo(originTi.Columns, pk); orginColumn != nil {
				column := &model.IndexColumn{
					Name:   model.NewCIStr(pk),
					Offset: orginColumn.Offset,
					Length: types.UnspecifiedLength,
				}
				columns = append(columns, column)
			}
		}
		if len(columns) != 0 {
			return &model.IndexInfo{
				Table:   model.NewCIStr(table),
				Unique:  true,
				Primary: true,
				State:   model.StatePublic,
				Tp:      model.IndexTypeBtree,
				Columns: columns,
			}, nil
		}
	}
	// else if len(index.uks) != 0 {
	// 	for i := 0; i < len(index.uks); i++ {
	// 		if !index.uksIsNull[i] {
	// 			if originColumn := model.FindColumnInfo(originTi.Columns, index.uks[i]); originColumn != nil {
	// 				return &model.IndexInfo{
	// 					Table:   model.NewCIStr(table),
	// 					Unique:  true,
	// 					Primary: false,
	// 					State:   model.StatePublic,
	// 					Tp:      model.IndexTypeBtree,
	// 					Columns: []*model.IndexColumn{{
	// 						Name:   model.NewCIStr(index.uks[i]),
	// 						Offset: originColumn.Offset,
	// 						Length: types.UnspecifiedLength,
	// 					}},
	// 				}, nil
	// 			}
	// 		}
	// 	}
	// }

	return nil, nil
}

// GetAvailableUKToIndexInfo gets available downstream UK whose data is not null
// note. this function will not init toIndexes.
func (tr *Tracker) GetAvailableUKToIndexInfo(db, table string, originTi *model.TableInfo, data []interface{}) *model.IndexInfo {
	if tr.toIndexes == nil || tr.toIndexes[db] == nil || tr.toIndexes[db][table] == nil {
		return nil
	}
	index := tr.toIndexes[db][table]
	for i := 0; i < len(index.uks); i++ {
		if originColumn := model.FindColumnInfo(originTi.Columns, index.uks[i]); originColumn != nil {
			if data[originColumn.Offset] != nil {
				return &model.IndexInfo{
					Table:   model.NewCIStr(table),
					Unique:  true,
					Primary: false,
					State:   model.StatePublic,
					Tp:      model.IndexTypeBtree,
					Columns: []*model.IndexColumn{{
						Name:   model.NewCIStr(index.uks[i]),
						Offset: originColumn.Offset,
						Length: types.UnspecifiedLength,
					}},
				}
			}
		}
	}
	return nil
}

// SetToIndexNotAvailable set toIndex available is false
// func (tr *Tracker) SetToIndexNotAvailable(db, table string) {

// 	if tr.toIndexes == nil || tr.toIndexes[db] == nil || tr.toIndexes[db][table] == nil || !tr.toIndexes[db][table].isAlive {
// 		return
// 	} else {
// 		tr.toIndexes[db][table].isAlive = false
// 	}
// }

// TrackToIndex remove schema or table in toIndex.
func (tr *Tracker) TrackToIndex(targetTables []*filter.Table) {
	if tr.toIndexes == nil || targetTables == nil {
		return
	}

	for i := 0; i < len(targetTables); i++ {
		db := targetTables[i].Schema
		table := targetTables[i].Name
		if tr.toIndexes[db] == nil {
			return
		}
		if table == "" {
			delete(tr.toIndexes, db)
			log.L().Info(fmt.Sprintf("Remove downStream schema tracker %s ", db))
		} else {
			if tr.toIndexes[db][table] == nil {
				return
			}
			delete(tr.toIndexes[db], table)
			log.L().Info(fmt.Sprintf("Remove downStream schema tracker %s.%s ", db, table))
		}
	}

}
