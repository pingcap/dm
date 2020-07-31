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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
)

const (
	waitDDLRetryCount = 10
	schemaLeaseTime   = 10 * time.Millisecond
)

// Tracker is used to track schema locally.
type Tracker struct {
	store kv.Storage
	dom   *domain.Domain
	se    session.Session
}

// NewTracker creates a new tracker.
func NewTracker(sessionCfg map[string]string) (*Tracker, error) {
	store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.MockTiKV))
	if err != nil {
		return nil, err
	}

	// shorten the schema lease, since the time needed to confirm DDL sync is
	// proportional to this duration (default = 1 second)
	session.SetSchemaLease(schemaLeaseTime)
	dom, err := session.BootstrapSession(store)
	if err != nil {
		return nil, err
	}

	se, err := session.CreateSession(store)
	if err != nil {
		return nil, err
	}

	for k, v := range sessionCfg {
		err = se.GetSessionVars().SetSystemVar(k, v)
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

// Close close a tracker
func (tr *Tracker) Close() error {
	tr.se.Close()
	tr.dom.Close()
	if err := tr.store.Close(); err != nil {
		return err
	}
	return nil
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
	infoSchema := tr.dom.InfoSchema()
	dbName := model.NewCIStr(db)
	tableName := model.NewCIStr(table)
	if infoSchema.TableExists(dbName, tableName) {
		return nil
	}

	dbInfo, exists := infoSchema.SchemaByName(dbName)
	if !exists || dbInfo == nil {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbName)
	}

	// we need to go through the low-level DDL Job API since we don't have a way
	// to recover a CreateTableStmt from a TableInfo yet.

	// First enqueue the DDL job.
	var jobID int64
	err := kv.RunInNewTxn(tr.store, true /*retryable*/, func(txn kv.Transaction) error {
		// reallocate IDs
		idsCount := 2
		if ti.Partition != nil {
			idsCount += len(ti.Partition.Definitions)
		}
		m := meta.NewMeta(txn)
		ids, err := m.GenGlobalIDs(idsCount)
		if err != nil {
			return err
		}

		jobID = ids[0]
		tableInfo := cloneTableInfo(ti)
		tableInfo.ID = ids[1]
		tableInfo.Name = tableName
		if tableInfo.Partition != nil {
			for i := range tableInfo.Partition.Definitions {
				tableInfo.Partition.Definitions[i].ID = ids[i+2]
			}
		}

		return m.EnQueueDDLJob(&model.Job{
			ID:         jobID,
			Type:       model.ActionCreateTable,
			SchemaID:   dbInfo.ID,
			TableID:    tableInfo.ID,
			SchemaName: dbName.O,
			Version:    1,
			StartTS:    txn.StartTS(),
			BinlogInfo: &model.HistoryInfo{},
			Args:       []interface{}{tableInfo},
		})
	})
	if err != nil {
		return err
	}

	// Then wait until the DDL job is synchronized (should take 2 * lease)
	lease := tr.dom.DDL().GetLease() * 2
	for i := 0; i < waitDDLRetryCount; i++ {
		var job *model.Job
		err = kv.RunInNewTxn(tr.store, false /*retryable*/, func(txn kv.Transaction) error {
			m := meta.NewMeta(txn)
			var e error
			job, e = m.GetHistoryDDLJob(jobID)
			return e
		})
		if err == nil && job != nil {
			if job.IsSynced() {
				return nil
			}
			if job.Error != nil {
				return job.Error
			}
		}
		time.Sleep(lease)
	}
	if err == nil {
		// reaching here is basically a bug.
		return errors.Errorf("Cannot create table %s.%s, the DDL job never returned", db, table)
	}
	return err
}
