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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
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
func NewTracker() (*Tracker, error) {
	store, err := mockstore.NewMockTikvStore()
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
		switch dbName.L {
		case "mysql", "performance_schema", "information_schema":
			continue
		}
		if err := ddl.DropSchema(tr.se, dbName); err != nil {
			return err
		}
	}
	return nil
}

// DropTable drops a table from this tracker.
func (tr *Tracker) DropTable(db, table string) error {
	return tr.dom.DDL().DropTable(tr.se, ast.Ident{Schema: model.NewCIStr(db), Name: model.NewCIStr(table)})
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
