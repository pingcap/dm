// Copyright 2020 PingCAP, Inc.
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

package v1dbschema

import (
	"fmt"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb/errno"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/conn"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/cputil"
)

// UpdateSchema updates the DB schema from v1.0.x to v2.0.x, including:
// - update checkpoint.
// - update online DDL meta.
func UpdateSchema(tctx *tcontext.Context, db *conn.BaseDB, cfg *config.SubTaskConfig) error {
	// get db connection.
	dbConn, err := db.GetBaseConn(tctx.Context())
	if err != nil {
		return err
	}

	// update checkpoint.
	err = updateSyncerCheckpoint(tctx, dbConn, cfg.Name, dbutil.TableName(cfg.MetaSchema, cputil.SyncerCheckpoint(cfg.Name)), cfg.EnableGTID)
	if err != nil {
		return err
	}

	// update online DDL meta.
	err = updateSyncerOnlineDDLMeta(tctx, dbConn, cfg.Name, dbutil.TableName(cfg.MetaSchema, cputil.SyncerOnlineDDL(cfg.Name)), cfg.SourceID, cfg.ServerID)
	return err
}

// updateSyncerCheckpoint updates the checkpoint table of sync unit, including:
// - update table schema:
//   - add column `binlog_gtid VARCHAR(256)`.
//   - add column `table_info JSON NOT NULL`.
// - update column value:
//   - fill `binlog_gtid` based on `binlog_name` and `binlog_pos` if GTID mode enable.
// NOTE: no need to update the value of `table_info` because DM can get schema automatically from downstream when replicating DML.
func updateSyncerCheckpoint(tctx *tcontext.Context, dbConn *conn.BaseConn, taskName, tableName string, fillGTIDs bool) error {
	if fillGTIDs {
		// TODO(csuzhangxc): fill `binlog_gtid` based on `binlog_name` and `binlog_pos`.
		return errors.New("Not Implemented")
	}

	// try to add columns.
	// NOTE: ignore already exists error to continue the process.
	sqls := []string{
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN binlog_gtid VARCHAR(256) AFTER binlog_pos`, tableName),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN table_info JSON NOT NULL AFTER binlog_gtid`, tableName),
	}
	_, err := dbConn.ExecuteSQLWithIgnoreError(tctx, nil, taskName, ignoreError, sqls)
	return err
}

// updateSyncerOnlineDDLMeta updates the online DDL meta data, including:
// - update the value of `id` from `server-id` to `source-id`.
func updateSyncerOnlineDDLMeta(tctx *tcontext.Context, dbConn *conn.BaseConn, taskName, tableName, sourceID string, serverID uint32) error {
	sqls := []string{
		fmt.Sprintf(`UPDATE %s SET id=? WHERE id=?`, tableName), // for multiple columns.
	}
	args := []interface{}{sourceID, serverID}
	_, err := dbConn.ExecuteSQL(tctx, nil, taskName, sqls, args)
	return err
}

func ignoreError(err error) bool {
	err = errors.Cause(err) // check the original error
	mysqlErr, ok := err.(*mysql.MySQLError)
	if !ok {
		return false
	}

	switch mysqlErr.Number {
	case errno.ErrDupFieldName:
		return true
	default:
		return false
	}
}
