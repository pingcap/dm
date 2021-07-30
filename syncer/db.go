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
	"context"
	"database/sql"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser"
	tmysql "github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/conn"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/pingcap/dm/syncer/dbconn"
)

// in MySQL, we can set `max_binlog_size` to control the max size of a binlog file.
// but this is not absolute:
// > A transaction is written in one chunk to the binary log, so it is never split between several binary logs.
// > Therefore, if you have big transactions, you might see binary log files larger than max_binlog_size.
// ref: https://dev.mysql.com/doc/refman/5.7/en/replication-options-binary-log.html#sysvar_max_binlog_size
// The max value of `max_binlog_size` is 1073741824 (1GB)
// but the actual file size still can be larger, and it may exceed the range of an uint32
// so, if we use go-mysql.Position(with uint32 Pos) to store the binlog size, it may become out of range.
// ps, use go-mysql.Position to store a position of binlog event (position of the next event) is enough.
type binlogSize struct {
	name string
	size int64
}

// UpStreamConn connect to upstream DB
// Normally, we need to get some upstream information through some helper functions
// these helper functions are all easy query functions, so we use a pool of connections here
// maybe change to one connection some day.
type UpStreamConn struct {
	BaseDB *conn.BaseDB
}

func (conn *UpStreamConn) getMasterStatus(ctx context.Context, flavor string) (mysql.Position, gtid.Set, error) {
	pos, gtidSet, err := utils.GetMasterStatus(ctx, conn.BaseDB.DB, flavor)

	failpoint.Inject("GetMasterStatusFailed", func(val failpoint.Value) {
		err = tmysql.NewErr(uint16(val.(int)))
		log.L().Warn("GetMasterStatus failed", zap.String("failpoint", "GetMasterStatusFailed"), zap.Error(err))
	})

	return pos, gtidSet, err
}

func (conn *UpStreamConn) getServerUUID(ctx context.Context, flavor string) (string, error) {
	return utils.GetServerUUID(ctx, conn.BaseDB.DB, flavor)
}

func (conn *UpStreamConn) getServerUnixTS(ctx context.Context) (int64, error) {
	return utils.GetServerUnixTS(ctx, conn.BaseDB.DB)
}

func (conn *UpStreamConn) getParser(ctx context.Context) (*parser.Parser, error) {
	return utils.GetParser(ctx, conn.BaseDB.DB)
}

func (conn *UpStreamConn) killConn(ctx context.Context, connID uint32) error {
	return utils.KillConn(ctx, conn.BaseDB.DB, connID)
}

func (conn *UpStreamConn) fetchAllDoTables(ctx context.Context, bw *filter.Filter) (map[string][]string, error) {
	return utils.FetchAllDoTables(ctx, conn.BaseDB.DB, bw)
}

func (conn *UpStreamConn) countBinaryLogsSize(pos mysql.Position) (int64, error) {
	return countBinaryLogsSize(pos, conn.BaseDB.DB)
}

func createUpStreamConn(dbCfg config.DBConfig) (*UpStreamConn, error) {
	baseDB, err := dbconn.CreateBaseDB(dbCfg)
	if err != nil {
		return nil, terror.WithScope(terror.DBErrorAdapt(err, terror.ErrDBDriverError), terror.ScopeUpstream)
	}
	return &UpStreamConn{BaseDB: baseDB}, nil
}

func closeUpstreamConn(tctx *tcontext.Context, conn *UpStreamConn) {
	if conn != nil {
		dbconn.CloseBaseDB(tctx, conn.BaseDB)
	}
}

func countBinaryLogsSize(fromFile mysql.Position, db *sql.DB) (int64, error) {
	files, err := getBinaryLogs(db)
	if err != nil {
		return 0, err
	}

	var total int64
	for _, file := range files {
		switch {
		case file.name < fromFile.Name:
			continue
		case file.name > fromFile.Name:
			total += file.size
		case file.name == fromFile.Name:
			if file.size > int64(fromFile.Pos) {
				total += file.size - int64(fromFile.Pos)
			}
		}
	}

	return total, nil
}

func getBinaryLogs(db *sql.DB) ([]binlogSize, error) {
	query := "SHOW BINARY LOGS"
	rows, err := db.Query(query)
	if err != nil {
		return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	files := make([]binlogSize, 0, 10)
	for rows.Next() {
		var file string
		var pos int64
		var nullPtr interface{}
		if len(rowColumns) == 2 {
			err = rows.Scan(&file, &pos)
		} else {
			err = rows.Scan(&file, &pos, &nullPtr)
		}
		if err != nil {
			return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
		}
		files = append(files, binlogSize{name: file, size: pos})
	}
	if rows.Err() != nil {
		return nil, terror.DBErrorAdapt(rows.Err(), terror.ErrDBDriverError)
	}
	return files, nil
}
