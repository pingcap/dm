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

package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/log"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/siddontang/go-mysql/replication"
	"go.uber.org/zap"
)

// DemoPlugin is a demo to show how to use plugin
type DemoPlugin struct {
	db *sql.DB
}

// NewPlugin creates a new DemoPlugin
func NewPlugin() interface{} {
	return &DemoPlugin{}
}

// Init implements Plugin's Init
func (dp *DemoPlugin) Init(cfg *config.SubTaskConfig) error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&interpolateParams=true",
		cfg.To.User, cfg.To.Password, cfg.To.Host, cfg.To.Port)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	dp.db = db

	return nil
}

// HandleDDLJobResult implements Plugin's HandleDDLJobResult
func (dp *DemoPlugin) HandleDDLJobResult(ev *replication.QueryEvent, err error) error {
	if err == nil {
		return nil
	}

	log.Info("demo plugin HandleDDLJobResult", zap.String("query", string(ev.Query)), zap.Error(err))
	if !strings.Contains(err.Error(), "unsupported modify column length") {
		log.Info("don't contain error message \"unsupported modify column length\"")
		return nil
	}

	stmt, err := parser.New().ParseOneStmt(string(ev.Query), "", "")
	if err != nil {
		log.Info("parser failed", zap.Error(err))
		return err
	}

	schema := string(ev.Schema)

	switch st := stmt.(type) {
	case *ast.AlterTableStmt:
		switch st.Specs[0].Tp {
		case ast.AlterTableModifyColumn:
			log.Info("handle AlterTableModifyColumn")

			originColName := st.Specs[0].NewColumns[0].Name.Name.O
			tmpColName := fmt.Sprintf("%s_tmp", st.Specs[0].NewColumns[0].Name)

			var sb strings.Builder
			st.Specs[0].NewColumns[0].Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
			originCol := sb.String()

			st.Specs[0].NewColumns[0].Name.Name = model.NewCIStr(tmpColName)
			var sb2 strings.Builder
			st.Specs[0].NewColumns[0].Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb2))
			tmpCol := sb2.String()
			//log.Info("after translate", zap.String("new col", tmpCol), zap.String("origin col", originColName))

			ctx := context.Background()
			tableInfo, err := dbutil.GetTableInfo(ctx, dp.db, schema, st.Table.Name.O, "")
			if err != nil {
				log.Info("GetTableInfo failed", zap.Error(err))
				return err
			}
			keys, _ := dbutil.SelectUniqueOrderKey(tableInfo)
			keysList := strings.Join(keys, ", ")

			addColSQL := fmt.Sprintf("alter table `%s`.`%s` add column %s after %s", schema, st.Table.Name.O, tmpCol, originColName)
			log.Info("execute", zap.String("sql", addColSQL))
			_, err = dp.db.ExecContext(ctx, addColSQL)
			if err != nil {
				log.Info("GetTableInfo failed", zap.Error(err))
				return err
			}

			insertSQL := fmt.Sprintf("replace into `%s`.`%s`(%s, %s) SELECT %s, %s AS %s FROM `%s`.`%s`;", schema, st.Table.Name.O, keysList, tmpColName, keysList, originColName, tmpColName, schema, st.Table.Name.O)
			log.Info("execute", zap.String("sql", insertSQL))
			_, err = dp.db.ExecContext(ctx, insertSQL)
			if err != nil {
				log.Info("GetTableInfo failed", zap.Error(err))
				return err
			}

			dropColSQL := fmt.Sprintf("alter table `%s`.`%s` drop column %s", schema, st.Table.Name.O, originColName)
			log.Info("execute", zap.String("sql", dropColSQL))
			_, err = dp.db.ExecContext(ctx, dropColSQL)
			if err != nil {
				log.Info("GetTableInfo failed", zap.Error(err))
				return err
			}

			changeColSQL := fmt.Sprintf("ALTER TABLE `%s`.`%s` CHANGE COLUMN %s %s;", schema, st.Table.Name.O, tmpColName, originCol)
			log.Info("execute", zap.String("sql", changeColSQL))
			_, err = dp.db.ExecContext(ctx, changeColSQL)
			if err != nil {
				return err
			}
		default:
			log.Info("ignore")
			return nil
		}
	default:
		log.Info("ignore")
		return nil
	}

	log.Info("ignore")
	return nil
}

// HandleDMLJobResult implements Plugin's HandleDMLJobResult
func (dp *DemoPlugin) HandleDMLJobResult(ev *replication.RowsEvent, err error) error {
	return err
}
