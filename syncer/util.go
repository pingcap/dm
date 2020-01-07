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
	"fmt"
	"os"
	"strconv"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/parser/ast"
)

func toBinlogType(enableRelay bool) BinlogType {
	if enableRelay {
		return LocalBinlog
	}

	return RemoteBinlog
}

func binlogTypeToString(binlogType BinlogType) string {
	switch binlogType {
	case RemoteBinlog:
		return "remote"
	case LocalBinlog:
		return "local"
	default:
		return "unknown"
	}
}

// tableNameForDML gets table name from INSERT/UPDATE/DELETE statement
func tableNameForDML(dml ast.DMLNode) (schema, table string, err error) {
	switch stmt := dml.(type) {
	case *ast.InsertStmt:
		if stmt.Table == nil || stmt.Table.TableRefs == nil || stmt.Table.TableRefs.Left == nil {
			return "", "", terror.ErrSyncUnitInvalidTableName.Generate(fmt.Sprintf("INSERT statement %s not valid", stmt.Text()))
		}
		schema, table, err = tableNameResultSet(stmt.Table.TableRefs.Left)
		return schema, table, terror.Annotatef(err, "INSERT statement %s", stmt.Text())
	case *ast.UpdateStmt:
		if stmt.TableRefs == nil || stmt.TableRefs.TableRefs == nil || stmt.TableRefs.TableRefs.Left == nil {
			return "", "", terror.ErrSyncUnitInvalidTableName.Generate(fmt.Sprintf("UPDATE statement %s not valid", stmt.Text()))
		}
		schema, table, err = tableNameResultSet(stmt.TableRefs.TableRefs.Left)
		return schema, table, terror.Annotatef(err, "UPDATE statement %s", stmt.Text())
	case *ast.DeleteStmt:
		if stmt.TableRefs == nil || stmt.TableRefs.TableRefs == nil || stmt.TableRefs.TableRefs.Left == nil {
			return "", "", terror.ErrSyncUnitInvalidTableName.Generate(fmt.Sprintf("DELETE statement %s not valid", stmt.Text()))
		}
		schema, table, err = tableNameResultSet(stmt.TableRefs.TableRefs.Left)
		return schema, table, terror.Annotatef(err, "DELETE statement %s", stmt.Text())
	}
	return "", "", terror.ErrSyncUnitNotSupportedDML.Generate(dml)
}

func tableNameResultSet(rs ast.ResultSetNode) (schema, table string, err error) {
	ts, ok := rs.(*ast.TableSource)
	if !ok {
		return "", "", terror.ErrSyncUnitTableNameQuery.Generate(fmt.Sprintf("ResultSetNode %s", rs.Text()))
	}
	tn, ok := ts.Source.(*ast.TableName)
	if !ok {
		return "", "", terror.ErrSyncUnitTableNameQuery.Generate(fmt.Sprintf("TableSource %s", ts.Text()))
	}
	return tn.Schema.O, tn.Name.O, nil
}

func getDBConfigFromEnv() config.DBConfig {
	host := os.Getenv("MYSQL_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port, _ := strconv.Atoi(os.Getenv("MYSQL_PORT"))
	if port == 0 {
		port = 3306
	}
	user := os.Getenv("MYSQL_USER")
	if user == "" {
		user = "root"
	}
	pswd := os.Getenv("MYSQL_PSWD")
	return config.DBConfig{
		Host:     host,
		User:     user,
		Password: pswd,
		Port:     port,
	}
}
