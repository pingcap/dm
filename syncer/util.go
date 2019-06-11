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
	"os"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"

	"github.com/pingcap/dm/dm/config"
)

func toBinlogType(bt string) BinlogType {
	bt = strings.ToLower(bt)
	switch bt {
	case "local":
		return LocalBinlog
	case "remote":
		return RemoteBinlog
	default:
		return RemoteBinlog
	}
}

// tableNameForDML gets table name from INSERT/UPDATE/DELETE statement
func tableNameForDML(dml ast.DMLNode) (schema, table string, err error) {
	switch dml.(type) {
	case *ast.InsertStmt:
		is := dml.(*ast.InsertStmt)
		if is.Table == nil || is.Table.TableRefs == nil || is.Table.TableRefs.Left == nil {
			return "", "", errors.NotValidf("INSERT statement %s", is.Text())
		}
		schema, table, err = tableNameResultSet(is.Table.TableRefs.Left)
		return schema, table, errors.Annotatef(err, "INSERT statement %s", is.Text())
	case *ast.UpdateStmt:
		us := dml.(*ast.UpdateStmt)
		if us.TableRefs == nil || us.TableRefs.TableRefs == nil || us.TableRefs.TableRefs.Left == nil {
			return "", "", errors.NotValidf("UPDATE statement %s", us.Text())
		}
		schema, table, err = tableNameResultSet(us.TableRefs.TableRefs.Left)
		return schema, table, errors.Annotatef(err, "UPDATE statement %s", us.Text())
	case *ast.DeleteStmt:
		ds := dml.(*ast.DeleteStmt)
		if ds.TableRefs == nil || ds.TableRefs.TableRefs == nil || ds.TableRefs.TableRefs.Left == nil {
			return "", "", errors.NotValidf("DELETE statement %s", ds.Text())
		}
		schema, table, err = tableNameResultSet(ds.TableRefs.TableRefs.Left)
		return schema, table, errors.Annotatef(err, "DELETE statement %s", ds.Text())
	}
	return "", "", errors.NotSupportedf("DMLNode %v", dml)
}

func tableNameResultSet(rs ast.ResultSetNode) (schema, table string, err error) {
	ts, ok := rs.(*ast.TableSource)
	if !ok {
		return "", "", errors.NotValidf("ResultSetNode %s", rs.Text())
	}
	tn, ok := ts.Source.(*ast.TableName)
	if !ok {
		return "", "", errors.NotValidf("TableSource %s", ts.Text())
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
