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
	"math/rand"

	"github.com/chaos-mesh/go-sqlsmith"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
)

type dmlType int

const (
	insertDML dmlType = iota + 1
	updateDML
	deleteDML
)

// randDML generates DML (INSERT, UPDATE or DELETE).
// NOTE: 3 DML types have the same weight now.
func randDML(ss *sqlsmith.SQLSmith) (dml string, t dmlType, err error) {
	t = dmlType(rand.Intn(3) + 1)
	switch t {
	case insertDML:
		dml, _, err = ss.InsertStmt(false)
	case updateDML:
		dml, _, err = ss.UpdateStmt()
	case deleteDML:
		dml, _, err = ss.DeleteStmt()
	}
	return
}

// randDDL generates `ALTER TABLE` DDL.
func randDDL(ss *sqlsmith.SQLSmith) (string, error) {
	for {
		sql, err := ss.AlterTableStmt(&sqlsmith.DDLOptions{
			OnlineDDL: false,
			Tables:    make([]string, 0),
		})

		if err != nil {
			return sql, err
		} else if !isValidSQL(sql) {
			continue
		}

		return sql, nil
	}
}

func isValidSQL(sql string) bool {
	_, err := parser.New().ParseOneStmt(sql, "", "")
	return err == nil
}

func isNotNullNonDefaultAddCol(sql string) (bool, error) {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return false, err
	}
	v, ok := stmt.(*ast.AlterTableStmt)
	if !ok {
		return false, nil
	}

	if len(v.Specs) == 0 || v.Specs[0].Tp != ast.AlterTableAddColumns || len(v.Specs[0].NewColumns) == 0 {
		return false, nil
	}

	spec := v.Specs[0]
	notNull := false
	for _, newCol := range spec.NewColumns {
		for _, opt := range newCol.Options {
			if opt.Tp == ast.ColumnOptionDefaultValue {
				return false, nil
			}
			if opt.Tp == ast.ColumnOptionNotNull {
				notNull = true
			}
		}
	}

	return notNull, nil
}
