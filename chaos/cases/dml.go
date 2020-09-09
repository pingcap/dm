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
)

type DMLType int

const (
	insertDML DMLType = iota + 1
	updateDML
	deleteDML
)

// randDML generates DML (INSERT, UPDATE or DELETE).
// NOTE: 3 DML types have the same weight now.
func randDML(ss *sqlsmith.SQLSmith) (dml string, dmlType DMLType, err error) {
	dmlType = DMLType(rand.Intn(3) + 1)
	switch dmlType {
	case insertDML:
		dml, _, err = ss.InsertStmt(false)
	case updateDML:
		dml, _, err = ss.UpdateStmt()
	case deleteDML:
		dml, _, err = ss.DeleteStmt()
	}
	return
}
