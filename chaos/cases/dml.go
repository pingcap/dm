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

// randDML generates DML (INSERT, UPDATE or DELETE).
// NOTE: 3 DML types have the same weight now.
func randDML(ss *sqlsmith.SQLSmith) (dml string, err error) {
	switch rand.Intn(3) {
	case 0:
		dml, _, err = ss.InsertStmt(false)
	case 1:
		dml, _, err = ss.UpdateStmt()
	case 2:
		dml, _, err = ss.DeleteStmt()
	}
	return
}
