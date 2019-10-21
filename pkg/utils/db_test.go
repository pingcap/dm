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

package utils

import (
	"context"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	"github.com/siddontang/go-mysql/mysql"
)

func (t *testUtilsSuite) TestGetSlaveServerID(c *C) {
	testCases := []struct {
		serverIDs []int64
	}{
		{
			[]int64{1, 2, 3},
		}, {
			[]int64{},
		},
	}

	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	flavors := []string{mysql.MariaDBFlavor, mysql.MySQLFlavor}

	for _, testCase := range testCases {
		for _, flavor := range flavors {
			t.createMockResult(mock, testCase.serverIDs, flavor)
			slaveHosts, err := GetSlaveServerID(context.Background(), db)
			c.Assert(err, IsNil)

			for _, serverID := range testCase.serverIDs {
				_, ok := slaveHosts[serverID]
				c.Assert(ok, IsTrue)
			}
		}
	}
}

func (t *testUtilsSuite) createMockResult(mock sqlmock.Sqlmock, serverIDs []int64, flavor string) {
	expectQuery := mock.ExpectQuery("SHOW SLAVE HOSTS")

	host := "test"
	port := 3306
	masterID := 1
	slaveUUID := "test"

	if flavor == mysql.MariaDBFlavor {
		rows := sqlmock.NewRows([]string{"Server_id", "Host", "Port", "Master_id"})
		for _, serverID := range serverIDs {
			rows.AddRow(serverID, host, port, masterID)
		}
		expectQuery.WillReturnRows(rows)

	} else {
		rows := sqlmock.NewRows([]string{"Server_id", "Host", "Port", "Master_id", "Slave_UUID"})
		for _, serverID := range serverIDs {
			rows.AddRow(serverID, host, port, masterID, slaveUUID)
		}
		expectQuery.WillReturnRows(rows)
	}

	return
}
