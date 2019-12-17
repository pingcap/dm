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

func (t *testUtilsSuite) TestGetAllServerID(c *C) {
	testCases := []struct {
		masterID  uint32
		serverIDs []uint32
	}{
		{
			1,
			[]uint32{2, 3, 4},
		}, {
			2,
			[]uint32{},
		},
	}

	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	flavors := []string{mysql.MariaDBFlavor, mysql.MySQLFlavor}

	for _, testCase := range testCases {
		for _, flavor := range flavors {
			t.createMockResult(mock, testCase.masterID, testCase.serverIDs, flavor)
			serverIDs, err2 := GetAllServerID(context.Background(), db)
			c.Assert(err2, IsNil)

			for _, serverID := range testCase.serverIDs {
				_, ok := serverIDs[serverID]
				c.Assert(ok, IsTrue)
			}

			_, ok := serverIDs[testCase.masterID]
			c.Assert(ok, IsTrue)
		}
	}

	err = mock.ExpectationsWereMet()
	c.Assert(err, IsNil)
}

func (t *testUtilsSuite) createMockResult(mock sqlmock.Sqlmock, masterID uint32, serverIDs []uint32, flavor string) {
	expectQuery := mock.ExpectQuery("SHOW SLAVE HOSTS")

	host := "test"
	port := 3306
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

	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'server_id'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("server_id", masterID))

	return
}
