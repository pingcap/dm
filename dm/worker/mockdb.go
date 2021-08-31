// Copyright 2021 PingCAP, Inc.
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

package worker

import (
	"database/sql"

	"github.com/pingcap/check"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/conn"
)

type mockDBProvider struct {
	db *sql.DB
}

// Apply will build BaseDB with DBConfig.
func (d *mockDBProvider) Apply(config config.DBConfig) (*conn.BaseDB, error) {
	return conn.NewBaseDB(d.db, func() {}), nil
}

func initMockDB(c *check.C) sqlmock.Sqlmock {
	db, mock, err := sqlmock.New()
	c.Assert(err, check.IsNil)
	conn.DefaultDBProvider = &mockDBProvider{db: db}
	return mock
}

func mockShowMasterStatus(mockDB sqlmock.Sqlmock) {
	rows := mockDB.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).AddRow(
		"mysql-bin.000009", 11232, nil, nil, "074be7f4-f0f1-11ea-95bd-0242ac120002:1-699",
	)
	mockDB.ExpectQuery(`SHOW MASTER STATUS`).WillReturnRows(rows)
}
