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

package checker

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/conn"

	tc "github.com/pingcap/check"
)

func TestChecker(t *testing.T) {
	tc.TestingT(t)
}

type testCheckerSuite struct{}

var _ = tc.Suite(&testCheckerSuite{})

type mockDBProvider struct {
	db *sql.DB
}

// Apply will build BaseDB with DBConfig
func (d *mockDBProvider) Apply(config config.DBConfig) (*conn.BaseDB, error) {
	return conn.NewBaseDB(d.db, func() {}), nil
}

func (s *testCheckerSuite) initMockDB(c *tc.C) sqlmock.Sqlmock {
	db, mock, err := sqlmock.New()
	c.Assert(err, tc.IsNil)
	conn.DefaultDBProvider = &mockDBProvider{db: db}
	return mock
}

func ignoreExcept(item string) []string {
	items := []string{
		config.DumpPrivilegeChecking,
		config.ReplicationPrivilegeChecking,
		config.VersionChecking,
		config.BinlogEnableChecking,
		config.BinlogFormatChecking,
		config.BinlogRowImageChecking,
		config.TableSchemaChecking,
		config.ShardTableSchemaChecking,
		config.ShardAutoIncrementIDChecking,
	}
	ignoreCheckingItems := make([]string, 0)
	for _, i := range items {
		if i != item {
			ignoreCheckingItems = append(ignoreCheckingItems, i)
		}
	}
	return ignoreCheckingItems
}

func (s *testCheckerSuite) TestIgnoreAllCheckingItems(c *tc.C) {
	c.Assert(CheckSyncConfig(context.Background(), nil), tc.IsNil)

	cfgs := []*config.SubTaskConfig{
		{
			IgnoreCheckingItems: []string{config.AllChecking},
		},
	}
	c.Assert(CheckSyncConfig(context.Background(), cfgs), tc.IsNil)
}

func (s *testCheckerSuite) TestDumpPrivilegeChecking(c *tc.C) {
	cfgs := []*config.SubTaskConfig{
		{
			IgnoreCheckingItems: ignoreExcept(config.DumpPrivilegeChecking),
		},
	}
	mock := s.initMockDB(c)
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for User"}).
		AddRow("GRANT USAGE ON *.* TO 'haha'@'%'"))
	c.Assert(CheckSyncConfig(context.Background(), cfgs), tc.ErrorMatches, "(.|\n)*lack of RELOAD,SELECT privilege(.|\n)*")

	mock = s.initMockDB(c)
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for User"}).
		AddRow("GRANT RELOAD,SELECT ON *.* TO 'haha'@'%'"))
	c.Assert(CheckSyncConfig(context.Background(), cfgs), tc.IsNil)
}

func (s *testCheckerSuite) TestReplicationPrivilegeChecking(c *tc.C) {
	cfgs := []*config.SubTaskConfig{
		{
			IgnoreCheckingItems: ignoreExcept(config.ReplicationPrivilegeChecking),
		},
	}
	mock := s.initMockDB(c)
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for User"}).
		AddRow("GRANT USAGE ON *.* TO 'haha'@'%'"))
	c.Assert(CheckSyncConfig(context.Background(), cfgs), tc.ErrorMatches, "(.|\n)*lack of REPLICATION SLAVE,REPLICATION CLIENT privilege(.|\n)*")

	mock = s.initMockDB(c)
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for User"}).
		AddRow("GRANT REPLICATION SLAVE,REPLICATION CLIENT ON *.* TO 'haha'@'%'"))
	c.Assert(CheckSyncConfig(context.Background(), cfgs), tc.IsNil)
}

func (s *testCheckerSuite) TestVersionChecking(c *tc.C) {
	cfgs := []*config.SubTaskConfig{
		{
			IgnoreCheckingItems: ignoreExcept(config.VersionChecking),
		},
	}

	mock := s.initMockDB(c)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.26-log"))
	c.Assert(CheckSyncConfig(context.Background(), cfgs), tc.IsNil)

	mock = s.initMockDB(c)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "10.1.29-MariaDB"))
	c.Assert(CheckSyncConfig(context.Background(), cfgs), tc.IsNil)

	mock = s.initMockDB(c)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.5.26-log"))
	c.Assert(CheckSyncConfig(context.Background(), cfgs), tc.ErrorMatches, "(.|\n)*version required at least .* but got 5.5.26(.|\n)*")

	mock = s.initMockDB(c)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "10.0.0-MariaDB"))
	c.Assert(CheckSyncConfig(context.Background(), cfgs), tc.ErrorMatches, "(.|\n)*version required at least .* but got 10.0.0(.|\n)*")
}
