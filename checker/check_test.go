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
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/conn"
	router "github.com/pingcap/tidb-tools/pkg/table-router"

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

func ignoreExcept(itemMap map[string]struct{}) []string {
	items := []string{
		config.DumpPrivilegeChecking,
		config.ReplicationPrivilegeChecking,
		config.VersionChecking,
		config.ServerIDChecking,
		config.BinlogEnableChecking,
		config.BinlogFormatChecking,
		config.BinlogRowImageChecking,
		config.TableSchemaChecking,
		config.ShardTableSchemaChecking,
		config.ShardAutoIncrementIDChecking,
	}
	ignoreCheckingItems := make([]string, 0, len(items)-len(itemMap))
	for _, i := range items {
		if _, ok := itemMap[i]; !ok {
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
			IgnoreCheckingItems: ignoreExcept(map[string]struct{}{config.DumpPrivilegeChecking: {}}),
		},
	}
	mock := s.initMockDB(c)
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for User"}).
		AddRow("GRANT USAGE ON *.* TO 'haha'@'%'"))
	err := CheckSyncConfig(context.Background(), cfgs)
	c.Assert(err, tc.ErrorMatches, "(.|\n)*lack.*RELOAD(.|\n)*")
	c.Assert(err, tc.ErrorMatches, "(.|\n)*lack.*Select(.|\n)*")

	mock = s.initMockDB(c)
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for User"}).
		AddRow("GRANT RELOAD,SELECT ON *.* TO 'haha'@'%'"))
	c.Assert(CheckSyncConfig(context.Background(), cfgs), tc.IsNil)
}

func (s *testCheckerSuite) TestReplicationPrivilegeChecking(c *tc.C) {
	cfgs := []*config.SubTaskConfig{
		{
			IgnoreCheckingItems: ignoreExcept(map[string]struct{}{config.ReplicationPrivilegeChecking: {}}),
		},
	}
	mock := s.initMockDB(c)
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for User"}).
		AddRow("GRANT USAGE ON *.* TO 'haha'@'%'"))
	err := CheckSyncConfig(context.Background(), cfgs)
	c.Assert(err, tc.ErrorMatches, "(.|\n)*lack.*REPLICATION SLAVE(.|\n)*")
	c.Assert(err, tc.ErrorMatches, "(.|\n)*lack.*REPLICATION CLIENT(.|\n)*")

	mock = s.initMockDB(c)
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for User"}).
		AddRow("GRANT REPLICATION SLAVE,REPLICATION CLIENT ON *.* TO 'haha'@'%'"))
	c.Assert(CheckSyncConfig(context.Background(), cfgs), tc.IsNil)
}

func (s *testCheckerSuite) TestVersionChecking(c *tc.C) {
	cfgs := []*config.SubTaskConfig{
		{
			IgnoreCheckingItems: ignoreExcept(map[string]struct{}{config.VersionChecking: {}}),
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

func (s *testCheckerSuite) TestServerIDChecking(c *tc.C) {
	cfgs := []*config.SubTaskConfig{
		{
			IgnoreCheckingItems: ignoreExcept(map[string]struct{}{config.ServerIDChecking: {}}),
		},
	}

	mock := s.initMockDB(c)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'server_id'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("server_id", "0"))
	c.Assert(CheckSyncConfig(context.Background(), cfgs), tc.ErrorMatches, "(.|\n)*please set server_id greater than 0(.|\n)*")

	mock = s.initMockDB(c)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'server_id'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("server_id", "1"))
	c.Assert(CheckSyncConfig(context.Background(), cfgs), tc.IsNil)
}

func (s *testCheckerSuite) TestBinlogEnableChecking(c *tc.C) {
	cfgs := []*config.SubTaskConfig{
		{
			IgnoreCheckingItems: ignoreExcept(map[string]struct{}{config.BinlogEnableChecking: {}}),
		},
	}

	mock := s.initMockDB(c)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'log_bin'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("log_bin", "OFF"))
	c.Assert(CheckSyncConfig(context.Background(), cfgs), tc.ErrorMatches, "(.|\n)*log_bin is OFF, and should be ON(.|\n)*")

	mock = s.initMockDB(c)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'log_bin'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("log_bin", "ON"))
	c.Assert(CheckSyncConfig(context.Background(), cfgs), tc.IsNil)
}

func (s *testCheckerSuite) TestBinlogFormatChecking(c *tc.C) {
	cfgs := []*config.SubTaskConfig{
		{
			IgnoreCheckingItems: ignoreExcept(map[string]struct{}{config.BinlogFormatChecking: {}}),
		},
	}

	mock := s.initMockDB(c)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'binlog_format'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("binlog_format", "STATEMENT"))
	c.Assert(CheckSyncConfig(context.Background(), cfgs), tc.ErrorMatches, "(.|\n)*binlog_format is STATEMENT, and should be ROW(.|\n)*")

	mock = s.initMockDB(c)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'binlog_format'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("binlog_format", "ROW"))
	c.Assert(CheckSyncConfig(context.Background(), cfgs), tc.IsNil)
}

func (s *testCheckerSuite) TestBinlogRowImageChecking(c *tc.C) {
	cfgs := []*config.SubTaskConfig{
		{
			IgnoreCheckingItems: ignoreExcept(map[string]struct{}{config.BinlogRowImageChecking: {}}),
		},
	}

	mock := s.initMockDB(c)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.26-log"))
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'binlog_row_image'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("binlog_row_image", "MINIMAL"))
	c.Assert(CheckSyncConfig(context.Background(), cfgs), tc.ErrorMatches, "(.|\n)*binlog_row_image is MINIMAL, and should be FULL(.|\n)*")

	mock = s.initMockDB(c)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "10.1.29-MariaDB"))
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'binlog_row_image'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("binlog_row_image", "FULL"))
	c.Assert(CheckSyncConfig(context.Background(), cfgs), tc.IsNil)
}

func (s *testCheckerSuite) TestTableSchemaChecking(c *tc.C) {
	cfgs := []*config.SubTaskConfig{
		{
			IgnoreCheckingItems: ignoreExcept(map[string]struct{}{config.TableSchemaChecking: {}}),
		},
	}

	schema := "db_1"
	tb1 := "t_1"
	tb2 := "t_2"
	createTable1 := `CREATE TABLE %s (
  					id int(11) DEFAULT NULL,
  					b int(11) DEFAULT NULL
					) ENGINE=InnoDB DEFAULT CHARSET=latin1`
	createTable2 := `CREATE TABLE %s (
  					id int(11) DEFAULT NULL,
  					b int(11) DEFAULT NULL,
  					UNIQUE KEY id (id)
					) ENGINE=InnoDB DEFAULT CHARSET=latin1`

	mock := s.initMockDB(c)
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(sqlmock.NewRows([]string{"DATABASE"}).AddRow(schema))
	mock.ExpectQuery("SHOW FULL TABLES").WillReturnRows(sqlmock.NewRows([]string{"Tables_in_" + schema, "Table_type"}).AddRow(tb1, "BASE TABLE").AddRow(tb2, "BASE TABLE"))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb1)))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb2)))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	c.Assert(CheckSyncConfig(context.Background(), cfgs), tc.ErrorMatches, "(.|\n)*primary/unique key does not exist(.|\n)*")

	mock = s.initMockDB(c)
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(sqlmock.NewRows([]string{"DATABASE"}).AddRow(schema))
	mock.ExpectQuery("SHOW FULL TABLES").WillReturnRows(sqlmock.NewRows([]string{"Tables_in_" + schema, "Table_type"}).AddRow(tb1, "BASE TABLE").AddRow(tb2, "BASE TABLE"))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable2, tb1)))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable2, tb2)))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	c.Assert(CheckSyncConfig(context.Background(), cfgs), tc.IsNil)
}

func (s *testCheckerSuite) TestShardTableSchemaChecking(c *tc.C) {
	cfgs := []*config.SubTaskConfig{
		{
			RouteRules: []*router.TableRule{
				{
					SchemaPattern: "db_1",
					TargetSchema:  "db",
					TablePattern:  "t_*",
					TargetTable:   "t",
				},
			},
			IgnoreCheckingItems: ignoreExcept(map[string]struct{}{config.ShardTableSchemaChecking: {}}),
		},
	}

	schema := "db_1"
	tb1 := "t_1"
	tb2 := "t_2"
	createTable1 := `CREATE TABLE %s (
				  	id int(11) DEFAULT NULL,
  					b int(11) DEFAULT NULL
					) ENGINE=InnoDB DEFAULT CHARSET=latin1`
	createTable2 := `CREATE TABLE %s (
  					id int(11) DEFAULT NULL,
  					c int(11) DEFAULT NULL
					) ENGINE=InnoDB DEFAULT CHARSET=latin1`

	mock := s.initMockDB(c)
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(sqlmock.NewRows([]string{"DATABASE"}).AddRow(schema))
	mock.ExpectQuery("SHOW FULL TABLES").WillReturnRows(sqlmock.NewRows([]string{"Tables_in_" + schema, "Table_type"}).AddRow(tb1, "BASE TABLE").AddRow(tb2, "BASE TABLE"))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb1)))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable2, tb2)))
	c.Assert(CheckSyncConfig(context.Background(), cfgs), tc.ErrorMatches, "(.|\n)*different column definition(.|\n)*")

	mock = s.initMockDB(c)
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(sqlmock.NewRows([]string{"DATABASE"}).AddRow(schema))
	mock.ExpectQuery("SHOW FULL TABLES").WillReturnRows(sqlmock.NewRows([]string{"Tables_in_" + schema, "Table_type"}).AddRow(tb1, "BASE TABLE").AddRow(tb2, "BASE TABLE"))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb1)))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb2)))
	c.Assert(CheckSyncConfig(context.Background(), cfgs), tc.IsNil)
}

func (s *testCheckerSuite) TestShardAutoIncrementIDChecking(c *tc.C) {
	cfgs := []*config.SubTaskConfig{
		{
			RouteRules: []*router.TableRule{
				{
					SchemaPattern: "db_1",
					TargetSchema:  "db",
					TablePattern:  "t_*",
					TargetTable:   "t",
				},
			},
			IgnoreCheckingItems: ignoreExcept(map[string]struct{}{config.ShardTableSchemaChecking: {}, config.ShardAutoIncrementIDChecking: {}}),
		},
	}

	schema := "db_1"
	tb1 := "t_1"
	tb2 := "t_2"
	createTable1 := `CREATE TABLE %s (
				  	id int(11) NOT NULL AUTO_INCREMENT,
  					b int(11) DEFAULT NULL,
					PRIMARY KEY (id),
					UNIQUE KEY u_b(b)
					) ENGINE=InnoDB DEFAULT CHARSET=latin1`

	createTable2 := `CREATE TABLE %s (
  					id int(11) NOT NULL,
  					b int(11) DEFAULT NULL,
					INDEX (id),
					UNIQUE KEY u_b(b)
					) ENGINE=InnoDB DEFAULT CHARSET=latin1`

	mock := s.initMockDB(c)
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(sqlmock.NewRows([]string{"DATABASE"}).AddRow(schema))
	mock.ExpectQuery("SHOW FULL TABLES").WillReturnRows(sqlmock.NewRows([]string{"Tables_in_" + schema, "Table_type"}).AddRow(tb1, "BASE TABLE").AddRow(tb2, "BASE TABLE"))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb1)))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb2)))
	c.Assert(CheckSyncConfig(context.Background(), cfgs), tc.ErrorMatches, "(.|\n)*instance  table .* of sharding .* have auto-increment key(.|\n)*")

	mock = s.initMockDB(c)
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(sqlmock.NewRows([]string{"DATABASE"}).AddRow(schema))
	mock.ExpectQuery("SHOW FULL TABLES").WillReturnRows(sqlmock.NewRows([]string{"Tables_in_" + schema, "Table_type"}).AddRow(tb1, "BASE TABLE").AddRow(tb2, "BASE TABLE"))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable2, tb1)))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable2, tb2)))
	c.Assert(CheckSyncConfig(context.Background(), cfgs), tc.IsNil)
}

func (s *testCheckerSuite) TestSameTargetTableDetection(c *tc.C) {
	cfgs := []*config.SubTaskConfig{
		{
			RouteRules: []*router.TableRule{
				{
					SchemaPattern: "db_1",
					TargetSchema:  "db",
					TablePattern:  "t_1",
					TargetTable:   "t",
				}, {
					SchemaPattern: "db_1",
					TargetSchema:  "db",
					TablePattern:  "t_2",
					TargetTable:   "T",
				},
			},
			IgnoreCheckingItems: ignoreExcept(map[string]struct{}{config.TableSchemaChecking: {}}),
		},
	}

	schema := "db_1"
	tb1 := "t_1"
	tb2 := "t_2"
	createTable1 := `CREATE TABLE %s (
				  	id int(11) NOT NULL AUTO_INCREMENT,
  					b int(11) DEFAULT NULL,
					PRIMARY KEY (id),
					UNIQUE KEY u_b(b)
					) ENGINE=InnoDB DEFAULT CHARSET=latin1`

	mock := s.initMockDB(c)
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(sqlmock.NewRows([]string{"DATABASE"}).AddRow(schema))
	mock.ExpectQuery("SHOW FULL TABLES").WillReturnRows(sqlmock.NewRows([]string{"Tables_in_" + schema, "Table_type"}).AddRow(tb1, "BASE TABLE").AddRow(tb2, "BASE TABLE"))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb1)))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb2)))
	c.Assert(CheckSyncConfig(context.Background(), cfgs), tc.ErrorMatches, "(.|\n)*same table name in case-insensitive(.|\n)*")
}
