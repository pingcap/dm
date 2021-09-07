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

package v1dbschema

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb-tools/pkg/dbutil"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/conn"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/cputil"
	"github.com/pingcap/dm/pkg/gtid"
)

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testSchema struct {
	host     string
	port     int
	user     string
	password string
	db       *conn.BaseDB
	mockDB   sqlmock.Sqlmock
}

var _ = Suite(&testSchema{})

func (t *testSchema) SetUpSuite(c *C) {
	t.setUpDBConn(c)
}

func (t *testSchema) TestTearDown(c *C) {
	t.db.Close()
}

func (t *testSchema) setUpDBConn(c *C) {
	t.host = os.Getenv("MYSQL_HOST")
	if t.host == "" {
		t.host = "127.0.0.1"
	}
	t.port, _ = strconv.Atoi(os.Getenv("MYSQL_PORT"))
	if t.port == 0 {
		t.port = 3306
	}
	t.user = os.Getenv("MYSQL_USER")
	if t.user == "" {
		t.user = "root"
	}
	t.password = os.Getenv("MYSQL_PSWD")

	cfg := config.DBConfig{
		Host:     t.host,
		Port:     t.port,
		User:     t.user,
		Password: t.password,
		Session:  map[string]string{"sql_log_bin": "off"}, // do not enable binlog to break other unit test cases.
	}
	cfg.Adjust()

	var err error
	t.mockDB = conn.InitMockDB(c)
	t.db, err = conn.DefaultDBProvider.Apply(cfg)
	c.Assert(err, IsNil)
}

func (t *testSchema) TestSchemaV106ToV20x(c *C) {
	var (
		_, currFile, _, _ = runtime.Caller(0)
		v1DataDir         = filepath.Join(filepath.Dir(currFile), "v106_data_for_test")
		tctx              = tcontext.Background()

		cfg = &config.SubTaskConfig{
			Name:       "test",
			SourceID:   "mysql-replica-01",
			ServerID:   429523137,
			MetaSchema: "dm_meta_v106_test",
			From: config.DBConfig{
				Host:     t.host,
				Port:     t.port,
				User:     t.user,
				Password: t.password,
			},
		}

		endPos = gmysql.Position{
			Name: "mysql-bin|000001.000001",
			Pos:  3574,
		}
		endGS, _ = gtid.ParserGTID(gmysql.MySQLFlavor, "ccb992ad-a557-11ea-ba6a-0242ac140002:1-16")
	)

	c.Assert(failpoint.Enable("github.com/pingcap/dm/pkg/v1dbschema/MockGetGTIDsForPos", `return("ccb992ad-a557-11ea-ba6a-0242ac140002:10-16")`), IsNil)
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/dm/pkg/v1dbschema/MockGetGTIDsForPos")
	c.Assert(failpoint.Enable("github.com/pingcap/dm/pkg/utils/GetGTIDPurged", `return("ccb992ad-a557-11ea-ba6a-0242ac140002:1-9")`), IsNil)
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/dm/pkg/utils/GetGTIDPurged")

	dbConn, err := t.db.GetBaseConn(tctx.Ctx)
	c.Assert(err, IsNil)
	defer func() {
		_, err = dbConn.ExecuteSQL(tctx, nil, cfg.Name, []string{
			`DROP DATABASE ` + cfg.MetaSchema,
		})
	}()

	// create metadata schema.
	createSchemaSQL := `CREATE DATABASE IF NOT EXISTS ` + cfg.MetaSchema
	t.mockDB.ExpectBegin()
	t.mockDB.ExpectExec(createSchemaSQL).WithArgs().WillReturnResult(sqlmock.NewErrorResult(nil))
	t.mockDB.ExpectCommit()
	_, err = dbConn.ExecuteSQL(tctx, nil, cfg.Name, []string{createSchemaSQL})
	c.Assert(err, IsNil)

	// create v1.0.6 checkpoint table.
	createCpV106, err := ioutil.ReadFile(filepath.Join(v1DataDir, "v106_syncer_checkpoint-schema.sql"))
	c.Assert(err, IsNil)
	createCpV106SQL := string(createCpV106)
	t.mockDB.ExpectBegin()
	t.mockDB.ExpectExec(createCpV106SQL[:10] + ".*").WithArgs().WillReturnResult(sqlmock.NewErrorResult(nil))
	t.mockDB.ExpectCommit()
	_, err = dbConn.ExecuteSQL(tctx, nil, cfg.Name, []string{createCpV106SQL})
	c.Assert(err, IsNil)

	// create v1.0.6 online DDL metadata table.
	createOnV106, err := ioutil.ReadFile(filepath.Join(v1DataDir, "v106_syncer_onlineddl-schema.sql"))
	c.Assert(err, IsNil)
	createOnV106SQL := string(createOnV106)
	t.mockDB.ExpectBegin()
	t.mockDB.ExpectExec(createOnV106SQL[:10] + ".*").WithArgs().WillReturnResult(sqlmock.NewErrorResult(nil))
	t.mockDB.ExpectCommit()
	_, err = dbConn.ExecuteSQL(tctx, nil, cfg.Name, []string{createOnV106SQL})
	c.Assert(err, IsNil)

	// update position.
	insertCpV106, err := ioutil.ReadFile(filepath.Join(v1DataDir, "v106_syncer_checkpoint.sql"))
	c.Assert(err, IsNil)
	insertCpV106sSQL := strings.ReplaceAll(string(insertCpV106), "123456", strconv.FormatUint(uint64(endPos.Pos), 10))
	// load syncer checkpoint into table.
	t.mockDB.ExpectBegin()
	t.mockDB.ExpectExec(insertCpV106sSQL[:10] + ".*").WithArgs().WillReturnResult(sqlmock.NewErrorResult(nil))
	t.mockDB.ExpectCommit()
	_, err = dbConn.ExecuteSQL(tctx, nil, cfg.Name, []string{insertCpV106sSQL})
	c.Assert(err, IsNil)

	// load online DDL metadata into table.
	insertOnV106, err := ioutil.ReadFile(filepath.Join(v1DataDir, "v106_syncer_onlineddl.sql"))
	insertOnV106SQL := string(insertOnV106)
	t.mockDB.ExpectBegin()
	t.mockDB.ExpectExec(insertOnV106SQL[:10] + ".*").WithArgs().WillReturnResult(sqlmock.NewErrorResult(nil))
	t.mockDB.ExpectCommit()
	c.Assert(err, IsNil)
	_, err = dbConn.ExecuteSQL(tctx, nil, cfg.Name, []string{insertOnV106SQL})
	c.Assert(err, IsNil)

	// update schema without GTID enabled.
	// mock updateSyncerCheckpoint
	t.mockDB.ExpectBegin()
	t.mockDB.ExpectExec("ALTER TABLE `dm_meta_v106_test`.`test_syncer_checkpoint` ADD COLUMN binlog_gtid TEXT AFTER binlog_pos").WithArgs().WillReturnResult(sqlmock.NewErrorResult(nil))
	t.mockDB.ExpectExec("ALTER TABLE `dm_meta_v106_test`.`test_syncer_checkpoint` ADD COLUMN table_info JSON NOT NULL AFTER binlog_gtid").WithArgs().WillReturnResult(sqlmock.NewErrorResult(nil))
	t.mockDB.ExpectCommit()
	// mock updateSyncerOnlineDDLMeta
	t.mockDB.ExpectBegin()
	t.mockDB.ExpectExec("UPDATE `dm_meta_v106_test`.`test_onlineddl`.*").WithArgs(cfg.SourceID, fmt.Sprint(cfg.ServerID)).WillReturnResult(sqlmock.NewErrorResult(nil))
	t.mockDB.ExpectCommit()
	c.Assert(UpdateSchema(tctx, t.db, cfg), IsNil)

	// verify the column data of online DDL already updated.
	countSQL := fmt.Sprintf("SELECT count(1) FROM %s", dbutil.TableName(cfg.MetaSchema, cputil.SyncerOnlineDDL(cfg.Name)))
	t.mockDB.ExpectQuery(countSQL[:10] + ".*").WillReturnRows(sqlmock.NewRows([]string{"COUNT(1)"}).AddRow(2))
	rows, err := dbConn.QuerySQL(tctx, countSQL)
	c.Assert(err, IsNil)
	c.Assert(rows.Next(), IsTrue)
	var count int
	c.Assert(rows.Scan(&count), IsNil)
	c.Assert(rows.Next(), IsFalse)
	c.Assert(rows.Err(), IsNil)
	defer rows.Close()
	c.Assert(count, Equals, 2)

	// verify the column data of checkpoint not updated.
	selectSQL := fmt.Sprintf(`SELECT binlog_gtid FROM %s`, dbutil.TableName(cfg.MetaSchema, cputil.SyncerCheckpoint(cfg.Name)))
	t.mockDB.ExpectQuery(selectSQL[:10] + ".*").WithArgs().WillReturnRows(sqlmock.NewRows([]string{"binlog_gtid"}).AddRow(nil))
	rows, err = dbConn.QuerySQL(tctx, selectSQL)
	c.Assert(err, IsNil)
	for rows.Next() {
		var gs sql.NullString
		c.Assert(rows.Scan(&gs), IsNil)
		c.Assert(gs.Valid, IsFalse)
	}
	c.Assert(rows.Err(), IsNil)
	defer rows.Close()

	// update schema with GTID enabled.
	cfg.EnableGTID = true
	c.Assert(failpoint.Enable("github.com/pingcap/dm/pkg/v1dbschema/MockGetGlobalPos", `return("mysql-bin.000001")`), IsNil)
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/dm/pkg/v1dbschema/MockGetGlobalPos")

	// mock updateSyncerCheckpoint
	t.mockDB.ExpectBegin()
	t.mockDB.ExpectExec("ALTER TABLE `dm_meta_v106_test`.`test_syncer_checkpoint` ADD COLUMN binlog_gtid TEXT AFTER binlog_pos").WithArgs().WillReturnResult(sqlmock.NewErrorResult(nil))
	t.mockDB.ExpectExec("ALTER TABLE `dm_meta_v106_test`.`test_syncer_checkpoint` ADD COLUMN table_info JSON NOT NULL AFTER binlog_gtid").WithArgs().WillReturnResult(sqlmock.NewErrorResult(nil))
	t.mockDB.ExpectCommit()
	t.mockDB.ExpectBegin()
	t.mockDB.ExpectExec("UPDATE `dm_meta_v106_test`.`test_syncer_checkpoint` SET binlog_gtid.*").
		WithArgs(endGS.String(), cfg.SourceID, true).WillReturnResult(sqlmock.NewErrorResult(nil))
	t.mockDB.ExpectCommit()
	// mock updateSyncerOnlineDDLMeta
	t.mockDB.ExpectBegin()
	t.mockDB.ExpectExec("UPDATE `dm_meta_v106_test`.`test_onlineddl`.*").WithArgs(cfg.SourceID, fmt.Sprint(cfg.ServerID)).WillReturnResult(sqlmock.NewErrorResult(nil))
	t.mockDB.ExpectCommit()
	c.Assert(UpdateSchema(tctx, t.db, cfg), IsNil)

	// verify the column data of global checkpoint already updated.
	selectSQL = fmt.Sprintf(`SELECT binlog_gtid FROM %s WHERE is_global=1`, dbutil.TableName(cfg.MetaSchema, cputil.SyncerCheckpoint(cfg.Name)))
	t.mockDB.ExpectQuery(selectSQL[:10] + ".*").WithArgs().WillReturnRows(sqlmock.NewRows([]string{"binlog_gtid"}).AddRow(endGS.String()))
	rows, err = dbConn.QuerySQL(tctx, selectSQL)
	c.Assert(err, IsNil)
	c.Assert(rows.Next(), IsTrue)
	var gs sql.NullString
	c.Assert(rows.Scan(&gs), IsNil)
	c.Assert(rows.Next(), IsFalse)
	c.Assert(rows.Err(), IsNil)
	defer rows.Close()
	c.Assert(gs.String, Equals, endGS.String())

	selectSQL = fmt.Sprintf(`SELECT binlog_gtid FROM %s WHERE is_global!=1`, dbutil.TableName(cfg.MetaSchema, cputil.SyncerCheckpoint(cfg.Name)))
	t.mockDB.ExpectQuery(selectSQL).WithArgs().WillReturnRows(sqlmock.NewRows([]string{"binlog_gtid"}).AddRow(nil))
	rows, err = dbConn.QuerySQL(tctx, selectSQL)
	c.Assert(err, IsNil)
	for rows.Next() {
		var gs sql.NullString
		c.Assert(rows.Scan(&gs), IsNil)
		c.Assert(gs.Valid, IsFalse)
	}
	c.Assert(rows.Err(), IsNil)
	defer rows.Close()
	c.Assert(t.mockDB.ExpectationsWereMet(), IsNil)
}
