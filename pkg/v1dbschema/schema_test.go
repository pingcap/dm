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

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	gmysql "github.com/siddontang/go-mysql/mysql"

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

	c.Assert(failpoint.Enable("github.com/pingcap/dm/pkg/v1dbschema/MockGetGTIDsForPos", `return("ccb992ad-a557-11ea-ba6a-0242ac140002:10-16")`), IsNil) // need `ResetStart`.
	defer failpoint.Disable("github.com/pingcap/dm/pkg/v1dbschema/MockGetGTIDsForPos")

	dbConn, err := t.db.GetBaseConn(tctx.Ctx)
	c.Assert(err, IsNil)

	defer func() {
		_, err = dbConn.ExecuteSQL(tctx, nil, cfg.Name, []string{
			`DROP DATABASE ` + cfg.MetaSchema,
		})
	}()

	// create metadata schema.
	_, err = dbConn.ExecuteSQL(tctx, nil, cfg.Name, []string{
		`CREATE DATABASE IF NOT EXISTS ` + cfg.MetaSchema,
	})
	c.Assert(err, IsNil)

	// create v1.0.6 checkpoint table.
	createCpV106, err := ioutil.ReadFile(filepath.Join(v1DataDir, "v106_syncer_checkpoint-schema.sql"))
	c.Assert(err, IsNil)
	_, err = dbConn.ExecuteSQL(tctx, nil, cfg.Name, []string{
		string(createCpV106),
	})
	c.Assert(err, IsNil)

	// create v1.0.6 online DDL metadata table.
	createOnV106, err := ioutil.ReadFile(filepath.Join(v1DataDir, "v106_syncer_onlineddl-schema.sql"))
	c.Assert(err, IsNil)
	_, err = dbConn.ExecuteSQL(tctx, nil, cfg.Name, []string{
		string(createOnV106),
	})
	c.Assert(err, IsNil)

	// update position.
	insertCpV106, err := ioutil.ReadFile(filepath.Join(v1DataDir, "v106_syncer_checkpoint.sql"))
	c.Assert(err, IsNil)
	insertCpV106s := strings.ReplaceAll(string(insertCpV106), "123456", strconv.FormatUint(uint64(endPos.Pos), 10))
	// load syncer checkpoint into table.
	_, err = dbConn.ExecuteSQL(tctx, nil, cfg.Name, []string{
		insertCpV106s,
	})
	c.Assert(err, IsNil)

	// load online DDL metadata into table.
	insertOnV106, err := ioutil.ReadFile(filepath.Join(v1DataDir, "v106_syncer_onlineddl.sql"))
	c.Assert(err, IsNil)
	_, err = dbConn.ExecuteSQL(tctx, nil, cfg.Name, []string{
		string(insertOnV106),
	})
	c.Assert(err, IsNil)

	// update schema without GTID enabled.
	c.Assert(UpdateSchema(tctx, t.db, cfg), IsNil)

	// verify the column data of online DDL already updated.
	rows, err := dbConn.QuerySQL(tctx, fmt.Sprintf(`SELECT count(*) FROM %s`, dbutil.TableName(cfg.MetaSchema, cputil.SyncerOnlineDDL(cfg.Name))))
	c.Assert(err, IsNil)
	c.Assert(rows.Next(), IsTrue)
	var count int
	c.Assert(rows.Scan(&count), IsNil)
	c.Assert(rows.Next(), IsFalse)
	c.Assert(rows.Err(), IsNil)
	rows.Close()
	c.Assert(count, Equals, 2)

	// verify the column data of checkpoint not updated.
	rows, err = dbConn.QuerySQL(tctx, fmt.Sprintf(`SELECT binlog_gtid FROM %s`, dbutil.TableName(cfg.MetaSchema, cputil.SyncerCheckpoint(cfg.Name))))
	c.Assert(err, IsNil)
	for rows.Next() {
		var gs sql.NullString
		c.Assert(rows.Scan(&gs), IsNil)
		c.Assert(gs.Valid, IsFalse)
	}
	c.Assert(rows.Err(), IsNil)
	rows.Close()

	// update schema with GTID enabled.
	cfg.EnableGTID = true
	c.Assert(UpdateSchema(tctx, t.db, cfg), IsNil)

	// verify the column data of global checkpoint already updated.
	rows, err = dbConn.QuerySQL(tctx, fmt.Sprintf(`SELECT binlog_gtid FROM %s WHERE is_global=1`, dbutil.TableName(cfg.MetaSchema, cputil.SyncerCheckpoint(cfg.Name))))
	c.Assert(err, IsNil)
	c.Assert(rows.Next(), IsTrue)
	var gs sql.NullString
	c.Assert(rows.Scan(&gs), IsNil)
	c.Assert(rows.Next(), IsFalse)
	c.Assert(rows.Err(), IsNil)
	rows.Close()
	c.Assert(gs.String, Equals, endGS.String())

	rows, err = dbConn.QuerySQL(tctx, fmt.Sprintf(`SELECT binlog_gtid FROM %s WHERE is_global!=1`, dbutil.TableName(cfg.MetaSchema, cputil.SyncerCheckpoint(cfg.Name))))
	c.Assert(err, IsNil)
	for rows.Next() {
		var gs sql.NullString
		c.Assert(rows.Scan(&gs), IsNil)
		c.Assert(gs.Valid, IsFalse)
	}
	c.Assert(rows.Err(), IsNil)
	rows.Close()
}
