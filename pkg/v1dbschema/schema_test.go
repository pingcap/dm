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
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/dbutil"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/conn"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/cputil"
)

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testSchema struct{}

var _ = Suite(&testSchema{})

func (t *testSchema) setUpDBConn(c *C) *conn.BaseDB {
	host := os.Getenv("MYSQL_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port, _ := strconv.Atoi(os.Getenv("MYSQL_PORT"))
	if port == 0 {
		port = 3306
	}
	user := os.Getenv("MYSQL_USER")
	if user == "" {
		user = "root"
	}
	password := os.Getenv("MYSQL_PSWD")

	cfg := config.DBConfig{
		Host:     host,
		Port:     port,
		User:     user,
		Password: password,
		Session:  map[string]string{"sql_log_bin ": "off"}, // do not enable binlog to break other unit test cases.
	}
	cfg.Adjust()

	db, err := conn.DefaultDBProvider.Apply(cfg)
	c.Assert(err, IsNil)

	return db
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
		}
	)

	db := t.setUpDBConn(c)
	defer db.Close()
	dbConn, err := db.GetBaseConn(tctx.Ctx)
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

	// load online DDL metadata into table.
	insertOnV106, err := ioutil.ReadFile(filepath.Join(v1DataDir, "v106_syncer_onlineddl.sql"))
	c.Assert(err, IsNil)
	_, err = dbConn.ExecuteSQL(tctx, nil, cfg.Name, []string{
		string(insertOnV106),
	})
	c.Assert(err, IsNil)

	// update schema without GTID enabled.
	c.Assert(UpdateSchema(tctx, db, cfg), IsNil)

	// verify the column data of online DDL already updated.
	rows, err := dbConn.QuerySQL(tctx, fmt.Sprintf(`SELECT count(*) FROM %s`, dbutil.TableName(cfg.MetaSchema, cputil.SyncerOnlineDDL(cfg.Name))))
	c.Assert(err, IsNil)
	c.Assert(rows.Next(), IsTrue)
	var count int
	err = rows.Scan(&count)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 2)
	c.Assert(rows.Next(), IsFalse)
	c.Assert(rows.Err(), IsNil)

	// update schema with GTID enabled.
	cfg.EnableGTID = true
	c.Assert(UpdateSchema(tctx, db, cfg), ErrorMatches, ".*Not Implemented.*")
}
