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
		tctx              = tcontext.Background()
		taskName          = "test"
		dbName            = "dm_meta_v106_test"
		syncerCptableName = dbutil.TableName(dbName, taskName+"_syncer_checkpoint")
	)

	db := t.setUpDBConn(c)
	defer db.Close()
	dbConn, err := db.GetBaseConn(tctx.Ctx)
	c.Assert(err, IsNil)

	defer func() {
		_, err = dbConn.ExecuteSQL(tctx, nil, taskName, []string{
			`DROP DATABASE ` + dbName,
		})
	}()

	// create metadata schema.
	_, err = dbConn.ExecuteSQL(tctx, nil, taskName, []string{
		`CREATE DATABASE IF NOT EXISTS ` + dbName,
	})
	c.Assert(err, IsNil)

	// create v1.0.6 checkpoint table.
	_, currFile, _, _ := runtime.Caller(0)
	v1DataDir := filepath.Join(filepath.Dir(currFile), "v106_data_for_test")
	createV106, err := ioutil.ReadFile(filepath.Join(v1DataDir, "v106_syncer_checkpoint-schema.sql"))
	c.Assert(err, IsNil)
	_, err = dbConn.ExecuteSQL(tctx, nil, taskName, []string{
		string(createV106),
	})
	c.Assert(err, IsNil)

	c.Assert(UpdateSyncerCheckpoint(tctx, taskName, syncerCptableName, db, false), IsNil)

	c.Assert(UpdateSyncerCheckpoint(tctx, taskName, syncerCptableName, db, true), ErrorMatches, ".*Not Implemented.*")
}
