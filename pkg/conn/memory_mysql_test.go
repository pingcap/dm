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

package conn

import (
	"strconv"

	"github.com/pingcap/check"
	"github.com/tikv/pd/pkg/tempurl"

	"github.com/pingcap/dm/dm/config"
)

var _ = check.Suite(&testMemoryMysqlSuite{})

type testMemoryMysqlSuite struct{}

func (t *testMemoryMysqlSuite) TestNewMemoryMysqlServer(c *check.C) {

	dbCfg := config.GetDBConfigFromEnv()
	freePortStr := tempurl.Alloc()[len("http://127.0.0.1:"):]
	freePort, err := strconv.Atoi(freePortStr)
	c.Assert(err, check.IsNil)
	dbCfg.Port = freePort
	server := NewMemoryMysqlServer(dbCfg.Host, dbCfg.User, dbCfg.Password, dbCfg.Port)
	go func() {
		c.Assert(server.Start(), check.IsNil)
	}()

	defer server.Close()

	db, err := DefaultDBProvider.Apply(dbCfg)
	c.Assert(err, check.IsNil)
	c.Assert(db.DB.Ping(), check.IsNil)

	_, err = db.DB.Exec("show databases;")
	c.Assert(err, check.IsNil)
}
