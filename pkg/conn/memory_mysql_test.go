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
	. "github.com/pingcap/check"

	"github.com/pingcap/dm/dm/config"
)

var _ = Suite(&testMemoryMysqlSuite{})

type testMemoryMysqlSuite struct{}

func (t *testMemoryMysqlSuite) TestNewMemoryMysqlServer(c *C) {
	dbDFG := config.GetDBConfigFromEnv()
	server := NewMemoryMysqlServer(dbDFG.Host, dbDFG.User, dbDFG.Password, dbDFG.Port)

	go func() {
		c.Assert(server.Start(), IsNil)
	}()

	defer server.Close()

	db, err := DefaultDBProvider.Apply(dbDFG)
	c.Assert(err, IsNil)
	c.Assert(db.DB.Ping(), IsNil)

	_, err = db.DB.Exec("show databases;")
	c.Assert(err, IsNil)
}
