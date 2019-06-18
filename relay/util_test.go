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

package relay

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"

	. "github.com/pingcap/check"
	gmysql "github.com/siddontang/go-mysql/mysql"

	"github.com/pingcap/dm/pkg/utils"
)

var _ = Suite(&testUtilSuite{})

type testUtilSuite struct {
	host     string
	port     int
	user     string
	password string
	db       *sql.DB
}

func (t *testUtilSuite) SetUpSuite(c *C) {
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

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4", t.user, t.password, t.host, t.port)
	db, err := sql.Open("mysql", dsn)
	c.Assert(err, IsNil)
	t.db = db
}

func (t *testUtilSuite) TestIsNewServer(c *C) {
	flavor := gmysql.MySQLFlavor

	// no prevUUID, is new server.
	isNew, err := isNewServer("", t.db, flavor)
	c.Assert(err, IsNil)
	c.Assert(isNew, IsTrue)

	// different server
	isNew, err = isNewServer("not-exists-uuid.000001", t.db, flavor)
	c.Assert(err, IsNil)
	c.Assert(isNew, IsTrue)

	// the same server
	currUUID, err := utils.GetServerUUID(t.db, flavor)
	c.Assert(err, IsNil)
	isNew, err = isNewServer(fmt.Sprintf("%s.000001", currUUID), t.db, flavor)
	c.Assert(err, IsNil)
	c.Assert(isNew, IsFalse)
}
