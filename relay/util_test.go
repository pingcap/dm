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
	"context"
	"fmt"

	. "github.com/pingcap/check"
	gmysql "github.com/siddontang/go-mysql/mysql"

	"github.com/pingcap/dm/pkg/utils"
)

var _ = Suite(&testUtilSuite{})

type testUtilSuite struct{}

func (t *testUtilSuite) TestIsNewServer(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), utils.DefaultDBTimeout)
	defer cancel()

	baseDB, err := openDBForTest()
	c.Assert(err, IsNil)
	defer baseDB.Close()
	db := baseDB.DB

	flavor := gmysql.MySQLFlavor

	// no prevUUID, is new server.
	isNew, err := isNewServer(ctx, "", db, flavor)
	c.Assert(err, IsNil)
	c.Assert(isNew, IsTrue)

	// different server
	isNew, err = isNewServer(ctx, "not-exists-uuid.000001", db, flavor)
	c.Assert(err, IsNil)
	c.Assert(isNew, IsTrue)

	// the same server
	currUUID, err := utils.GetServerUUID(ctx, db, flavor)
	c.Assert(err, IsNil)
	isNew, err = isNewServer(ctx, fmt.Sprintf("%s.000001", currUUID), db, flavor)
	c.Assert(err, IsNil)
	c.Assert(isNew, IsFalse)
}
