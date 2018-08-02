// Copyright 2016 PingCAP, Inc.
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

package syncer

import (
	. "github.com/pingcap/check"
	gouuid "github.com/satori/go.uuid"
	"github.com/siddontang/go-mysql/mysql"
)

func (s *testSyncerSuite) TestGetMasterStatus(c *C) {
	binlogPos, _, err := getMasterStatus(s.db, mysql.MySQLFlavor)
	c.Assert(err, IsNil)
	c.Assert(binlogPos.Name, Not(Equals), "")
	c.Assert(binlogPos.Pos, Not(Equals), 0)
}

func (s *testSyncerSuite) TestGetServerUUID(c *C) {
	uuid, err := getServerUUID(s.db)
	c.Assert(err, IsNil)
	_, err = gouuid.FromString(uuid)
	c.Assert(err, IsNil)
}
