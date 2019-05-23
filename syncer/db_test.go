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

package syncer

import (
	. "github.com/pingcap/check"
	gouuid "github.com/satori/go.uuid"
	"github.com/siddontang/go-mysql/mysql"

	"github.com/pingcap/dm/pkg/utils"
)

func (s *testSyncerSuite) TestGetServerUUID(c *C) {
	uuid, err := utils.GetServerUUID(s.db, "mysql")
	c.Assert(err, IsNil)
	_, err = gouuid.FromString(uuid)
	c.Assert(err, IsNil)
}

func (s *testSyncerSuite) TestGetServerID(c *C) {
	id, err := utils.GetServerID(s.db)
	c.Assert(err, IsNil)
	c.Assert(id, Greater, int64(0))
}

func (s *testSyncerSuite) TestBinaryLogs(c *C) {
	files, err := getBinaryLogs(s.db)
	c.Assert(err, IsNil)
	c.Assert(files, Not(HasLen), 0)

	fileNum := len(files)
	pos := mysql.Position{
		Name: files[fileNum-1].name,
		Pos:  0,
	}

	remainingSize, err := countBinaryLogsSize(pos, s.db)
	c.Assert(err, IsNil)
	c.Assert(remainingSize, Equals, files[fileNum-1].size)

	s.db.Exec("FLUSH BINARY LOGS")
	files, err = getBinaryLogs(s.db)
	c.Assert(err, IsNil)
	c.Assert(files, HasLen, fileNum+1)

	pos = mysql.Position{
		Name: files[fileNum].name,
		Pos:  0,
	}

	remainingSize, err = countBinaryLogsSize(pos, s.db)
	c.Assert(err, IsNil)
	c.Assert(remainingSize, Equals, files[fileNum].size)

}
