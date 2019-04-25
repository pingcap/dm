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

package event

import (
	"time"

	. "github.com/pingcap/check"
	gmysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"

	"github.com/pingcap/dm/pkg/gtid"
)

var _ = Suite(&testHelperSuite{})

type testHelperSuite struct {
}

func (t *testHelperSuite) TestGTIDsFromPreviousGTIDsEvent(c *C) {
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
		}
		latestPos uint32 = 4
	)

	// invalid binlog type, QueryEvent
	queryEv, err := GenQueryEvent(header, latestPos, 0, 0, 0, nil, []byte("schema"), []byte("BEGIN"))
	c.Assert(err, IsNil)
	gSet, err := GTIDsFromPreviousGTIDsEvent(queryEv)
	c.Assert(err, ErrorMatches, ".*should be a GenericEvent in go-mysql.*")
	c.Assert(gSet, IsNil)

	// invalid binlog type, USER_VAR_EVENT
	userVarEv, err := GenDummyEvent(header, latestPos, MinUserVarEventLen)
	c.Assert(err, IsNil)
	gSet, err = GTIDsFromPreviousGTIDsEvent(userVarEv)
	c.Assert(err, ErrorMatches, ".*invalid event type.*")
	c.Assert(gSet, IsNil)

	// valid MySQL GTIDs
	gtidStr := "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14,406a3f61-690d-11e7-87c5-6c92bf46f384:1-94321383,53bfca22-690d-11e7-8a62-18ded7a37b78:1-495,686e1ab6-c47e-11e7-a42c-6c92bf46f384:1-34981190,03fc0263-28c7-11e7-a653-6c0b84d59f30:1-7041423,05474d3c-28c7-11e7-8352-203db246dd3d:1-170,10b039fc-c843-11e7-8f6a-1866daf8d810:1-308290454"
	gSetExpect, err := gtid.ParserGTID(gmysql.MySQLFlavor, gtidStr)
	c.Assert(err, IsNil)
	previousGTIDEv, err := GenPreviousGTIDsEvent(header, latestPos, gSetExpect)
	c.Assert(err, IsNil)
	gSet, err = GTIDsFromPreviousGTIDsEvent(previousGTIDEv)
	c.Assert(err, IsNil)
	c.Assert(gSetExpect, DeepEquals, gSet)
}
