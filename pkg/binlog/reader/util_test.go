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

package reader

import (
	"context"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	. "github.com/pingcap/check"

	"github.com/pingcap/dm/pkg/utils"
)

// added to testTCPReaderSuite to re-use DB connection.
func (t *testTCPReaderSuite) TestGetGTIDsForPos(c *C) {
	var (
		cfg = replication.BinlogSyncerConfig{
			ServerID:       serverIDs[1],
			Flavor:         flavor,
			Host:           t.host,
			Port:           uint16(t.port),
			User:           t.user,
			Password:       t.password,
			UseDecimal:     false,
			VerifyChecksum: true,
		}
		ctx, cancel = context.WithTimeout(context.Background(), utils.DefaultDBTimeout)
	)
	defer cancel()

	endPos, endGS, err := utils.GetMasterStatus(ctx, t.db, flavor)
	c.Assert(err, IsNil)

	r1 := NewTCPReader(cfg)
	c.Assert(r1, NotNil)
	defer r1.Close()

	gs, err := GetGTIDsForPos(ctx, r1, endPos)
	c.Assert(err, IsNil)
	c.Assert(gs.Equal(endGS), IsTrue)

	// try to get for an invalid pos.
	r2 := NewTCPReader(cfg)
	c.Assert(r2, NotNil)
	defer r2.Close()
	gs, err = GetGTIDsForPos(ctx, r2, gmysql.Position{
		Name: endPos.Name,
		Pos:  endPos.Pos - 1,
	})
	c.Assert(err, ErrorMatches, ".*invalid position .* or GTID not enabled in upstream.*")
	c.Assert(gs, IsNil)
}

// added to testTCPReaderSuite to re-use DB connection.
func (t *testTCPReaderSuite) TestGetPreviousGTIDFromGTIDSet(c *C) {
	var (
		cfg = replication.BinlogSyncerConfig{
			ServerID:       serverIDs[1],
			Flavor:         flavor,
			Host:           t.host,
			Port:           uint16(t.port),
			User:           t.user,
			Password:       t.password,
			UseDecimal:     false,
			VerifyChecksum: true,
		}
		ctx, cancel = context.WithTimeout(context.Background(), utils.DefaultDBTimeout)
	)
	defer cancel()

	_, endGS, err := utils.GetMasterStatus(ctx, t.db, flavor)
	c.Assert(err, IsNil)

	r1 := NewTCPReader(cfg)
	c.Assert(r1, NotNil)
	defer r1.Close()

	gs, err := GetPreviousGTIDFromGTIDSet(ctx, r1, endGS)
	c.Assert(err, IsNil)
	c.Assert(endGS.Contain(gs), IsTrue)
}
