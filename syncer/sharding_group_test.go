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
	"github.com/siddontang/go-mysql/mysql"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/binlog"
	tcontext "github.com/pingcap/dm/pkg/context"
)

var _ = Suite(&testShardingGroupSuite{})

type testShardingGroupSuite struct {
}

func (t *testShardingGroupSuite) TestLowestFirstPosInGroups(c *C) {
	cfg := &config.SubTaskConfig{
		SourceID:   "mysql-replica-01",
		MetaSchema: "test",
		Name:       "checkpoint_ut",
	}

	ddls := []string{"DUMMY DDL"}

	k := NewShardingGroupKeeper(tcontext.Background(), cfg)

	g1 := NewShardingGroup(k.cfg.SourceID, k.shardMetaSchema, k.shardMetaTable, []string{"db1.tbl1", "db1.tbl2"}, nil, false, "", false)
	pos1 := mysql.Position{Name: "mysql-bin.000002", Pos: 123}
	endPos1 := mysql.Position{Name: "mysql-bin.000002", Pos: 456}
	_, _, _, err := g1.TrySync("db1.tbl1", binlog.Location{Position: pos1}, binlog.Location{Position: endPos1}, ddls)
	c.Assert(err, IsNil)

	// lowest
	g2 := NewShardingGroup(k.cfg.SourceID, k.shardMetaSchema, k.shardMetaTable, []string{"db2.tbl1", "db2.tbl2"}, nil, false, "", false)
	pos2 := mysql.Position{Name: "mysql-bin.000001", Pos: 123}
	endPos2 := mysql.Position{Name: "mysql-bin.000001", Pos: 456}
	_, _, _, err = g2.TrySync("db2.tbl1", binlog.Location{Position: pos2}, binlog.Location{Position: endPos2}, ddls)
	c.Assert(err, IsNil)

	g3 := NewShardingGroup(k.cfg.SourceID, k.shardMetaSchema, k.shardMetaTable, []string{"db3.tbl1", "db3.tbl2"}, nil, false, "", false)
	pos3 := mysql.Position{Name: "mysql-bin.000003", Pos: 123}
	endPos3 := mysql.Position{Name: "mysql-bin.000003", Pos: 456}
	_, _, _, err = g3.TrySync("db3.tbl1", binlog.Location{Position: pos3}, binlog.Location{Position: endPos3}, ddls)
	c.Assert(err, IsNil)

	k.groups["db1.tbl"] = g1
	k.groups["db2.tbl"] = g2
	k.groups["db3.tbl"] = g3

	c.Assert(k.lowestFirstLocationInGroups().Position, DeepEquals, pos2)
}
