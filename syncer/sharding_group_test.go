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
	"sort"

	. "github.com/pingcap/check"
	"github.com/siddontang/go-mysql/mysql"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/binlog"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/terror"
)

var _ = Suite(&testShardingGroupSuite{})

type testShardingGroupSuite struct {
	cfg *config.SubTaskConfig
}

func (t *testShardingGroupSuite) SetUpSuite(c *C) {
	t.cfg = &config.SubTaskConfig{
		SourceID:   "mysql-replica-01",
		MetaSchema: "test",
		Name:       "checkpoint_ut",
	}
}

func (t *testShardingGroupSuite) TestLowestFirstPosInGroups(c *C) {
	ddls := []string{"DUMMY DDL"}

	k := NewShardingGroupKeeper(tcontext.Background(), t.cfg)

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

func (t *testShardingGroupSuite) TestMergeAndLeave(c *C) {
	var (
		source1 = "db1.tbl1"
		source2 = "db1.tbl2"
		source3 = "db1.tbl3"
	)
	k := NewShardingGroupKeeper(tcontext.Background(), t.cfg)
	g1 := NewShardingGroup(k.cfg.SourceID, k.shardMetaSchema, k.shardMetaTable, []string{source1, source2}, nil, false, "", false)
	c.Assert(g1.Sources(), DeepEquals, map[string]bool{source1: false, source2: false})

	needShardingHandle, synced, remain, err := g1.Merge([]string{source3})
	c.Assert(err, IsNil)
	c.Assert(needShardingHandle, IsFalse)
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 3)

	// repeat merge has no side effect
	needShardingHandle, synced, remain, err = g1.Merge([]string{source3})
	c.Assert(err, IsNil)
	c.Assert(needShardingHandle, IsFalse)
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 3)

	err = g1.Leave([]string{source1})
	c.Assert(err, IsNil)
	c.Assert(g1.Sources(), DeepEquals, map[string]bool{source3: false, source2: false})

	// repeat leave has no side effect
	err = g1.Leave([]string{source1})
	c.Assert(err, IsNil)
	c.Assert(g1.Sources(), DeepEquals, map[string]bool{source3: false, source2: false})

	ddls := []string{"DUMMY DDL"}
	pos1 := mysql.Position{Name: "mysql-bin.000002", Pos: 123}
	endPos1 := mysql.Position{Name: "mysql-bin.000002", Pos: 456}
	_, _, _, err = g1.TrySync(source1, binlog.Location{Position: pos1}, binlog.Location{Position: endPos1}, ddls)
	c.Assert(err, IsNil)

	_, _, _, err = g1.Merge([]string{source1})
	c.Assert(terror.ErrSyncUnitAddTableInSharding.Equal(err), IsTrue)
	err = g1.Leave([]string{source2})
	c.Assert(terror.ErrSyncUnitDropSchemaTableInSharding.Equal(err), IsTrue)
}

func (t *testShardingGroupSuite) TestSync(c *C) {
	var (
		source1  = "`db1`.`tbl1`"
		source2  = "`db1`.`tbl2`"
		pos11    = mysql.Position{Name: "mysql-bin.000002", Pos: 123}
		endPos11 = mysql.Position{Name: "mysql-bin.000002", Pos: 456}
		pos12    = mysql.Position{Name: "mysql-bin.000002", Pos: 789}
		endPos12 = mysql.Position{Name: "mysql-bin.000002", Pos: 999}
		pos21    = mysql.Position{Name: "mysql-bin.000001", Pos: 123}
		endPos21 = mysql.Position{Name: "mysql-bin.000001", Pos: 456}
		pos22    = mysql.Position{Name: "mysql-bin.000001", Pos: 789}
		endPos22 = mysql.Position{Name: "mysql-bin.000001", Pos: 999}
		ddls1    = []string{"DUMMY DDL"}
		ddls2    = []string{"ANOTHER DUMMY DDL"}
	)
	k := NewShardingGroupKeeper(tcontext.Background(), t.cfg)
	g1 := NewShardingGroup(k.cfg.SourceID, k.shardMetaSchema, k.shardMetaTable, []string{source1, source2}, nil, false, "", false)
	synced, active, remain, err := g1.TrySync(source1, binlog.Location{Position: pos11}, binlog.Location{Position: endPos11}, ddls1)
	c.Assert(err, IsNil)
	c.Assert(synced, IsFalse)
	c.Assert(active, IsTrue)
	c.Assert(remain, Equals, 1)
	synced, active, remain, err = g1.TrySync(source1, binlog.Location{Position: pos12}, binlog.Location{Position: endPos12}, ddls2)
	c.Assert(err, IsNil)
	c.Assert(synced, IsFalse)
	c.Assert(active, IsFalse)
	c.Assert(remain, Equals, 1)

	c.Assert(g1.FirstLocationUnresolved(), DeepEquals, &binlog.Location{Position: pos11})
	c.Assert(g1.FirstEndPosUnresolved(), DeepEquals, &binlog.Location{Position: endPos11})
	loc, err := g1.ActiveDDLFirstLocation()
	c.Assert(err, IsNil)
	c.Assert(loc, DeepEquals, binlog.Location{Position: pos11})

	// not call `TrySync` for source2, beforeActiveDDL is always true
	beforeActiveDDL := g1.CheckSyncing(source2, binlog.Location{Position: pos21})
	c.Assert(beforeActiveDDL, IsTrue)

	info := g1.UnresolvedGroupInfo()
	shouldBe := &pb.ShardingGroup{Target: "", DDLs: ddls1, FirstLocation: binlog.Location{Position: pos11}.String(), Synced: []string{source1}, Unsynced: []string{source2}}
	c.Assert(info, DeepEquals, shouldBe)

	// simple sort for [][]string{[]string{"db1", "tbl2"}, []string{"db1", "tbl1"}}
	tbls1 := g1.Tables()
	tbls2 := g1.UnresolvedTables()
	if tbls1[0][1] != tbls2[0][1] {
		tbls1[0], tbls1[1] = tbls1[1], tbls1[0]
	}
	c.Assert(tbls1, DeepEquals, tbls2)

	// sync first DDL for source2, synced but not resolved
	synced, active, remain, err = g1.TrySync(source2, binlog.Location{Position: pos21}, binlog.Location{Position: endPos21}, ddls1)
	c.Assert(err, IsNil)
	c.Assert(synced, IsTrue)
	c.Assert(active, IsTrue)
	c.Assert(remain, Equals, 0)

	// active DDL is at pos21
	beforeActiveDDL = g1.CheckSyncing(source2, binlog.Location{Position: pos21})
	c.Assert(beforeActiveDDL, IsFalse)

	info = g1.UnresolvedGroupInfo()
	sort.Strings(info.Synced)
	shouldBe = &pb.ShardingGroup{Target: "", DDLs: ddls1, FirstLocation: binlog.Location{Position: pos11}.String(), Synced: []string{source1, source2}, Unsynced: []string{}}
	c.Assert(info, DeepEquals, shouldBe)

	resolved := g1.ResolveShardingDDL()
	c.Assert(resolved, IsFalse)

	// next active DDL not present
	beforeActiveDDL = g1.CheckSyncing(source2, binlog.Location{Position: pos21})
	c.Assert(beforeActiveDDL, IsTrue)

	synced, active, remain, err = g1.TrySync(source2, binlog.Location{Position: pos22}, binlog.Location{Position: endPos22}, ddls2)
	c.Assert(err, IsNil)
	c.Assert(synced, IsTrue)
	c.Assert(active, IsTrue)
	c.Assert(remain, Equals, 0)
	resolved = g1.ResolveShardingDDL()
	c.Assert(resolved, IsTrue)

	// caller should reset sharding group if DDL is successful executed
	g1.Reset()

	info = g1.UnresolvedGroupInfo()
	c.Assert(info, IsNil)
	c.Assert(g1.IsUnresolved(), IsFalse)
	c.Assert(g1.UnresolvedTables(), IsNil)
}

func (t *testShardingGroupSuite) TestTableID(c *C) {
	cases := [][]string{
		{"db", "table"},
		{`d"b`, `t"able"`},
		{"d`b", "t`able"},
	}
	for _, ca := range cases {
		// ignore isSchemaOnly
		id, _ := GenTableID(ca[0], ca[1])
		schema, table := UnpackTableID(id)
		c.Assert(schema, Equals, ca[0])
		c.Assert(table, Equals, ca[1])
	}
}
