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
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/filter"

	"github.com/pingcap/dm/dm/config"
)

var _ = Suite(&testHeartbeatSuite{})

type testHeartbeatSuite struct {
	from config.DBConfig
	lag  map[string]float64
}

func (t *testHeartbeatSuite) SetUpSuite(c *C) {
	t.from = getDBConfigFromEnv()
	t.lag = make(map[string]float64)
	reportLagFunc = t.reportLag
}

func (t *testHeartbeatSuite) TestTearDown(c *C) {
	reportLagFunc = reportLag
}

func (t *testHeartbeatSuite) reportLag(taskName string, lag float64) {
	t.lag[taskName] = lag
}

func (t *testHeartbeatSuite) TestHeartbeatConfig(c *C) {
	cfg1 := &HeartbeatConfig{
		serverID:       123,
		primaryCfg:     t.from,
		updateInterval: int64(1),
		reportInterval: int64(1),
	}

	cfg2 := &HeartbeatConfig{
		serverID:       234,
		primaryCfg:     t.from,
		updateInterval: int64(1),
		reportInterval: int64(1),
	}

	err := cfg1.Equal(cfg2)
	c.Assert(err, ErrorMatches, ".*serverID not equal.*")
}

func (t *testHeartbeatSuite) TestHeartbeat(c *C) {
	heartbeat, err := GetHeartbeat(&HeartbeatConfig{
		serverID:       123,
		primaryCfg:     t.from,
		updateInterval: int64(1),
		reportInterval: int64(1),
	})
	c.Assert(err, IsNil)
	err = heartbeat.AddTask("heartbeat_test_1")
	c.Assert(err, IsNil)

	err = heartbeat.AddTask("heartbeat_test_1")
	c.Assert(err, ErrorMatches, ".*already exists.*")

	err = heartbeat.AddTask("heartbeat_test_2")
	c.Assert(err, IsNil)

	err = heartbeat.updateTS()
	c.Assert(err, IsNil)

	err = heartbeat.calculateLag(context.Background())
	c.Assert(err, IsNil)

	c.Assert(t.lag, Not(Equals), float64(0))
	oldlag1 := t.lag["heartbeat_test_1"]
	oldlag2 := t.lag["heartbeat_test_2"]

	heartbeat.TryUpdateTaskTs("heartbeat_test_1", filter.DMHeartbeatSchema, filter.DMHeartbeatTable, [][]interface{}{
		{
			"2019-05-15 15:25:42",
			int32(123),
		},
	})

	err = heartbeat.calculateLag(context.Background())
	c.Assert(err, IsNil)
	c.Assert(t.lag["heartbeat_test_1"], Not(Equals), oldlag1)
	c.Assert(t.lag["heartbeat_test_2"], Equals, oldlag2)

	err = heartbeat.RemoveTask("wrong")
	c.Assert(err, ErrorMatches, ".*not found.*")

	err = heartbeat.RemoveTask("heartbeat_test_1")
	c.Assert(err, IsNil)

	err = heartbeat.RemoveTask("heartbeat_test_2")
	c.Assert(err, IsNil)
}
