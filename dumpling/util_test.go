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

package dumpling

import (
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/dumpling/v4/export"
)

var _ = Suite(&testDumplingSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testDumplingSuite struct {
}

func (m *testDumplingSuite) TestParseArgs(c *C) {
	extraArgs := `--statement-size=100 --where "t > 10" --threads 8 -F 50`
	cfg := export.DefaultConfig()
	err := parseExtraArgs(cfg, strings.Fields(extraArgs))
	c.Assert(err, IsNil)
	c.Assert(cfg.StatementSize, Equals, uint64(100))
	c.Assert(cfg.Where, Equals, "t > 10")
	c.Assert(cfg.Threads, Equals, 8)
	c.Assert(cfg.FileSize, Equals, uint64(50))

	extraArgs = `--statement-size=100 --skip-tz-utc`
	err = parseExtraArgs(cfg, strings.Fields(extraArgs))
	c.Assert(err, NotNil)
}
