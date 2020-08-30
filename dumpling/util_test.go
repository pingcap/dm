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

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/log"

	. "github.com/pingcap/check"
)

var _ = Suite(&testDumplingSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testDumplingSuite struct{}

func (m *testDumplingSuite) TestParseArgs(c *C) {
	logger := log.L()

	cfg := &config.SubTaskConfig{}
	cfg.ExtraArgs = `--statement-size=100 --where "t>10" --threads 8 -F 50B`
	d := NewDumpling(cfg)
	exportCfg, err := d.constructArgs()
	c.Assert(err, IsNil)
	c.Assert(exportCfg.StatementSize, Equals, uint64(100))
	c.Assert(exportCfg.Where, Equals, "t>10")
	c.Assert(exportCfg.Threads, Equals, 8)
	c.Assert(exportCfg.FileSize, Equals, uint64(50))

	extraArgs := `--threads 16 --skip-tz-utc`
	err = parseExtraArgs(&logger, exportCfg, strings.Fields(extraArgs))
	c.Assert(err, NotNil)
	c.Assert(exportCfg.Threads, Equals, 16)
	c.Assert(exportCfg.StatementSize, Equals, uint64(100))

	// no `--tables-list` or `--filter` specified, match anything
	c.Assert(exportCfg.TableFilter.MatchTable("foo", "bar"), IsTrue)
	c.Assert(exportCfg.TableFilter.MatchTable("bar", "foo"), IsTrue)

	// specify `--tables-list`.
	extraArgs = `--threads 16 --tables-list=foo.bar`
	err = parseExtraArgs(&logger, exportCfg, strings.Fields(extraArgs))
	c.Assert(err, IsNil)
	c.Assert(exportCfg.TableFilter.MatchTable("foo", "bar"), IsTrue)
	c.Assert(exportCfg.TableFilter.MatchTable("bar", "foo"), IsFalse)

	// specify `--tables-list` and `--filter`
	extraArgs = `--threads 16 --tables-list=foo.bar --filter=*.foo`
	err = parseExtraArgs(&logger, exportCfg, strings.Fields(extraArgs))
	c.Assert(err, ErrorMatches, ".*cannot pass --tables-list and --filter together.*")

	// only specify `--filter`.
	extraArgs = `--threads 16 --filter=*.foo`
	err = parseExtraArgs(&logger, exportCfg, strings.Fields(extraArgs))
	c.Assert(err, IsNil)
	c.Assert(exportCfg.TableFilter.MatchTable("foo", "bar"), IsFalse)
	c.Assert(exportCfg.TableFilter.MatchTable("bar", "foo"), IsTrue)

	// compatibility for `--no-locks`
	extraArgs = `--no-locks`
	err = parseExtraArgs(&logger, exportCfg, strings.Fields(extraArgs))
	c.Assert(err, IsNil)
	c.Assert(exportCfg.Consistency, Equals, "none")

	// compatibility for `--no-locks`
	extraArgs = `--no-locks --consistency none`
	err = parseExtraArgs(&logger, exportCfg, strings.Fields(extraArgs))
	c.Assert(err, IsNil)
	c.Assert(exportCfg.Consistency, Equals, "none")

	extraArgs = `--consistency lock`
	err = parseExtraArgs(&logger, exportCfg, strings.Fields(extraArgs))
	c.Assert(err, IsNil)
	c.Assert(exportCfg.Consistency, Equals, "lock")

	// compatibility for `--no-locks`
	extraArgs = `--no-locks --consistency lock`
	err = parseExtraArgs(&logger, exportCfg, strings.Fields(extraArgs))
	c.Assert(err.Error(), Equals, "cannot both specify `--no-locks` and `--consistency` other than `none`")
}
