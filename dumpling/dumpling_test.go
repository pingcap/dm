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
	"context"
	"testing"
	"time"

	"github.com/pingcap/dumpling/v4/export"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb-tools/pkg/filter"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/log"

	. "github.com/pingcap/check"
)

var _ = Suite(&testDumplingSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testDumplingSuite struct {
	cfg *config.SubTaskConfig
}

func (d *testDumplingSuite) SetUpSuite(c *C) {
	dir := c.MkDir()
	d.cfg = &config.SubTaskConfig{
		Name: "dumpling_ut",
		From: config.GetDBConfigFromEnv(),
		LoaderConfig: config.LoaderConfig{
			Dir: dir,
		},
		BAList: &filter.Rules{
			DoDBs: []string{"information_schema"},
		},
	}
	c.Assert(log.InitLogger(&log.Config{}), IsNil)
}

func (d *testDumplingSuite) TestDumpling(c *C) {
	dumpling := NewDumpling(d.cfg)

	err := dumpling.Init(context.Background())
	c.Assert(err, IsNil)
	resultCh := make(chan pb.ProcessResult, 1)

	dumpling.Start(resultCh)

	result := <-resultCh
	c.Assert(len(resultCh), Equals, 0)
	c.Assert(result.IsCanceled, IsFalse)
	c.Assert(len(result.Errors), Equals, 0)

	dumpling.Close()

	// test process returned error

	c.Assert(dumpling.Init(context.Background()), IsNil)

	c.Assert(failpoint.Enable("github.com/pingcap/dm/dumpling/dumpUnitProcessWithError", `return("unknown error")`), IsNil)
	// nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/dm/dumpling/dumpUnitProcessWithError")

	// return error
	dumpling.Start(resultCh)
	result = <-resultCh
	c.Assert(len(resultCh), Equals, 0)
	c.Assert(result.IsCanceled, IsFalse)
	c.Assert(len(result.Errors), Equals, 1)
	c.Assert(result.Errors[0].Message, Equals, "unknown error")

	dumpling.Close()

	// test process is blocked and can be canceled

	c.Assert(dumpling.Init(context.Background()), IsNil)

	// nolint:errcheck
	failpoint.Disable("github.com/pingcap/dm/dumpling/dumpUnitProcessWithError")

	c.Assert(failpoint.Enable("github.com/pingcap/dm/dumpling/dumpUnitProcessCancel", `return("unknown error")`), IsNil)
	// nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/dm/dumpling/dumpUnitProcessCancel")

	dumpling.Start(resultCh)
	select {
	case <-resultCh:
		c.Fatal("expected nothing send to channel")
	case <-time.After(5 * time.Second):
		dumpling.Close()
	}

	result = <-resultCh
	c.Assert(len(resultCh), Equals, 0)
	c.Assert(result.IsCanceled, IsTrue)
	c.Assert(len(result.Errors), Equals, 0)
}

func (d *testDumplingSuite) TestDefaultConfig(c *C) {
	dumpling := NewDumpling(d.cfg)
	ctx := context.Background()
	c.Assert(dumpling.Init(ctx), IsNil)
	c.Assert(dumpling.dumpConfig.StatementSize, Not(Equals), export.UnspecifiedSize)
	c.Assert(dumpling.dumpConfig.Rows, Not(Equals), export.UnspecifiedSize)
}
