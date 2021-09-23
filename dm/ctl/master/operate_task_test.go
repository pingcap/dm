// Copyright 2021 PingCAP, Inc.
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

package master

import (
	"github.com/pingcap/check"
	"github.com/spf13/cobra"
)

func (t *testCtlMaster) TestParseBatchTaskParameters(c *check.C) {
	{
		cmd := prepareTestCmd()
		_ = cmd.ParseFlags([]string{"task-name"})
		_, _, ok := parseOperateSourceTaskParams(cmd)
		c.Assert(ok, check.IsFalse)
	}
	{
		cmd := prepareTestCmd()
		_, _, ok := parseOperateSourceTaskParams(cmd)
		c.Assert(ok, check.IsFalse)
	}
	{
		cmd := prepareTestCmd()
		_ = cmd.ParseFlags([]string{"-s", "source-name", "-s", "source-name2"})
		_, _, ok := parseOperateSourceTaskParams(cmd)
		c.Assert(ok, check.IsFalse)
	}
	{
		cmd := prepareTestCmd()
		_ = cmd.ParseFlags([]string{"-s", "source-name"})
		source, _, ok := parseOperateSourceTaskParams(cmd)
		c.Assert(source, check.Equals, "source-name")
		c.Assert(ok, check.IsTrue)
	}
	{
		cmd := prepareTestCmd()
		_ = cmd.ParseFlags([]string{"-s", "source-name"})
		source, batchSize, ok := parseOperateSourceTaskParams(cmd)
		c.Assert(source, check.Equals, "source-name")
		c.Assert(batchSize, check.Equals, defaultBatchSize)
		c.Assert(ok, check.IsTrue)
	}
	{
		cmd := prepareTestCmd()
		_ = cmd.ParseFlags([]string{"-s", "source-name", "--batch-size", "2"})
		source, batchSize, ok := parseOperateSourceTaskParams(cmd)
		c.Assert(source, check.Equals, "source-name")
		c.Assert(batchSize, check.Equals, 2)
		c.Assert(ok, check.IsTrue)
	}
}

func prepareTestCmd() *cobra.Command {
	cmd := NewPauseTaskCmd()
	// --source is added in ctl package, import it may cause cyclic import, so we mock one
	cmd.PersistentFlags().StringSliceVarP(&[]string{}, "source", "s", []string{}, "MySQL Source ID.")
	return cmd
}
