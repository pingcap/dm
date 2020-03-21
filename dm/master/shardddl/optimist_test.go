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

package shardddl

import (
	"context"
	"time"

	. "github.com/pingcap/check"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/shardddl/optimism"
	"github.com/pingcap/dm/pkg/utils"
)

type testOptimist struct{}

var _ = Suite(&testOptimist{})

// clear keys in etcd test cluster.
func clearOptimistTestSourceInfoOperation(c *C) {
	c.Assert(optimism.ClearTestInfoOperation(etcdTestCli), IsNil)
}

func (t *testOptimist) TestOptimistSourceTables(c *C) {
	defer clearOptimistTestSourceInfoOperation(c)

	var (
		logger  = log.L()
		o       = NewOptimist(&logger)
		task    = "task"
		source1 = "mysql-replica-1"
		source2 = "mysql-replica-2"
		st1     = optimism.NewSourceTables(task, source1, map[string]map[string]struct{}{
			"db": {"tbl-1": struct{}{}, "tbl-2": struct{}{}},
		})
		st2 = optimism.NewSourceTables(task, source2, map[string]map[string]struct{}{
			"db": {"tbl-1": struct{}{}, "tbl-2": struct{}{}},
		})
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// CASE 1: start without any previous kv and no etcd operation.
	c.Assert(o.Start(ctx, etcdTestCli), IsNil)
	c.Assert(o.tk.FindTables(task), IsNil)
	o.Close()
	o.Close() // close multiple times.

	// CASE 2: start again without any previous kv.
	c.Assert(o.Start(ctx, etcdTestCli), IsNil)
	c.Assert(o.tk.FindTables(task), IsNil)

	// PUT st1, should find tables.
	_, err := optimism.PutSourceTables(etcdTestCli, st1)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		sts := o.tk.FindTables(task)
		return len(sts) == 1
	}), IsTrue)
	sts := o.tk.FindTables(task)
	c.Assert(sts, HasLen, 1)
	c.Assert(sts[0], DeepEquals, st1)
	o.Close()

	// CASE 3: start again with previous source tables.
	c.Assert(o.Start(ctx, etcdTestCli), IsNil)
	sts = o.tk.FindTables(task)
	c.Assert(sts, HasLen, 1)
	c.Assert(sts[0], DeepEquals, st1)

	// PUT st2, should find more tables.
	_, err = optimism.PutSourceTables(etcdTestCli, st2)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		sts = o.tk.FindTables(task)
		return len(sts) == 2
	}), IsTrue)
	sts = o.tk.FindTables(task)
	c.Assert(sts, HasLen, 2)
	c.Assert(sts[0], DeepEquals, st1)
	c.Assert(sts[1], DeepEquals, st2)
	o.Close()

	// CASE 4: create (not re-start) a new optimist with previous source tables.
	o = NewOptimist(&logger)
	c.Assert(o.Start(ctx, etcdTestCli), IsNil)
	sts = o.tk.FindTables(task)
	c.Assert(sts, HasLen, 2)
	c.Assert(sts[0], DeepEquals, st1)
	c.Assert(sts[1], DeepEquals, st2)

	// DELETE st1, should find less tables.
	_, err = optimism.DeleteSourceTables(etcdTestCli, st1)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		sts = o.tk.FindTables(task)
		return len(sts) == 1
	}), IsTrue)
	sts = o.tk.FindTables(task)
	c.Assert(sts, HasLen, 1)
	c.Assert(sts[0], DeepEquals, st2)
	o.Close()
}
