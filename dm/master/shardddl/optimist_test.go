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

	. "github.com/pingcap/check"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/shardddl/optimism"
)

type testOptimist struct{}

var _ = Suite(&testOptimist{})

// clear keys in etcd test cluster.
func clearOptimistTestSourceInfoOperation(c *C) {
	c.Assert(optimism.ClearTestInfoOperation(etcdTestCli), IsNil)
}

func (t *testOptimist) TestOptimist(c *C) {
	defer clearOptimistTestSourceInfoOperation(c)

	var (
		logger = log.L()
		o      = NewOptimist(&logger)
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// CASE 1: start without any previous kv and no etcd operation.
	c.Assert(o.Start(ctx, etcdTestCli), IsNil)
	c.Assert(o.Locks(), HasLen, 0)
	o.Close()
	o.Close() // close multiple times.
}
