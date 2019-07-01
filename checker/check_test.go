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

package checker

import (
	"context"
	"testing"

	"github.com/pingcap/dm/dm/config"

	tc "github.com/pingcap/check"
)

func TestChecker(t *testing.T) {
	tc.TestingT(t)
}

type testChecker struct{}

var _ = tc.Suite(&testChecker{})

func (t *testChecker) TestCheckSyncConfig(c *tc.C) {
	c.Assert(CheckSyncConfig(context.Background(), nil), tc.IsNil)

	cfgs := []*config.SubTaskConfig{
		{
			IgnoreCheckingItems: []string{config.AllChecking},
		},
	}
	c.Assert(CheckSyncConfig(context.Background(), cfgs), tc.IsNil)
}
