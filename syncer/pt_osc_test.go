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

	//"github.com/pingcap/dm/dm/config"
)

var _ = Suite(&testPTSuite{})

type testPTSuite struct {}

func (t *testPTSuite) TestPT (c *C) {
	/*
	cfg := &config.SubTaskConfig {

	}
	pt, err := NewPT(cfg)
	c.Assert(err, IsNil)
	
	_, _, _, err = pt.Apply(nil, "create table test(id int)", nil)
	c.Assert(err, IsNil)
	*/
}