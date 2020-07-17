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

package ha

import (
	. "github.com/pingcap/check"

	"github.com/pingcap/dm/dm/config"
)

const (
	// do not forget to update this path if the file removed/renamed.
	taskSampleFile = "../../dm/master/task_basic.yaml"
)

func (t *testForEtcd) TestTaskEtcd(c *C) {
	defer clearTestInfoOperation(c)

	cfg1 := config.TaskConfig{}
	c.Assert(cfg1.DecodeFile(taskSampleFile), IsNil)
	taskName1 := cfg1.Name

	taskName2 := taskName1 + "2"
	cfg2 := cfg1
	cfg2.Name = taskName2

	for _, mysqlInstance := range cfg1.MySQLInstances {
		mysqlInstance.RemoveDuplicateCfg()
	}
	for _, mysqlInstance := range cfg2.MySQLInstances {
		mysqlInstance.RemoveDuplicateCfg()
	}
	cfgStr1 := cfg1.String()
	cfgStr2 := cfg2.String()

	// no task config exist.
	tcm1, rev1, err := GetTaskCfg(etcdTestCli, taskName1)
	c.Assert(err, IsNil)
	c.Assert(rev1, Greater, int64(0))
	c.Assert(tcm1, HasLen, 0)

	// put task config 1
	rev2, err := PutTaskCfg(etcdTestCli, taskName1, cfgStr1)
	c.Assert(err, IsNil)
	c.Assert(rev2, Greater, rev1)

	// put task config 2
	rev3, err := PutTaskCfg(etcdTestCli, taskName2, cfgStr2)
	c.Assert(err, IsNil)
	c.Assert(rev3, Greater, rev2)

	// get single config back.
	tsm2, rev4, err := GetTaskCfg(etcdTestCli, taskName1)
	c.Assert(err, IsNil)
	c.Assert(rev4, Equals, rev3)
	c.Assert(tsm2, HasLen, 1)
	c.Assert(tsm2, HasKey, taskName1)
	c.Assert(tsm2[taskName1], Equals, cfgStr1)

	tsm3, rev5, err := GetTaskCfg(etcdTestCli, "")
	c.Assert(err, IsNil)
	c.Assert(rev5, Equals, rev4)
	c.Assert(tsm3, HasLen, 2)
	c.Assert(tsm3, HasKey, taskName1)
	c.Assert(tsm3, HasKey, taskName2)
	c.Assert(tsm3[taskName1], DeepEquals, cfgStr1)
	c.Assert(tsm3[taskName2], DeepEquals, cfgStr2)

	// delete the config.
	rev6, err := DeleteTaskCfg(etcdTestCli, taskName1)
	c.Assert(err, IsNil)
	c.Assert(rev6, Greater, rev5)

	// get again, not exists now.
	tsm4, rev7, err := GetTaskCfg(etcdTestCli, taskName1)
	c.Assert(err, IsNil)
	c.Assert(tsm4, HasLen, 0)
	c.Assert(rev7, Equals, rev6)

	// put task config.
	rev8, err := PutTaskCfg(etcdTestCli, taskName1, cfgStr1)
	c.Assert(err, IsNil)
	c.Assert(rev8, Greater, rev7)

	// update task config.
	cfg3 := cfg1
	cfg3.TaskMode = "full"
	for _, mysqlInstance := range cfg2.MySQLInstances {
		mysqlInstance.RemoveDuplicateCfg()
	}
	cfgStr3 := cfg3.String()
	rev9, err := PutTaskCfg(etcdTestCli, cfg3.Name, cfgStr3)
	c.Assert(err, IsNil)
	c.Assert(rev9, Greater, rev8)

	// get updated task
	tsm5, rev10, err := GetTaskCfg(etcdTestCli, taskName1)
	c.Assert(err, IsNil)
	c.Assert(rev10, Equals, rev9)
	c.Assert(tsm5, HasLen, 1)
	c.Assert(tsm5, HasKey, taskName1)
	c.Assert(tsm5[taskName1], DeepEquals, cfgStr3)
}
