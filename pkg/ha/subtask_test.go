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
	"context"

	. "github.com/pingcap/check"

	"github.com/pingcap/dm/dm/config"
)

const (
	// do not forget to update this path if the file removed/renamed.
	subTaskSampleFile = "../../dm/worker/subtask.toml"
)

func (t *testForEtcd) TestSubTaskEtcd(c *C) {
	defer clearTestInfoOperation(c)

	cfg1 := config.SubTaskConfig{}
	c.Assert(cfg1.DecodeFile(subTaskSampleFile), IsNil)
	source := cfg1.SourceID
	taskName1 := cfg1.Name

	taskName2 := taskName1 + "2"
	cfg2 := cfg1
	cfg2.Name = taskName2
	// to support loader config adjust
	cfg2.LoaderConfig.Dir += "." + cfg2.Name

	// no subtask config exist.
	tsm1, rev1, err := GetSubTaskCfg(etcdTestCli, source, taskName1)
	c.Assert(err, IsNil)
	c.Assert(rev1, Equals, int64(0))
	c.Assert(tsm1, HasLen, 0)

	// put a subtask config.
	rev2, err := PutSubTaskCfg(etcdTestCli, cfg1)
	c.Assert(err, IsNil)
	c.Assert(rev2, Greater, rev1)

	// put another subtask config.
	rev3, err := PutSubTaskCfg(etcdTestCli, cfg2)
	c.Assert(err, IsNil)
	c.Assert(rev3, Greater, rev2)

	// get single config back.
	tsm2, rev4, err := GetSubTaskCfg(etcdTestCli, source, taskName1)
	c.Assert(err, IsNil)
	c.Assert(rev4, Equals, rev3)
	c.Assert(tsm2, HasLen, 1)
	c.Assert(tsm2, HasKey, taskName1)
	c.Assert(tsm2[taskName1], DeepEquals, cfg1)

	tsm3, rev5, err := GetSubTaskCfg(etcdTestCli, source, "")
	c.Assert(err, IsNil)
	c.Assert(rev5, Equals, rev4)
	c.Assert(tsm3, HasLen, 2)
	c.Assert(tsm3, HasKey, taskName1)
	c.Assert(tsm3, HasKey, taskName2)
	c.Assert(tsm3[taskName1], DeepEquals, cfg1)
	c.Assert(tsm3[taskName2], DeepEquals, cfg2)

	// delete the config.
	deleteOp := deleteSubTaskCfgOp(source, taskName1)
	_, err = etcdTestCli.Txn(context.Background()).Then(deleteOp).Commit()
	c.Assert(err, IsNil)
	deleteOp = deleteSubTaskCfgOp(source, taskName2)
	_, err = etcdTestCli.Txn(context.Background()).Then(deleteOp).Commit()
	c.Assert(err, IsNil)

	// get again, not exists now.
	tsm4, rev6, err := GetSubTaskCfg(etcdTestCli, source, taskName1)
	c.Assert(err, IsNil)
	c.Assert(rev6, Equals, int64(0))
	c.Assert(tsm4, HasLen, 0)
}
