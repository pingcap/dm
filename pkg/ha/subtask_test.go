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
	"go.etcd.io/etcd/clientv3"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/dm/config"
)

const (
	// do not forget to update this path if the file removed/renamed.
	subTaskSampleFile = "../../dm/worker/subtask.toml"
)

// clear keys in etcd test cluster.
func clearSubTaskTestInfoOperation(c *C) {
	clearSubTask := clientv3.OpDelete(common.UpstreamSubTaskKeyAdapter.Path(), clientv3.WithPrefix())
	_, err := etcdTestCli.Txn(context.Background()).Then(clearSubTask).Commit()
	c.Assert(err, IsNil)
}

func (t *testForEtcd) TestSubTaskEtcd(c *C) {
	defer clearSubTaskTestInfoOperation(c)

	var (
		emptyCfg = config.SubTaskConfig{}
		cfg      = config.SubTaskConfig{}
	)
	c.Assert(cfg.DecodeFile(subTaskSampleFile), IsNil)
	source := cfg.SourceID
	taskName := cfg.Name

	// no subtask config exist.
	cfg1, rev1, err := GetSubTaskCfg(etcdTestCli, source, taskName)
	c.Assert(err, IsNil)
	c.Assert(rev1, Equals, int64(0))
	c.Assert(cfg1, DeepEquals, emptyCfg)

	// put a subtask config.
	rev2, err := PutSubTaskCfg(etcdTestCli, cfg)
	c.Assert(err, IsNil)
	c.Assert(rev2, Greater, rev1)

	// get the config back.
	cfg2, rev3, err := GetSubTaskCfg(etcdTestCli, source, taskName)
	c.Assert(err, IsNil)
	c.Assert(rev3, Equals, rev2)
	c.Assert(cfg2, DeepEquals, cfg)

	// delete the config.
	deleteOp := deleteSubTaskCfgOp(source, taskName)
	_, err = etcdTestCli.Txn(context.Background()).Then(deleteOp).Commit()
	c.Assert(err, IsNil)

	// get again, not exists now.
	cfg3, rev4, err := GetSubTaskCfg(etcdTestCli, source, taskName)
	c.Assert(err, IsNil)
	c.Assert(rev4, Equals, int64(0))
	c.Assert(cfg3, DeepEquals, emptyCfg)
}
