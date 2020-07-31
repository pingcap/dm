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

	"go.etcd.io/etcd/clientv3"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/pkg/etcdutil"
	"github.com/pingcap/dm/pkg/terror"
)

func taskCfgFromResp(task string, resp *clientv3.GetResponse) (map[string]string, error) {
	tcm := make(map[string]string)
	if resp.Count == 0 {
		return tcm, nil
	} else if task != "" && resp.Count > 1 {
		// this should not happen.
		return tcm, terror.ErrConfigMoreThanOne.Generate(resp.Count, "task", "task name: "+task)
	}

	for _, kvs := range resp.Kvs {
		keys, err := common.TaskConfigKeyAdapter.Decode(string(kvs.Key))
		if err != nil {
			return tcm, err
		}
		tcm[keys[0]] = string(kvs.Value)
	}
	return tcm, nil
}

// PutTaskCfg puts the config string of task into etcd.
// k/v: task name -> task config string.
func PutTaskCfg(cli *clientv3.Client, task string, cfg string) (int64, error) {
	key := common.TaskConfigKeyAdapter.Encode(task)
	_, rev, err := etcdutil.DoOpsInOneTxnWithRetry(cli, clientv3.OpPut(key, cfg))
	return rev, err
}

// GetTaskCfg gets the config string of task.
// k/v: task name -> task config string.
// if the task name is "", it will return all task configs as a map{task-name: task-config-string}.
// if the task name is given, it will return a map{task-name: task-config-string} whose length is 1.
func GetTaskCfg(cli *clientv3.Client, task string) (map[string]string, int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	var (
		tcm  = make(map[string]string)
		resp *clientv3.GetResponse
		err  error
	)
	if task != "" {
		resp, err = cli.Get(ctx, common.TaskConfigKeyAdapter.Encode(task))
	} else {
		resp, err = cli.Get(ctx, common.TaskConfigKeyAdapter.Path(), clientv3.WithPrefix())
	}

	if err != nil {
		return tcm, 0, err
	}

	tcm, err = taskCfgFromResp(task, resp)
	if err != nil {
		return tcm, 0, err
	}

	return tcm, resp.Header.Revision, nil
}

// DeleteTaskCfg deletes the config of task.
func DeleteTaskCfg(cli *clientv3.Client, task string) (int64, error) {
	_, rev, err := etcdutil.DoOpsInOneTxnWithRetry(cli, clientv3.OpDelete(common.TaskConfigKeyAdapter.Encode(task)))
	return rev, err
}
