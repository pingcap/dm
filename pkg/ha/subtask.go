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
	"fmt"

	"go.etcd.io/etcd/clientv3"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/etcdutil"
)

// PutSubTaskCfg puts the subtask configs of the specified source and task name into etcd.
// k/k/v: sourceID, taskName -> subtask config.
func PutSubTaskCfg(cli *clientv3.Client, cfgs ...config.SubTaskConfig) (int64, error) {
	ops, err := putSubTaskCfgOp(cfgs...)
	if err != nil {
		return 0, err
	}

	return etcdutil.DoOpsInOneTxn(cli, ops...)
}

// GetSubTaskCfg gets the subtask config of the specified source and task name.
// if the config for the source not exist, return with `err == nil` and `revision=0`.
// if taskName is "", will return all the subtaskConfigs as a map{taskName: subtaskConfig} of the source
// if taskName if given, will return a map{taskName: subtaskConfig} whose length is 1
func GetSubTaskCfg(cli *clientv3.Client, source, taskName string, rev int64) (map[string]config.SubTaskConfig, int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	tsm := make(map[string]config.SubTaskConfig)
	var (
		resp *clientv3.GetResponse
		err  error
	)
	if taskName != "" {
		resp, err = cli.Get(ctx, common.UpstreamSubTaskKeyAdapter.Encode(source, taskName), clientv3.WithRev(rev))
	} else {
		resp, err = cli.Get(ctx, common.UpstreamSubTaskKeyAdapter.Encode(source), clientv3.WithPrefix(), clientv3.WithRev(rev))
	}

	if err != nil {
		return tsm, 0, err
	}

	if resp.Count == 0 {
		return tsm, 0, nil
	} else if taskName != "" && resp.Count > 1 {
		// TODO(lichunzhu): add terror.
		// this should not happen.
		return tsm, 0, fmt.Errorf("too many config (%d) exist for the subtask {sourceID: %s, task name: %s}", resp.Count, source, taskName)
	}

	for _, kvs := range resp.Kvs {
		cfg := config.SubTaskConfig{}
		err = cfg.Decode(string(kvs.Value), true)
		if err != nil {
			return tsm, 0, err
		}

		tsm[cfg.Name] = cfg
	}

	return tsm, resp.Header.Revision, nil
}

// GetAllSubTaskCfg gets all subtask configs.
// k/v: source ID -> task name -> subtask config
func GetAllSubTaskCfg(cli *clientv3.Client) (map[string]map[string]config.SubTaskConfig, int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Get(ctx, common.UpstreamSubTaskKeyAdapter.Path(), clientv3.WithPrefix())

	if err != nil {
		return nil, 0, err
	}

	cfgs := make(map[string]map[string]config.SubTaskConfig)
	for _, kvs := range resp.Kvs {
		cfg := config.SubTaskConfig{}
		err = cfg.Decode(string(kvs.Value), true)
		if err != nil {
			return nil, 0, err
		}
		if _, ok := cfgs[cfg.SourceID]; !ok {
			cfgs[cfg.SourceID] = make(map[string]config.SubTaskConfig)
		}
		cfgs[cfg.SourceID][cfg.Name] = cfg
	}

	return cfgs, resp.Header.Revision, nil
}

// putSubTaskCfgOp returns a PUT etcd operation for the subtask config.
func putSubTaskCfgOp(cfgs ...config.SubTaskConfig) ([]clientv3.Op, error) {
	ops := make([]clientv3.Op, 0, len(cfgs))
	for _, cfg := range cfgs {
		value, err := cfg.Toml()
		if err != nil {
			return ops, err
		}
		key := common.UpstreamSubTaskKeyAdapter.Encode(cfg.SourceID, cfg.Name)
		ops = append(ops, clientv3.OpPut(key, value))
	}
	return ops, nil
}

// deleteSubTaskCfgOp returns a DELETE etcd operation for the subtask config.
func deleteSubTaskCfgOp(cfgs ...config.SubTaskConfig) []clientv3.Op {
	ops := make([]clientv3.Op, 0, len(cfgs))
	for _, cfg := range cfgs {
		ops = append(ops, clientv3.OpDelete(common.UpstreamSubTaskKeyAdapter.Encode(cfg.SourceID, cfg.Name)))
	}
	return ops
}
