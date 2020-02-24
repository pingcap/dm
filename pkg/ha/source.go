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

// PutSourceCfg puts the config of the upstream source into etcd.
// k/v: sourceID -> source config.
func PutSourceCfg(cli *clientv3.Client, cfg config.SourceConfig) (int64, error) {
	value, err := cfg.Toml()
	if err != nil {
		return 0, err
	}
	key := common.UpstreamConfigKeyAdapter.Encode(cfg.SourceID)

	return etcdutil.DoOpsInOneTxn(cli, clientv3.OpPut(key, value))
}

// GetSourceCfg gets the config of the specified source.
// if the config for the source not exist, return with `err == nil` and `revision=0`.
func GetSourceCfg(cli *clientv3.Client, source string, rev int64) (config.SourceConfig, int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	var cfg config.SourceConfig
	resp, err := cli.Get(ctx, common.UpstreamConfigKeyAdapter.Encode(source), clientv3.WithRev(rev))
	if err != nil {
		return cfg, 0, err
	}

	if resp.Count == 0 {
		return cfg, 0, nil
	} else if resp.Count > 1 {
		// TODO(csuzhangxc): add terror.
		// this should not happen.
		return cfg, 0, fmt.Errorf("too many config (%d) exist for the source %s", resp.Count, source)
	}

	err = cfg.Parse(string(resp.Kvs[0].Value))
	if err != nil {
		return cfg, 0, err
	}

	return cfg, resp.Header.Revision, nil
}

// GetAllSourceCfg gets all upstream source configs.
// k/v: source ID -> source config.
func GetAllSourceCfg(cli *clientv3.Client) (map[string]config.SourceConfig, int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Get(ctx, common.UpstreamConfigKeyAdapter.Path(), clientv3.WithPrefix())
	if err != nil {
		return nil, 0, err
	}

	scm := make(map[string]config.SourceConfig)
	for _, kv := range resp.Kvs {
		var cfg config.SourceConfig
		err = cfg.Parse(string(kv.Value))
		if err != nil {
			// TODO(csuzhangxc): add terror and including `key`.
			return nil, 0, err
		}
		scm[cfg.SourceID] = cfg
	}

	return scm, resp.Header.Revision, nil
}

// deleteSourceCfgOp returns a DELETE etcd operation for the source config.
func deleteSourceCfgOp(source string) clientv3.Op {
	return clientv3.OpDelete(common.UpstreamConfigKeyAdapter.Encode(source))
}

// ClearTestInfoOperation is used to clear all DM-HA relative etcd keys' information
// this function shouldn't be used in development environment
func ClearTestInfoOperation(cli *clientv3.Client) error {
	clearSource := clientv3.OpDelete(common.UpstreamConfigKeyAdapter.Path(), clientv3.WithPrefix())
	clearSubTask := clientv3.OpDelete(common.UpstreamSubTaskKeyAdapter.Path(), clientv3.WithPrefix())
	clearWorkerInfo := clientv3.OpDelete(common.WorkerRegisterKeyAdapter.Path(), clientv3.WithPrefix())
	clearWorkerKeepAlive := clientv3.OpDelete(common.WorkerKeepAliveKeyAdapter.Path(), clientv3.WithPrefix())
	clearBound := clientv3.OpDelete(common.UpstreamBoundWorkerKeyAdapter.Path(), clientv3.WithPrefix())
	clearRelayStage := clientv3.OpDelete(common.StageRelayKeyAdapter.Path(), clientv3.WithPrefix())
	clearSubTaskStage := clientv3.OpDelete(common.StageSubTaskKeyAdapter.Path(), clientv3.WithPrefix())
	_, err := cli.Txn(context.Background()).Then(
		clearSource, clearSubTask, clearWorkerInfo, clearBound, clearWorkerKeepAlive, clearRelayStage, clearSubTaskStage,
	).Commit()
	return err
}
