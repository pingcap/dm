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

	"github.com/pingcap/failpoint"
	"go.etcd.io/etcd/clientv3"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/etcdutil"
	"github.com/pingcap/dm/pkg/terror"
)

// PutSourceCfg puts the config of the upstream source into etcd.
// k/v: sourceID -> source config.
func PutSourceCfg(cli *clientv3.Client, cfg config.SourceConfig) (int64, error) {
	value, err := cfg.Toml()
	if err != nil {
		return 0, err
	}
	key := common.UpstreamConfigKeyAdapter.Encode(cfg.SourceID)
	_, rev, err := etcdutil.DoOpsInOneTxnWithRetry(cli, clientv3.OpPut(key, value))
	return rev, err
}

// GetSourceCfg gets upstream source configs.
// k/v: source ID -> source config.
// if the source config for the sourceID not exist, return with `err == nil`.
// if the source name is "", it will return all source configs as a map{sourceID: config}.
// if the source name is given, it will return a map{sourceID: config} whose length is 1.
func GetSourceCfg(cli *clientv3.Client, source string, rev int64) (map[string]config.SourceConfig, int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	var (
		scm  = make(map[string]config.SourceConfig)
		resp *clientv3.GetResponse
		err  error
	)
	failpoint.Inject("FailToGetSourceCfg", func() {
		failpoint.Return(scm, 0, context.DeadlineExceeded)
	})
	if source != "" {
		resp, err = cli.Get(ctx, common.UpstreamConfigKeyAdapter.Encode(source), clientv3.WithRev(rev))
	} else {
		resp, err = cli.Get(ctx, common.UpstreamConfigKeyAdapter.Path(), clientv3.WithPrefix(), clientv3.WithRev(rev))
	}

	if err != nil {
		return scm, 0, err
	}

	scm, err = sourceCfgFromResp(source, resp)
	if err != nil {
		return scm, 0, err
	}

	return scm, resp.Header.Revision, nil
}

// deleteSourceCfgOp returns a DELETE etcd operation for the source config.
func deleteSourceCfgOp(source string) clientv3.Op {
	return clientv3.OpDelete(common.UpstreamConfigKeyAdapter.Encode(source))
}

func sourceCfgFromResp(source string, resp *clientv3.GetResponse) (map[string]config.SourceConfig, error) {
	scm := make(map[string]config.SourceConfig)
	if resp.Count == 0 {
		return scm, nil
	} else if source != "" && resp.Count > 1 {
		// this should not happen.
		return scm, terror.ErrConfigMoreThanOne.Generate(resp.Count, "config", "source: "+source)
	}

	for _, kv := range resp.Kvs {
		var cfg config.SourceConfig
		err := cfg.Parse(string(kv.Value))
		if err != nil {
			return scm, terror.ErrConfigEtcdParse.Delegate(err, kv.Key)
		}
		scm[cfg.SourceID] = cfg
	}
	return scm, nil
}

// ClearTestInfoOperation is used to clear all DM-HA relative etcd keys' information
// this function shouldn't be used in development environment.
func ClearTestInfoOperation(cli *clientv3.Client) error {
	clearSource := clientv3.OpDelete(common.UpstreamConfigKeyAdapter.Path(), clientv3.WithPrefix())
	clearTask := clientv3.OpDelete(common.TaskConfigKeyAdapter.Path(), clientv3.WithPrefix())
	clearSubTask := clientv3.OpDelete(common.UpstreamSubTaskKeyAdapter.Path(), clientv3.WithPrefix())
	clearWorkerInfo := clientv3.OpDelete(common.WorkerRegisterKeyAdapter.Path(), clientv3.WithPrefix())
	clearWorkerKeepAlive := clientv3.OpDelete(common.WorkerKeepAliveKeyAdapter.Path(), clientv3.WithPrefix())
	clearBound := clientv3.OpDelete(common.UpstreamBoundWorkerKeyAdapter.Path(), clientv3.WithPrefix())
	clearLastBound := clientv3.OpDelete(common.UpstreamLastBoundWorkerKeyAdapter.Path(), clientv3.WithPrefix())
	clearRelayStage := clientv3.OpDelete(common.StageRelayKeyAdapter.Path(), clientv3.WithPrefix())
	clearRelayConfig := clientv3.OpDelete(common.UpstreamRelayWorkerKeyAdapter.Path(), clientv3.WithPrefix())
	clearSubTaskStage := clientv3.OpDelete(common.StageSubTaskKeyAdapter.Path(), clientv3.WithPrefix())
	_, _, err := etcdutil.DoOpsInOneTxnWithRetry(cli, clearSource, clearTask, clearSubTask, clearWorkerInfo, clearBound,
		clearLastBound, clearWorkerKeepAlive, clearRelayStage, clearRelayConfig, clearSubTaskStage)
	return err
}
