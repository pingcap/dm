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
func PutSourceCfg(cli *clientv3.Client, cfg config.MysqlConfig) (int64, error) {
	value, err := cfg.Toml()
	if err != nil {
		return 0, err
	}
	key := common.UpstreamConfigKeyAdapter.Encode(cfg.SourceID)

	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Put(ctx, key, value)
	if err != nil {
		return 0, err
	}
	return resp.Header.Revision, nil
}

// GetSourceCfg gets the config of the specified source.
// if the config for the source not exist, return with `err == nil` and `revision=0`.
func GetSourceCfg(cli *clientv3.Client, source string) (config.MysqlConfig, int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	cfg := config.MysqlConfig{}
	resp, err := cli.Get(ctx, common.UpstreamConfigKeyAdapter.Encode(source))
	if err != nil {
		return cfg, 0, err
	}

	if resp.Count == 0 {
		return cfg, 0, err
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

// deleteSourceCfgOp returns a DELETE etcd operation for the source config.
func deleteSourceCfgOp(source string) clientv3.Op {
	return clientv3.OpDelete(common.UpstreamConfigKeyAdapter.Encode(source))
}
