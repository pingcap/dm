// Copyright 2021 PingCAP, Inc.
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

package pessimism

import (
	"context"
	"encoding/json"

	"go.etcd.io/etcd/clientv3"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/pkg/etcdutil"
)

// putLatestDoneDDLsOp returns a PUT etcd operation for latest done ddls.
// This operation should often be sent by DM-master.
func putLatestDoneDDLsOp(lockID string, ddls []string) (clientv3.Op, error) {
	data, err := json.Marshal(ddls)
	if err != nil {
		return clientv3.Op{}, err
	}
	key := common.ShardDDLPessimismDDLsKeyAdapter.Encode(lockID)

	return clientv3.OpPut(key, string(data)), nil
}

// PutLatestDoneDDLs puts the last done shard DDL ddls into etcd.
func PutLatestDoneDDLs(cli *clientv3.Client, lockID string, ddls []string) (int64, error) {
	data, err := json.Marshal(ddls)
	if err != nil {
		return 0, err
	}
	value := string(data)
	key := common.ShardDDLPessimismDDLsKeyAdapter.Encode(lockID)

	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Put(ctx, key, value)
	if err != nil {
		return 0, err
	}
	return resp.Header.Revision, nil
}

// GetAllLatestDoneDDLs gets all last done shard DDL ddls in etcd currently.
// k/v: lockID -> DDLs
// This function should often be called by DM-master.
func GetAllLatestDoneDDLs(cli *clientv3.Client) (map[string][]string, int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Get(ctx, common.ShardDDLPessimismDDLsKeyAdapter.Path(), clientv3.WithPrefix())
	if err != nil {
		return nil, 0, err
	}

	ddlsMap := make(map[string][]string, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var ddls []string
		if err2 := json.Unmarshal(kv.Value, &ddls); err2 != nil {
			return nil, 0, err2
		}
		keys, err2 := common.ShardDDLPessimismDDLsKeyAdapter.Decode(string(kv.Key))
		if err2 != nil {
			return nil, 0, err2
		}
		lockID := keys[0]

		ddlsMap[lockID] = ddls
	}

	return ddlsMap, resp.Header.Revision, nil
}
