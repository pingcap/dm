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

package optimism

import (
	"encoding/json"

	"go.etcd.io/etcd/clientv3"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/pkg/etcdutil"
)

// GetAllDroppedColumns gets the all partially dropped columns.
// return lockID -> column-name -> source-id -> upstream-schema-name -> upstream-table-name.
func GetAllDroppedColumns(cli *clientv3.Client) (map[string]map[string]map[string]map[string]map[string]DropColumnStage, int64, error) {
	var done DropColumnStage
	colm := make(map[string]map[string]map[string]map[string]map[string]DropColumnStage)
	op := clientv3.OpGet(common.ShardDDLOptimismDroppedColumnsKeyAdapter.Path(), clientv3.WithPrefix())
	respTxn, rev, err := etcdutil.DoOpsInOneTxnWithRetry(cli, op)
	if err != nil {
		return colm, 0, err
	}
	resp := respTxn.Responses[0].GetResponseRange()

	if resp.Count > 0 {
		for _, kv := range resp.Kvs {
			keys, err := common.ShardDDLOptimismDroppedColumnsKeyAdapter.Decode(string(kv.Key))
			if err != nil {
				return colm, 0, err
			}
			err = json.Unmarshal(kv.Value, &done)
			if err != nil {
				return colm, 0, err
			}
			lockID := keys[0]
			column := keys[1]
			source := keys[2]
			upSchema := keys[3]
			upTable := keys[4]
			if _, ok := colm[lockID]; !ok {
				colm[lockID] = make(map[string]map[string]map[string]map[string]DropColumnStage)
			}
			if _, ok := colm[lockID][column]; !ok {
				colm[lockID][column] = make(map[string]map[string]map[string]DropColumnStage)
			}
			if _, ok := colm[lockID][column][source]; !ok {
				colm[lockID][column][source] = make(map[string]map[string]DropColumnStage)
			}
			if _, ok := colm[lockID][column][source][upSchema]; !ok {
				colm[lockID][column][source][upSchema] = make(map[string]DropColumnStage)
			}
			colm[lockID][column][source][upSchema][upTable] = done
		}
	}
	return colm, rev, nil
}

// PutDroppedColumn puts the partially dropped column name into ectd.
// When we drop a column, we save this column's name in etcd.
func PutDroppedColumn(cli *clientv3.Client, lockID, column, source, upSchema, upTable string, done DropColumnStage) (rev int64, putted bool, err error) {
	key := common.ShardDDLOptimismDroppedColumnsKeyAdapter.Encode(lockID, column, source, upSchema, upTable)
	val, err := json.Marshal(done)
	if err != nil {
		return 0, false, err
	}
	op := clientv3.OpPut(key, string(val))

	resp, rev, err := etcdutil.DoOpsInOneTxnWithRetry(cli, op)
	if err != nil {
		return 0, false, err
	}
	return rev, resp.Succeeded, nil
}

// DeleteDroppedColumns tries to delete the partially dropped columns for the specified lock ID.
// Only when this column is fully dropped in downstream database,
// in other words, **we receive all `Done` operation from dm-worker**,
// we can delete this column's name from the etcd.
func DeleteDroppedColumns(cli *clientv3.Client, lockID string, columns ...string) (rev int64, deleted bool, err error) {
	ops := make([]clientv3.Op, 0, len(columns))
	for _, col := range columns {
		ops = append(ops, deleteDroppedColumnByColumnOp(lockID, col))
	}
	resp, rev, err := etcdutil.DoOpsInOneTxnWithRetry(cli, ops...)
	if err != nil {
		return 0, false, err
	}
	return rev, resp.Succeeded, nil
}

// deleteDroppedColumnOp returns a DELETE etcd operation for the specified task and column name.
func deleteDroppedColumnByColumnOp(lockID, column string) clientv3.Op {
	return clientv3.OpDelete(common.ShardDDLOptimismDroppedColumnsKeyAdapter.Encode(lockID, column), clientv3.WithPrefix())
}

// deleteDroppedColumnsByLockOp returns a DELETE etcd operation for the specified lock.
func deleteDroppedColumnsByLockOp(lockID string) clientv3.Op {
	return clientv3.OpDelete(common.ShardDDLOptimismDroppedColumnsKeyAdapter.Encode(lockID), clientv3.WithPrefix())
}
