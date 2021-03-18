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
	"go.etcd.io/etcd/clientv3"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/pkg/etcdutil"
)

// GetAllDroppedColumns gets the all dropped columns.
func GetAllDroppedColumns(cli *clientv3.Client) (map[string]map[string]map[string]map[string]map[string]interface{}, int64, error) {
	colm := make(map[string]map[string]map[string]map[string]map[string]interface{})
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
			task := keys[0]
			downSchema := keys[1]
			downTable := keys[2]
			column := keys[3]
			source := keys[4]
			upSchema := keys[5]
			upTable := keys[6]
			info := NewInfo(task, source, upSchema, upTable, downSchema, downTable, nil, nil, nil)
			lockID := genDDLLockID(info)
			if _, ok := colm[lockID]; !ok {
				colm[lockID] = make(map[string]map[string]map[string]map[string]interface{})
			}
			if _, ok := colm[lockID][column]; !ok {
				colm[lockID][column] = make(map[string]map[string]map[string]interface{})
			}
			if _, ok := colm[lockID][column][source]; !ok {
				colm[lockID][column][source] = make(map[string]map[string]interface{})
			}
			if _, ok := colm[lockID][column][source][upSchema]; !ok {
				colm[lockID][column][source][upSchema] = make(map[string]interface{})
			}
			colm[lockID][column][source][upSchema][upTable] = struct{}{}
		}
	}
	return colm, rev, nil
}

// PutDroppedColumn puts the undropped column name into ectd.
// When we drop a column, we save this column's name in etcd.
func PutDroppedColumn(cli *clientv3.Client, info Info, column string) (rev int64, putted bool, err error) {
	key := common.ShardDDLOptimismDroppedColumnsKeyAdapter.Encode(
		info.Task, info.DownSchema, info.DownTable, column, info.Source, info.UpSchema, info.UpTable)

	op := clientv3.OpPut(key, "")

	resp, rev, err := etcdutil.DoOpsInOneTxnWithRetry(cli, op)
	if err != nil {
		return 0, false, err
	}
	return rev, resp.Succeeded, nil
}

// DeleteDroppedColumns tries to delete the dropped columns for the specified lock ID.
// Only when this column is fully dropped in downstream database,
// in other words, **we receive a `Done` DDL group from dm-worker)**,
// we can delete this column's name from the etcd.
func DeleteDroppedColumns(cli *clientv3.Client, task, downSchema, downTable string, columns ...string) (rev int64, deleted bool, err error) {
	ops := make([]clientv3.Op, 0, len(columns))
	for _, col := range columns {
		ops = append(ops, deleteDroppedColumnByColumnOp(task, downSchema, downTable, col))
	}
	resp, rev, err := etcdutil.DoOpsInOneTxnWithRetry(cli, ops...)
	if err != nil {
		return 0, false, err
	}
	return rev, resp.Succeeded, nil
}

// deleteDroppedColumnOp returns a DELETE etcd operation for the specified task and column name.
func deleteDroppedColumnByColumnOp(task, downSchema, downTable, column string) clientv3.Op {
	return clientv3.OpDelete(common.ShardDDLOptimismDroppedColumnsKeyAdapter.Encode(task, downSchema, downTable, column), clientv3.WithPrefix())
}

// deleteDroppedColumnsByLockOp returns a DELETE etcd operation for the specified task.
func deleteDroppedColumnsByLockOp(task, downSchema, downTable string) clientv3.Op {
	return clientv3.OpDelete(common.ShardDDLOptimismDroppedColumnsKeyAdapter.Encode(task), clientv3.WithPrefix())
}
