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
func GetAllDroppedColumns(cli *clientv3.Client) (map[string]map[string]interface{}, int64, error) {
	colm := make(map[string]map[string]interface{})
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
			info := Info{Task: task, DownSchema: downSchema, DownTable: downTable}
			lockID := genDDLLockID(info)
			if _, ok := colm[lockID]; !ok {
				colm[lockID] = make(map[string]interface{})
			}
			colm[lockID][column] = struct{}{}
		}
	}
	return colm, rev, nil
}

// PutDroppedColumn puts the undropped column name into ectd.
func PutDroppedColumn(cli *clientv3.Client, task, downSchema, downTable, column string) (rev int64, putted bool, err error) {
	key := common.ShardDDLOptimismDroppedColumnsKeyAdapter.Encode(task, downSchema, downTable, column)

	op := clientv3.OpPut(key, "")

	resp, rev, err := etcdutil.DoOpsInOneTxnWithRetry(cli, op)
	if err != nil {
		return 0, false, err
	}
	return rev, resp.Succeeded, nil
}

// DeleteDroppedColumns tries to delete the dropped columns for the specified lock ID.
func DeleteDroppedColumns(cli *clientv3.Client, task, downSchema, downTable string, columns ...string) (rev int64, deleted bool, err error) {
	ops := make([]clientv3.Op, 0, len(columns))
	for _, col := range columns {
		ops = append(ops, deleteDroppedColumnOp(task, downSchema, downTable, col))
	}
	resp, rev, err := etcdutil.DoOpsInOneTxnWithRetry(cli, ops...)
	if err != nil {
		return 0, false, err
	}
	return rev, resp.Succeeded, nil
}

// deleteDroppedColumnOp returns a DELETE etcd operation for init schema.
func deleteDroppedColumnOp(task, downSchema, downTable, column string) clientv3.Op {
	return clientv3.OpDelete(common.ShardDDLOptimismDroppedColumnsKeyAdapter.Encode(task, downSchema, downTable, column))
}

// deleteDroppedColumnOp returns a DELETE etcd operation for init schema.
func deleteDroppedColumnsOp(task, downSchema, downTable string) clientv3.Op {
	return clientv3.OpDelete(common.ShardDDLOptimismDroppedColumnsKeyAdapter.Encode(task, downSchema, downTable), clientv3.WithPrefix())
}
