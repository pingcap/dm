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

package optimism

import (
	"go.etcd.io/etcd/clientv3"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/pkg/etcdutil"
)

// PutSourceTablesInfo puts source tables and a shard DDL info.
// This function is often used in DM-worker when handling `CREATE TABLE`.
func PutSourceTablesInfo(cli *clientv3.Client, st SourceTables, info Info) (int64, error) {
	stOp, err := putSourceTablesOp(st)
	if err != nil {
		return 0, err
	}
	infoOp, err := putInfoOp(info)
	if err != nil {
		return 0, err
	}
	_, rev, err := etcdutil.DoOpsInOneTxnWithRetry(cli, stOp, infoOp)
	return rev, err
}

// PutSourceTablesDeleteInfo puts source tables and deletes a shard DDL info.
// This function is often used in DM-worker when handling `DROP TABLE`.
func PutSourceTablesDeleteInfo(cli *clientv3.Client, st SourceTables, info Info) (int64, error) {
	stOp, err := putSourceTablesOp(st)
	if err != nil {
		return 0, err
	}
	infoOp := deleteInfoOp(info)
	_, rev, err := etcdutil.DoOpsInOneTxnWithRetry(cli, stOp, infoOp)
	return rev, err
}

// DeleteInfosOperationsSchema deletes the shard DDL infos, operations and init schemas in etcd.
// This function should often be called by DM-master when removing the lock.
func DeleteInfosOperationsSchema(cli *clientv3.Client, infos []Info, ops []Operation, schema InitSchema) (int64, error) {
	opsDel := make([]clientv3.Op, 0, len(infos)+len(ops))
	for _, info := range infos {
		opsDel = append(opsDel, deleteInfoOp(info))
	}
	for _, op := range ops {
		opsDel = append(opsDel, deleteOperationOp(op))
	}
	opsDel = append(opsDel, deleteInitSchemaOp(schema.Task, schema.DownSchema, schema.DownTable))
	_, rev, err := etcdutil.DoOpsInOneTxnWithRetry(cli, opsDel...)
	return rev, err
}

// DeleteInfosOperationsTablesSchemasByTask deletes the shard DDL infos and operations in etcd.
func DeleteInfosOperationsTablesSchemasByTask(cli *clientv3.Client, task string) (int64, error) {
	opsDel := make([]clientv3.Op, 0, 3)
	opsDel = append(opsDel, clientv3.OpDelete(common.ShardDDLOptimismInfoKeyAdapter.Encode(task), clientv3.WithPrefix()))
	opsDel = append(opsDel, clientv3.OpDelete(common.ShardDDLOptimismOperationKeyAdapter.Encode(task), clientv3.WithPrefix()))
	opsDel = append(opsDel, clientv3.OpDelete(common.ShardDDLOptimismSourceTablesKeyAdapter.Encode(task), clientv3.WithPrefix()))
	opsDel = append(opsDel, clientv3.OpDelete(common.ShardDDLOptimismInitSchemaKeyAdapter.Encode(task), clientv3.WithPrefix()))
	_, rev, err := etcdutil.DoOpsInOneTxnWithRetry(cli, opsDel...)
	return rev, err
}
