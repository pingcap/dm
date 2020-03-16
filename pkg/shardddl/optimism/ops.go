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

	"github.com/pingcap/dm/pkg/etcdutil"
)

// DeleteInfosOperations deletes the shard DDL infos and operations in etcd.
// This function should often be called by DM-master when removing the lock.
func DeleteInfosOperations(cli *clientv3.Client, infos []Info, ops []Operation) (int64, error) {
	opsDel := make([]clientv3.Op, 0, len(infos)+len(ops))
	for _, info := range infos {
		opsDel = append(opsDel, deleteInfoOp(info))
	}
	for _, op := range ops {
		opsDel = append(opsDel, deleteOperationOp(op))
	}
	_, rev, err := etcdutil.DoOpsInOneTxn(cli, opsDel...)
	return rev, err
}
