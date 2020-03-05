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

package pessimism

import (
	"context"

	"go.etcd.io/etcd/clientv3"

	"github.com/pingcap/dm/pkg/etcdutil"
)

// TODO(csuzhangxc): assign terror code before merged into the master branch.

// PutOperationDeleteExistInfo puts an operation and deletes an info in one txn,
// if the info exists in etcd before.
// This function should often be called by DM-worker.
func PutOperationDeleteExistInfo(cli *clientv3.Client, op Operation, info Info) (done bool, rev int64, err error) {
	putOp, err := putOperationOp(op)
	if err != nil {
		return false, 0, nil
	}
	delOp := deleteInfoOp(info)

	infoCmp := infoExistCmp(info)

	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Txn(ctx).If(infoCmp).Then(putOp, delOp).Commit()
	if err != nil {
		return false, 0, err
	}
	return resp.Succeeded, resp.Header.Revision, nil
}
