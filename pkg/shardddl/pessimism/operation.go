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
	"encoding/json"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/pkg/etcdutil"
	"github.com/pingcap/dm/pkg/log"
)

// Operation represents a shard DDL coordinate operation.
// This information should be persistent in etcd so can be retrieved after the DM-master leader restarted or changed.
// NOTE: `Task` and `Source` are redundant in the etcd key path for convenient.
type Operation struct {
	ID     string   `json:"id"`     // the corresponding DDL lock ID
	Task   string   `json:"task"`   // data migration task name
	Source string   `json:"source"` // upstream source ID
	DDLs   []string `json:"ddls"`   // DDL statements
	Exec   bool     `json:"exec"`   // execute or skip the DDL statements
	Done   bool     `json:"done"`   // whether the `Exec` operation has done
}

// NewOperation creates a new Operation instance.
func NewOperation(ID, task, source string, DDLs []string, exec, done bool) Operation {
	return Operation{
		ID:     ID,
		Task:   task,
		Source: source,
		DDLs:   DDLs,
		Exec:   exec,
		Done:   done,
	}
}

// String implements Stringer interface.
func (o Operation) String() string {
	s, _ := o.toJSON()
	return s
}

// toJSON returns the string of JSON represent.
func (o Operation) toJSON() (string, error) {
	data, err := json.Marshal(o)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// operationFromJSON constructs Operation from its JSON represent.
func operationFromJSON(s string) (o Operation, err error) {
	err = json.Unmarshal([]byte(s), &o)
	return
}

// PutOperations puts the shard DDL operations into etcd.
// if `skipDone` is `true`, and the `done` filed in any of these operations in etcd is also `true`,
// then this `PutOperations` skip to overwrite the k/v in etcd.
// This function should often be called by DM-master.
func PutOperations(cli *clientv3.Client, skipDone bool, ops ...Operation) (int64, bool, error) {
	cmps := make([]clientv3.Cmp, 0, len(ops))
	opsPut := make([]clientv3.Op, 0, len(ops))
	for _, op := range ops {
		value, err := op.toJSON()
		if err != nil {
			return 0, false, err
		}

		key := common.ShardDDLPessimismOperationKeyAdapter.Encode(op.Task, op.Source)
		opsPut = append(opsPut, clientv3.OpPut(key, value))

		if skipDone {
			opDone := op
			opDone.Done = true // set `done` to `true`.
			valueDone, err2 := opDone.toJSON()
			if err2 != nil {
				return 0, false, err2
			}
			cmps = append(cmps, clientv3.Compare(clientv3.Value(key), "!=", valueDone))
		}
	}

	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Txn(ctx).If(cmps...).Then(opsPut...).Commit()
	if err != nil {
		return 0, false, err
	}
	return resp.Header.Revision, resp.Succeeded, nil
}

// DeleteOperations deletes the shard DDL operations in etcd.
// This function should often be called by DM-master.
func DeleteOperations(cli *clientv3.Client, ops ...Operation) (int64, error) {
	opsDel := make([]clientv3.Op, 0, len(ops))
	for _, op := range ops {
		key := common.ShardDDLPessimismOperationKeyAdapter.Encode(op.Task, op.Source)
		opsDel = append(opsDel, clientv3.OpDelete(key))
	}

	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Txn(ctx).Then(opsDel...).Commit()
	if err != nil {
		return 0, err
	}
	return resp.Header.Revision, nil
}

// GetAllOperations gets all DDL lock operation in etcd currently.
// k/k/v: task-name -> source-ID -> lock operation.
// This function is often used for debugging or testing.
func GetAllOperations(cli *clientv3.Client) (map[string]map[string]Operation, int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Get(ctx, common.ShardDDLPessimismOperationKeyAdapter.Path(), clientv3.WithPrefix())
	if err != nil {
		return nil, 0, err
	}

	opm := make(map[string]map[string]Operation)
	for _, kv := range resp.Kvs {
		op, err2 := operationFromJSON(string(kv.Value))
		if err2 != nil {
			return nil, 0, err2
		}

		if _, ok := opm[op.Task]; !ok {
			opm[op.Task] = make(map[string]Operation)
		}
		opm[op.Task][op.Source] = op
	}

	return opm, resp.Header.Revision, nil
}

// WatchOperationPut watches PUT operations for DDL lock operation.
// If want to watch all operations, pass empty string for `task` and `source`.
// This function can be called by DM-worker and DM-master.
func WatchOperationPut(ctx context.Context, cli *clientv3.Client, task, source string, revision int64, outCh chan<- Operation) {
	ch := cli.Watch(ctx, common.ShardDDLPessimismOperationKeyAdapter.Encode(task, source),
		clientv3.WithPrefix(), clientv3.WithRev(revision))

	for {
		select {
		case <-ctx.Done():
			return
		case resp := <-ch:
			if resp.Canceled {
				return
			}

			for _, ev := range resp.Events {
				if ev.Type != mvccpb.PUT {
					continue
				}

				op, err := operationFromJSON(string(ev.Kv.Value))
				if err != nil {
					// this should not happen.
					log.L().Error("fail to construct shard DDL operation from json", zap.ByteString("json", ev.Kv.Value))
					continue
				}
				outCh <- op
			}
		}
	}
}

// putOperationOp returns a PUT etcd operation for Operation.
// This operation should often be sent by DM-worker.
func putOperationOp(o Operation) (clientv3.Op, error) {
	value, err := o.toJSON()
	if err != nil {
		return clientv3.Op{}, err
	}
	key := common.ShardDDLPessimismOperationKeyAdapter.Encode(o.Task, o.Source)

	return clientv3.OpPut(key, value), nil
}
