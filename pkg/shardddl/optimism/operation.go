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
	"context"
	"encoding/json"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/clientv3util"
	"go.etcd.io/etcd/mvcc/mvccpb"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/pkg/etcdutil"
)

// ConflictStage represents the current shard DDL conflict stage in the optimistic mode.
type ConflictStage string

const (
	// ConflictNone indicates no conflict exists,
	// DM-worker can execute DDL/DML to the downstream normally.
	ConflictNone ConflictStage = "none"
	// ConflictDetected indicates a conflict will exist after applied the shard DDL.
	// in this stage, DM-worker should not execute/skip DDL/DML,
	// but it should still try to find the DDL which can resolve the conflict in the binlog stream.
	ConflictDetected ConflictStage = "detected"
	// ConflictResolved indicates a conflict will be resolved after applied the shard DDL.
	// in this stage, DM-worker should replay DML skipped in ConflictDetected to downstream.
	ConflictResolved ConflictStage = "resolved"
)

// Operation represents a shard DDL coordinate operation.
// This information should be persistent in etcd so can be retrieved after the DM-master leader restarted or changed.
// NOTE: `Task`, `Source`, `UpSchema` and `UpTable` are redundant in the etcd key path for convenient.
// Operation is putted when coordinating a shard DDL operation for DM-worker by DM-master,
// and is updated (with `done`) after DM-worker has done the operation by DM-worker,
// and is deleted when removing the lock by DM-master.
// because we need the newest stage in Operation to recover the lock when restarting DM-master.
type Operation struct {
	ID            string        `json:"id"`             // the corresponding DDL lock ID
	Task          string        `json:"task"`           // data migration task name
	Source        string        `json:"source"`         // upstream source ID
	UpSchema      string        `json:"up-schema"`      // upstream/source schema name, different sources can have the same schema name
	UpTable       string        `json:"up-table"`       // upstream/source table name, different sources can have the same table name
	DDLs          []string      `json:"ddls"`           // DDL statements need to apply to the downstream.
	ConflictStage ConflictStage `json:"conflict-stage"` // current conflict stage.
	Done          bool          `json:"done"`           // whether the operation has done
}

// NewOperation creates a new Operation instance.
func NewOperation(ID, task, source, upSchema, upTable string,
	DDLs []string, conflictStage ConflictStage, done bool) Operation {
	return Operation{
		ID:            ID,
		Task:          task,
		Source:        source,
		UpSchema:      upSchema,
		UpTable:       upTable,
		DDLs:          DDLs,
		ConflictStage: conflictStage,
		Done:          done,
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

// PutOperation puts the shard DDL operation into etcd.
func PutOperation(cli *clientv3.Client, skipDone bool, op Operation) (rev int64, putted bool, err error) {
	value, err := op.toJSON()
	if err != nil {
		return 0, false, err
	}
	key := common.ShardDDLOptimismOperationKeyAdapter.Encode(op.Task, op.Source, op.UpSchema, op.UpTable)
	opPut := clientv3.OpPut(key, value)

	cmpsNotExist := make([]clientv3.Cmp, 0, 1)
	cmpsNotDone := make([]clientv3.Cmp, 0, 1)
	if skipDone {
		opDone := op
		opDone.Done = true // set `done` to `true`.
		valueDone, err2 := opDone.toJSON()
		if err2 != nil {
			return 0, false, err2
		}
		cmpsNotExist = append(cmpsNotExist, clientv3util.KeyMissing(key))
		cmpsNotDone = append(cmpsNotDone, clientv3.Compare(clientv3.Value(key), "!=", valueDone))
	}

	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	// txn 1: try to PUT if the key "not exist".
	resp, err := cli.Txn(ctx).If(cmpsNotExist...).Then(opPut).Commit()
	if err != nil {
		return 0, false, err
	} else if resp.Succeeded {
		return resp.Header.Revision, resp.Succeeded, nil
	}

	// txn 2: try to PUT if the key "the `done`" field is not `true`.
	resp, err = cli.Txn(ctx).If(cmpsNotDone...).Then(opPut).Commit()
	if err != nil {
		return 0, false, err
	}
	return resp.Header.Revision, resp.Succeeded, nil
}

// GetAllOperations gets all shard DDL operation in etcd currently.
// This function should often be called by DM-master.
// k/k/k/k/v: task-name -> source-ID -> upstream-schema-name -> upstream-table-name -> shard DDL operation.
func GetAllOperations(cli *clientv3.Client) (map[string]map[string]map[string]map[string]Operation, int64, error) {
	respTxn, _, err := etcdutil.DoOpsInOneTxnWithRetry(cli, clientv3.OpGet(common.ShardDDLOptimismOperationKeyAdapter.Path(), clientv3.WithPrefix()))
	if err != nil {
		return nil, 0, err
	}
	resp := respTxn.Responses[0].GetResponseRange()

	opm := make(map[string]map[string]map[string]map[string]Operation)
	for _, kv := range resp.Kvs {
		op, err2 := operationFromJSON(string(kv.Value))
		if err2 != nil {
			return nil, 0, err2
		}

		if _, ok := opm[op.Task]; !ok {
			opm[op.Task] = make(map[string]map[string]map[string]Operation)
		}
		if _, ok := opm[op.Task][op.Source]; !ok {
			opm[op.Task][op.Source] = make(map[string]map[string]Operation)
		}
		if _, ok := opm[op.Task][op.Source][op.UpSchema]; !ok {
			opm[op.Task][op.Source][op.UpSchema] = make(map[string]Operation)
		}
		opm[op.Task][op.Source][op.UpSchema][op.UpTable] = op
	}

	return opm, resp.Header.Revision, nil
}

// GetInfosOperationsByTask gets all shard DDL info and operation in etcd currently.
// This function should often be called by DM-master.
func GetInfosOperationsByTask(cli *clientv3.Client, task string) ([]Info, []Operation, int64, error) {
	respTxn, _, err := etcdutil.DoOpsInOneTxnWithRetry(cli,
		clientv3.OpGet(common.ShardDDLOptimismInfoKeyAdapter.Encode(task), clientv3.WithPrefix()),
		clientv3.OpGet(common.ShardDDLOptimismOperationKeyAdapter.Encode(task), clientv3.WithPrefix()))

	if err != nil {
		return nil, nil, 0, err
	}
	infoResp := respTxn.Responses[0].GetResponseRange()
	opsResp := respTxn.Responses[1].GetResponseRange()
	var (
		infos = make([]Info, 0, len(infoResp.Kvs))
		ops   = make([]Operation, 0, len(opsResp.Kvs))
	)
	for _, kv := range infoResp.Kvs {
		info, err2 := infoFromJSON(string(kv.Value))
		if err2 != nil {
			return nil, nil, 0, err2
		}
		infos = append(infos, info)
	}
	for _, kv := range opsResp.Kvs {
		op, err2 := operationFromJSON(string(kv.Value))
		if err2 != nil {
			return nil, nil, 0, err2
		}
		ops = append(ops, op)
	}
	return infos, ops, respTxn.Header.Revision, nil
}

// WatchOperationPut watches PUT operations for DDL lock operation.
// If want to watch all operations matching, pass empty string for `task`, `source`, `upSchema` and `upTable`.
// This function can be called by DM-worker and DM-master.
func WatchOperationPut(ctx context.Context, cli *clientv3.Client,
	task, source, upSchema, upTable string, revision int64,
	outCh chan<- Operation, errCh chan<- error) {
	ch := cli.Watch(ctx, common.ShardDDLOptimismOperationKeyAdapter.Encode(task, source, upSchema, upTable),
		clientv3.WithPrefix(), clientv3.WithRev(revision))

	for {
		select {
		case <-ctx.Done():
			return
		case resp, ok := <-ch:
			if !ok {
				return
			}
			if resp.Canceled {
				select {
				case errCh <- resp.Err():
				case <-ctx.Done():
				}
				return
			}

			for _, ev := range resp.Events {
				if ev.Type != mvccpb.PUT {
					continue
				}

				op, err := operationFromJSON(string(ev.Kv.Value))
				if err != nil {
					select {
					case errCh <- err:
					case <-ctx.Done():
						return
					}
				} else {
					select {
					case outCh <- op:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}
}

// deleteOperationOp returns a DELETE etcd operation for Operation.
func deleteOperationOp(op Operation) clientv3.Op {
	return clientv3.OpDelete(common.ShardDDLOptimismOperationKeyAdapter.Encode(op.Task, op.Source, op.UpSchema, op.UpTable))
}
