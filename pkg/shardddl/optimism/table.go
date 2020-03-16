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
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/pkg/etcdutil"
	"github.com/pingcap/dm/pkg/log"
)

// SourceTables represents the upstream/sources tables for a data migration subtask.
// This information should be persistent in etcd so can be retrieved after the DM-master leader restarted or changed.
// We need this because only one shard group exists in the optimistic mode (in DM-master),
// so we need DM-worker to report its upstream table names to DM-master.
// NOTE: `Task` and `Source` are redundant in the etcd key path for convenient.
// SourceTables is putted when starting the subtask by DM-worker,
// add is updated when new tables added/removed in the upstream source by DM-worker,
// and is deleted when stopping the subtask by DM-worker,
type SourceTables struct {
	Task   string                         `json:"task"`   // data migration task name
	Source string                         `json:"source"` // upstream source ID
	Tables map[string]map[string]struct{} `json:"tables"` // table names, schema name -> table names.

	// only used to report to the caller of the watcher, do not marsh it.
	// if it's true, it means the bound has been deleted in etcd.
	IsDeleted bool `json:"-"`
}

// NewSourceTables creates a new SourceTables instances.
func NewSourceTables(task, source string, tables map[string]map[string]struct{}) SourceTables {
	return SourceTables{
		Task:   task,
		Source: source,
		Tables: tables,
	}
}

// String implements Stringer interface.
func (st SourceTables) String() string {
	s, _ := st.toJSON()
	return s
}

// toJSON returns the string of JSON represent.
func (st SourceTables) toJSON() (string, error) {
	data, err := json.Marshal(st)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// sourceTablesFromJSON constructs SourceTables from its JSON represent.
func sourceTablesFromJSON(s string) (st SourceTables, err error) {
	err = json.Unmarshal([]byte(s), &st)
	return
}

// PutSourceTables puts source tables into etcd.
// This function should often be called by DM-worker.
func PutSourceTables(cli *clientv3.Client, st SourceTables) (int64, error) {
	value, err := st.toJSON()
	if err != nil {
		return 0, err
	}
	key := common.ShardDDLOptimismSourceTablesKeyAdapter.Encode(st.Task, st.Source)

	_, rev, err := etcdutil.DoOpsInOneTxn(cli, clientv3.OpPut(key, value))
	return rev, err
}

// DeleteSourceTables deletes the source tables in etcd.
// This function should often be called by DM-worker.
func DeleteSourceTables(cli *clientv3.Client, st SourceTables) (int64, error) {
	key := common.ShardDDLOptimismSourceTablesKeyAdapter.Encode(st.Task, st.Source)
	_, rev, err := etcdutil.DoOpsInOneTxn(cli, clientv3.OpDelete(key))
	return rev, err
}

// GetAllSourceTables gets all source tables in etcd currently.
// This function should often be called by DM-master.
// k/k/v: task-name -> source-ID -> source tables.
func GetAllSourceTables(cli *clientv3.Client) (map[string]map[string]SourceTables, int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Get(ctx, common.ShardDDLOptimismSourceTablesKeyAdapter.Path(), clientv3.WithPrefix())
	if err != nil {
		return nil, 0, err
	}

	stm := make(map[string]map[string]SourceTables)
	for _, kv := range resp.Kvs {
		st, err2 := sourceTablesFromJSON(string(kv.Value))
		if err2 != nil {
			return nil, 0, err2
		}

		if _, ok := stm[st.Task]; !ok {
			stm[st.Task] = make(map[string]SourceTables)
		}
		stm[st.Task][st.Source] = st
	}

	return stm, resp.Header.Revision, nil
}

// WatchSourceTables watches PUT & DELETE operations for source tables.
// This function should often be called by DM-master.
func WatchSourceTables(ctx context.Context, cli *clientv3.Client, revision int64, outCh chan<- SourceTables) {
	ch := cli.Watch(ctx, common.ShardDDLOptimismSourceTablesKeyAdapter.Path(),
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
				var (
					st  SourceTables
					err error
				)

				switch ev.Type {
				case mvccpb.PUT:
					st, err = sourceTablesFromJSON(string(ev.Kv.Value))
				case mvccpb.DELETE:
					st, err = sourceTablesFromKey(string(ev.Kv.Key))
					st.IsDeleted = true
				default:
					// this should not happen.
					log.L().Error("unsupported etcd event type", zap.Reflect("kv", ev.Kv), zap.Reflect("type", ev.Type))
					continue
				}

				if err != nil {
					log.L().Error("fail to construct source tables", zap.Reflect("kv", ev.Kv), zap.Error(err))
				} else {
					select {
					case outCh <- st:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}
}

// sourceTablesFromKey constructs an incompleted SourceTables from an etcd key.
func sourceTablesFromKey(key string) (SourceTables, error) {
	var st SourceTables
	ks, err := common.ShardDDLOptimismSourceTablesKeyAdapter.Decode(key)
	if err != nil {
		return st, err
	}
	st.Task = ks[0]
	st.Source = ks[1]
	return st, nil
}
