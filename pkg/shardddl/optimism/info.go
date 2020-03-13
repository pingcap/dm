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

	"github.com/pingcap/parser/model"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/pkg/etcdutil"
	"github.com/pingcap/dm/pkg/log"
)

// TODO: much of the code in optimistic mode is very similar to pessimistic mode, we can try to combine them together.

// Info represents the shard DDL information.
// This information should be persistent in etcd so can be retrieved after the DM-master leader restarted or changed.
// NOTE: `Task`, `Source`, `UpSchema` and `DownTable` are redundant in the etcd key path for convenient.
// Info is putted when receiving a shard DDL for a table in DM-worker,
// and is deleted when removing the lock by DM-master
// because we need the newest schema in Info to recover the lock when restarting DM-master.
// when new Info is putted to overwrite the old one, the DM-master should update the lock based on the new one.
type Info struct {
	Task       string   `json:"task"`        // data migration task name
	Source     string   `json:"source"`      // upstream source ID
	UpSchema   string   `json:"up-schema"`   // upstream/source schema name, different sources can have the same schema name
	UpTable    string   `json:"up-table"`    // upstream/source table name, different sources can have the same table name
	DownSchema string   `json:"down-schema"` // downstream/target schema name
	DownTable  string   `json:"down-table"`  // downstream/target table name
	DDLs       []string `json:"ddls"`        // DDL statements

	TableInfoBefore *model.TableInfo `json:"table-info-before"` // the tracked table schema before applying the DDLs
	TableInfoAfter  *model.TableInfo `json:"table-info-after"`  // the tracked table schema after applying the DDLs
}

// NewInfo creates a new Info instance.
func NewInfo(task, source, upSchema, upTable, downSchema, downTable string,
	DDLs []string, tableInfoBefore, tableInfoAfter *model.TableInfo) Info {
	return Info{
		Task:            task,
		Source:          source,
		UpSchema:        upSchema,
		UpTable:         upTable,
		DownSchema:      downSchema,
		DownTable:       downTable,
		DDLs:            DDLs,
		TableInfoBefore: tableInfoBefore,
		TableInfoAfter:  tableInfoAfter,
	}
}

// String implements Stringer interface.
func (i Info) String() string {
	s, _ := i.toJSON()
	return s
}

// toJSON returns the string of JSON represent.
func (i Info) toJSON() (string, error) {
	data, err := json.Marshal(i)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// infoFromJSON constructs Info from its JSON represent.
func infoFromJSON(s string) (i Info, err error) {
	err = json.Unmarshal([]byte(s), &i)
	return
}

// PutInfo puts the shard DDL info into etcd.
// NOTE:
//   In some cases before the lock resolved, the same DDL info may be PUT multiple times:
//     1. start-task after stop-task.
//     2. resume-task after paused manually or automatically.
//     3. the task scheduled to another DM-worker instance (just like case-1).
//   Then we need to ensure re-PUT is safe:
//     1. DM-master can construct the lock and do the coordination correctly.
//     2. DM-worker can re-PUT and comply with the coordination correctly.
// This function should often be called by DM-worker.
func PutInfo(cli *clientv3.Client, info Info) (int64, error) {
	value, err := info.toJSON()
	if err != nil {
		return 0, err
	}
	key := common.ShardDDLOptimismInfoKeyAdapter.Encode(info.Task, info.Source, info.UpSchema, info.UpTable)

	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Put(ctx, key, value)
	if err != nil {
		return 0, err
	}
	return resp.Header.Revision, nil
}

// GetAllInfo gets all shard DDL info in etcd currently.
// This function should often be called by DM-master.
// k/k/k/k/v: task-name -> source-ID -> upstream-schema-name -> upstream-table-name -> shard DDL info.
// ugly code, but have no better idea now.
func GetAllInfo(cli *clientv3.Client) (map[string]map[string]map[string]map[string]Info, int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Get(ctx, common.ShardDDLOptimismInfoKeyAdapter.Path(), clientv3.WithPrefix())
	if err != nil {
		return nil, 0, err
	}

	ifm := make(map[string]map[string]map[string]map[string]Info)
	for _, kv := range resp.Kvs {
		info, err2 := infoFromJSON(string(kv.Value))
		if err2 != nil {
			return nil, 0, err2
		}

		if _, ok := ifm[info.Task]; !ok {
			ifm[info.Task] = make(map[string]map[string]map[string]Info)
		}
		if _, ok := ifm[info.Task][info.Source]; !ok {
			ifm[info.Task][info.Source] = make(map[string]map[string]Info)
		}
		if _, ok := ifm[info.Task][info.Source][info.UpSchema]; !ok {
			ifm[info.Task][info.Source][info.UpSchema] = make(map[string]Info)
		}
		ifm[info.Task][info.Source][info.UpSchema][info.UpTable] = info
	}

	return ifm, resp.Header.Revision, nil
}

// WatchInfoPut watches PUT operations for info.
// This function should often be called by DM-master.
func WatchInfoPut(ctx context.Context, cli *clientv3.Client, revision int64, outCh chan<- Info) {
	ch := cli.Watch(ctx, common.ShardDDLOptimismInfoKeyAdapter.Path(),
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

				info, err := infoFromJSON(string(ev.Kv.Value))
				if err != nil {
					// this should not happen.
					log.L().Error("fail to construct shard DDL info from json", zap.ByteString("json", ev.Kv.Value))
					continue
				}
				select {
				case outCh <- info:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// deleteInfoOp returns a DELETE etcd operation for info.
// This operation should often be sent by DM-worker.
func deleteInfoOp(info Info) clientv3.Op {
	return clientv3.OpDelete(common.ShardDDLOptimismInfoKeyAdapter.Encode(
		info.Task, info.Source, info.UpSchema, info.UpTable))
}
