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

// Info represents the shard DDL information.
// NOTE: `Task` and `Source` are redundant in the etcd key path for convenient.
type Info struct {
	Task   string   `json:"task"`   // data migration task name
	Source string   `json:"source"` // upstream source ID
	Schema string   `json:"schema"` // schema name of the DDL
	Table  string   `json:"table"`  // table name of the DDL
	DDLs   []string `json:"ddls"`   // DDL statements
}

// NewInfo creates a new Info instance.
func NewInfo(task, source, schema, table string, DDLs []string) Info {
	return Info{
		Task:   task,
		Source: source,
		Schema: schema,
		Table:  table,
		DDLs:   DDLs,
	}
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
func PutInfo(cli *clientv3.Client, info Info) (int64, error) {
	value, err := info.toJSON()
	if err != nil {
		return 0, err
	}
	key := common.ShardDDLPessimismInfoKeyAdapter.Encode(info.Task, info.Source)

	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Put(ctx, key, value)
	if err != nil {
		return 0, err
	}
	return resp.Header.Revision, nil
}

// GetAllInfo gets all shard DDL info in etcd currently.
// k/k/v: task-name -> source-ID -> DDL info.
func GetAllInfo(cli *clientv3.Client) (map[string]map[string]Info, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Get(ctx, common.ShardDDLPessimismInfoKeyAdapter.Path(), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	ifm := make(map[string]map[string]Info)
	for _, kv := range resp.Kvs {
		info, err2 := infoFromJSON(string(kv.Value))
		if err2 != nil {
			return nil, err2
		}

		if _, ok := ifm[info.Task]; !ok {
			ifm[info.Task] = make(map[string]Info)
		}
		ifm[info.Task][info.Source] = info
	}

	return ifm, nil
}

// WatchInfoPut watches PUT operations for info.
func WatchInfoPut(ctx context.Context, cli *clientv3.Client, revision int64, outCh chan<- Info) {
	ch := cli.Watch(ctx, common.ShardDDLPessimismInfoKeyAdapter.Path(),
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
				outCh <- info
			}
		}
	}
}
