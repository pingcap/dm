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

package ha

import (
	"context"
	"encoding/json"
	"fmt"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/pkg/etcdutil"
	"github.com/pingcap/dm/pkg/log"
)

// SourceBound represents the bound relationship between the DM-worker instance and the upstream MySQL source.
type SourceBound struct {
	Source string `json:"source"` // the source ID of the upstream.
	Worker string `json:"worker"` // the name of the bounded DM-worker for the source.

	// only used to report to the caller of the watcher, do not marsh it.
	// if it's true, it means the bound has been deleted in etcd.
	IsDeleted bool `json:"-"`
	// only has value in watcher, will get 0 in GetSourceBound
	// record the etcd revision right after putting this SourceBound
	Revision int64 `json:"-"`
}

// NewSourceBound creates a new SourceBound instance.
func NewSourceBound(source, worker string) SourceBound {
	return SourceBound{
		Source: source,
		Worker: worker,
	}
}

// String implements Stringer interface.
func (b SourceBound) String() string {
	s, _ := b.toJSON()
	return s
}

// toJSON returns the string of JSON represent.
func (b SourceBound) toJSON() (string, error) {
	data, err := json.Marshal(b)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// sourceBoundFromJSON constructs SourceBound from its JSON represent.
func sourceBoundFromJSON(s string) (b SourceBound, err error) {
	err = json.Unmarshal([]byte(s), &b)
	return
}

// PutSourceBound puts the bound relationship into etcd.
// k/v: worker-name -> bound relationship.
func PutSourceBound(cli *clientv3.Client, bounds ...SourceBound) (int64, error) {
	ops := make([]clientv3.Op, 0, len(bounds))
	for _, bound := range bounds {
		op, err := putSourceBoundOp(bound)
		if err != nil {
			return 0, err
		}
		ops = append(ops, op)
	}

	return etcdutil.DoOpsInOneTxn(cli, ops...)
}

// DeleteSourceBound deletes the bound relationship in etcd for the specified worker.
func DeleteSourceBound(cli *clientv3.Client, workers ...string) (int64, error) {
	ops := make([]clientv3.Op, 0, len(workers))
	for _, worker := range workers {
		ops = append(ops, deleteSourceBoundOp(worker))
	}
	return etcdutil.DoOpsInOneTxn(cli, ops...)
}

// GetSourceBound gets the source bound relationship for the specified DM-worker.
// if the bound relationship for the worker name not exist, return with `err == nil` and `revision == 0`.
// if the worker name is "", it will return all bound relationships as a map{worker-name: bound}.
// if the worker name is given, it will return a map{worker-name: bound} whose length is 1.
func GetSourceBound(cli *clientv3.Client, worker string) (map[string]SourceBound, int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	var (
		sbm  = make(map[string]SourceBound)
		resp *clientv3.GetResponse
		err  error
	)
	if worker != "" {
		resp, err = cli.Get(ctx, common.UpstreamBoundWorkerKeyAdapter.Encode(worker))
	} else {
		resp, err = cli.Get(ctx, common.UpstreamBoundWorkerKeyAdapter.Path(), clientv3.WithPrefix())
	}

	if err != nil {
		return sbm, 0, err
	}

	if resp.Count == 0 {
		return sbm, 0, nil
	} else if worker != "" && resp.Count > 1 {
		// TODO(csuzhangxc): add terror.
		// this should not happen.
		return sbm, 0, fmt.Errorf("too many bound relationship (%d) exist for the DM-worker %s", resp.Count, worker)
	}

	for _, kvs := range resp.Kvs {
		bound, err2 := sourceBoundFromJSON(string(kvs.Value))
		if err2 != nil {
			return sbm, 0, err2
		}
		sbm[bound.Worker] = bound
	}

	return sbm, resp.Header.Revision, nil
}

// WatchSourceBound watches PUT & DELETE operations for the bound relationship of the specified DM-worker.
// For the DELETE operations, it returns an empty bound relationship.
func WatchSourceBound(ctx context.Context, cli *clientv3.Client,
	worker string, revision int64, outCh chan<- SourceBound, errCh chan<- error) {
	ch := cli.Watch(ctx, common.UpstreamBoundWorkerKeyAdapter.Encode(worker), clientv3.WithRev(revision))

	for {
		select {
		case <-ctx.Done():
			return
		case resp := <-ch:
			if resp.Canceled {
				// TODO(csuzhangxc): do retry here.
				if resp.Err() != nil {
					select {
					case errCh <- resp.Err():
					case <-ctx.Done():
					}
				}
				return
			}

			for _, ev := range resp.Events {
				var (
					bound SourceBound
					err   error
				)
				switch ev.Type {
				case mvccpb.PUT:
					bound, err = sourceBoundFromJSON(string(ev.Kv.Value))
				case mvccpb.DELETE:
					bound, err = sourceBoundFromKey(string(ev.Kv.Key))
					bound.IsDeleted = true
				default:
					// this should not happen.
					log.L().Error("unsupported etcd event type", zap.Reflect("kv", ev.Kv), zap.Reflect("type", ev.Type))
					continue
				}
				bound.Revision = ev.Kv.ModRevision

				if err != nil {
					select {
					case errCh <- err:
					case <-ctx.Done():
						return
					}
				} else {
					select {
					case outCh <- bound:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}
}

// sourceBoundFromKey constructs an incomplete bound relationship from an etcd key.
func sourceBoundFromKey(key string) (SourceBound, error) {
	var bound SourceBound
	ks, err := common.UpstreamBoundWorkerKeyAdapter.Decode(key)
	if err != nil {
		return bound, err
	}
	bound.Worker = ks[0]
	return bound, nil
}

// deleteSourceBoundOp returns a DELETE ectd operation for the bound relationship of the specified DM-worker.
func deleteSourceBoundOp(worker string) clientv3.Op {
	return clientv3.OpDelete(common.UpstreamBoundWorkerKeyAdapter.Encode(worker))
}

// putSourceBoundOp returns a PUT etcd operation for the bound relationship.
// k/v: worker-name -> bound relationship.
func putSourceBoundOp(bound SourceBound) (clientv3.Op, error) {
	value, err := bound.toJSON()
	if err != nil {
		return clientv3.Op{}, err
	}
	key := common.UpstreamBoundWorkerKeyAdapter.Encode(bound.Worker)

	return clientv3.OpPut(key, value), nil
}
