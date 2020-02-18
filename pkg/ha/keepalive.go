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
	"time"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/pkg/etcdutil"
	"github.com/pingcap/dm/pkg/log"
)

// WorkerEvent represents the PUT/DELETE keepalive event of DM-worker.
type WorkerEvent struct {
	WorkerName string    `json:"worker-name"` // the worker name of the worker.
	JoinTime   time.Time `json:"join-time"`   // the time when worker start to keepalive with etcd

	// only used to report to the caller of the watcher, do not marsh it.
	// if it's true, it means the worker has been deleted in etcd.
	IsDeleted bool `json:"-"`
}

// String implements Stringer interface.
func (w WorkerEvent) String() string {
	str, _ := w.toJSON()
	return str
}

// toJSON returns the string of JSON represent.
func (w WorkerEvent) toJSON() (string, error) {
	data, err := json.Marshal(w)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// workerEventFromJSON constructs WorkerEvent from its JSON represent.
func workerEventFromJSON(s string) (w WorkerEvent, err error) {
	err = json.Unmarshal([]byte(s), &w)
	return
}

func workerEventFromKey(key string) (WorkerEvent, error) {
	var w WorkerEvent
	ks, err := common.WorkerKeepAliveKeyAdapter.Decode(key)
	if err != nil {
		return w, err
	}
	w.WorkerName = ks[0]
	return w, nil
}

// KeepAlive puts the join time of the workerName into etcd.
// this key will be kept in etcd until the worker is blocked or failed
// k/v: workerName -> join time.
// TODO: fetch the actual master endpoints, the master member maybe changed.
func KeepAlive(ctx context.Context, cli *clientv3.Client, workerName string, keepAliveTTL int64) error {
	cliCtx, cancel := context.WithTimeout(ctx, etcdutil.DefaultRequestTimeout)
	defer cancel()
	lease, err := cli.Grant(cliCtx, keepAliveTTL)
	if err != nil {
		return err
	}
	k := common.WorkerKeepAliveKeyAdapter.Encode(workerName)
	workerEventJSON, err := WorkerEvent{
		WorkerName: workerName,
		JoinTime:   time.Now(),
	}.toJSON()
	if err != nil {
		return err
	}
	_, err = cli.Put(cliCtx, k, workerEventJSON, clientv3.WithLease(lease.ID))
	if err != nil {
		return err
	}
	ch, err := cli.KeepAlive(ctx, lease.ID)
	if err != nil {
		return err
	}
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				log.L().Info("keep alive channel is closed")
				return nil
			}
		case <-ctx.Done():
			log.L().Info("ctx is canceled, keepalive will exit now")
			ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
			cli.Revoke(ctx, lease.ID)
			cancel()
			return nil
		}
	}
}

// WatchWorkerEvent watches the online and offline of workers from etcd.
// this function will output the worker event to evCh, output the error to errCh
func WatchWorkerEvent(ctx context.Context, cli *clientv3.Client, rev int64, outCh chan<- WorkerEvent, errCh chan<- error) {
	watcher := clientv3.NewWatcher(cli)
	ch := watcher.Watch(ctx, common.WorkerKeepAliveKeyAdapter.Path(), clientv3.WithPrefix(), clientv3.WithRev(rev))

	for {
		select {
		case <-ctx.Done():
			log.L().Info("watch keepalive worker quit due to context canceled")
			return
		case resp := <-ch:
			if resp.Canceled {
				select {
				case errCh <- resp.Err():
				case <-ctx.Done():
				}
				return
			}

			for _, ev := range resp.Events {
				log.L().Info("receive dm-worker keep alive event", zap.String("operation", ev.Type.String()), zap.String("kv", string(ev.Kv.Key)))
				var (
					event WorkerEvent
					err   error
				)
				switch ev.Type {
				case mvccpb.PUT:
					event, err = workerEventFromJSON(string(ev.Kv.Value))
				case mvccpb.DELETE:
					event, err = workerEventFromKey(string(ev.Kv.Key))
					event.IsDeleted = true
				default:
					// this should not happen.
					log.L().Error("unsupported etcd event type", zap.Reflect("kv", ev.Kv), zap.Reflect("type", ev.Type))
					continue
				}
				if err != nil {
					select {
					case errCh <- err:
					case <-ctx.Done():
						return
					}
				} else {
					select {
					case outCh <- event:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}
}

// GetKeepAliveWorkers gets current alive workers,
// and returns a map{workerName: WorkerEvent}, revision and error
func GetKeepAliveWorkers(cli *clientv3.Client) (map[string]WorkerEvent, int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	var wwm map[string]WorkerEvent
	resp, err := cli.Get(ctx, common.WorkerKeepAliveKeyAdapter.Path(), clientv3.WithPrefix())
	if err != nil {
		return wwm, 0, err
	}

	wwm = make(map[string]WorkerEvent, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		w, err := workerEventFromJSON(string(kv.Value))
		if err != nil {
			return wwm, 0, err
		}
		wwm[w.WorkerName] = w
	}
	return wwm, resp.Header.Revision, nil
}
