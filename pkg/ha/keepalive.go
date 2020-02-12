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
	"time"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/pkg/etcdutil"
	"github.com/pingcap/dm/pkg/log"
)

var (
	timeLayout = "2006-01-02 15:04:05.999999999"
)

// WorkerEvent represents the PUT/DELETE keepalive event of DM-worker.
type WorkerEvent struct {
	EventType  mvccpb.Event_EventType
	WorkerName string
	JoinTime   time.Time
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
	_, err = cli.Put(cliCtx, k, time.Now().Format(timeLayout), clientv3.WithLease(lease.ID))
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
			_, _ = cli.Revoke(ctx, lease.ID)
			cancel()
			return nil
		}
	}
}

// WatchWorkerEvent watches the online and offline of workers from etcd.
// this function will output the worker event to evCh, output the error to errCh
func WatchWorkerEvent(ctx context.Context, cli *clientv3.Client, rev int64, evCh chan<- WorkerEvent, errCh chan<- error) {
	watcher := clientv3.NewWatcher(cli)
	ch := watcher.Watch(ctx, common.WorkerKeepAliveKeyAdapter.Path(), clientv3.WithPrefix(), clientv3.WithRev(rev))

	for {
		select {
		case wresp := <-ch:
			if wresp.Canceled {
				select {
				case errCh <- wresp.Err():
				case <-ctx.Done():
				}
				return
			}

			for _, ev := range wresp.Events {
				log.L().Info("receive dm-worker keep alive event", zap.String("operation", ev.Type.String()), zap.String("kv", string(ev.Kv.Key)))
				kvs, err := common.WorkerKeepAliveKeyAdapter.Decode(string(ev.Kv.Key))
				if err != nil {
					log.L().Warn("fail to decode dm-worker keep alive event key", zap.String("key", string(ev.Kv.Key)), zap.Error(err))
					continue
				}
				name := kvs[0]
				workerEv := WorkerEvent{
					EventType:  ev.Type,
					WorkerName: name,
				}
				if ev.Type == mvccpb.PUT {
					joinTime := string(ev.Kv.Value)
					workerEv.JoinTime, err = time.Parse(timeLayout, joinTime)
					if err != nil {
						log.L().Warn("invalid joinTime format. This etcd key might have been used by other process", zap.String("joinTime", joinTime))
						continue
					}
				}
				select {
				case evCh <- workerEv:
				case <-ctx.Done():
					log.L().Info("watch keepalive worker quit due to context canceled")
					return
				}
			}
		case <-ctx.Done():
			log.L().Info("watch keepalive worker quit due to context canceled")
			return
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
		keys, err := common.WorkerKeepAliveKeyAdapter.Decode(string(kv.Key))
		if err != nil {
			return wwm, 0, err
		}
		workerName := keys[0]
		joinTime, err := time.Parse(timeLayout, string(kv.Value))
		if err != nil {
			return wwm, 0, err
		}
		wwm[workerName] = WorkerEvent{
			WorkerName: workerName,
			JoinTime:   joinTime,
		}
	}
	return wwm, resp.Header.Revision, nil
}
