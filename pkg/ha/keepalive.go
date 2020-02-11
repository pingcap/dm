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
	"github.com/pingcap/dm/pkg/log"
)

var (
	defaultKeepAliveTTL = int64(3)
	revokeLeaseTimeout  = time.Second
	timeLayout          = "2006-01-02 15:04:05.999999999 -0700 MST"
)

type workerEvent struct {
	eventType  mvccpb.Event_EventType
	workerName string
	joinTime   time.Time
}

// KeepAlive puts the join time of the workerName into etcd.
// this key will be kept in etcd until the worker is blocked or failed
// k/v: workerName -> join time.
// returns shouldExit, error. When keepalive quited because the outside context is canceled, will return true
// TODO: fetch the actual master endpoints, the master member maybe changed.
func KeepAlive(cli *clientv3.Client, ctx context.Context, workerName string) (bool, error) {
	cliCtx, cancel := context.WithTimeout(ctx, revokeLeaseTimeout)
	defer cancel()
	lease, err := cli.Grant(cliCtx, defaultKeepAliveTTL)
	if err != nil {
		return false, err
	}
	k := common.WorkerKeepAliveKeyAdapter.Encode(workerName)
	_, err = cli.Put(cliCtx, k, time.Now().String(), clientv3.WithLease(lease.ID))
	if err != nil {
		return false, err
	}
	ch, err := cli.KeepAlive(ctx, lease.ID)
	if err != nil {
		return false, err
	}
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				log.L().Info("keep alive channel is closed")
				return false, nil
			}
		case <-ctx.Done():
			log.L().Info("server is closing, exits keepalive")
			ctx, cancel := context.WithTimeout(ctx, revokeLeaseTimeout)
			defer cancel()
			cli.Revoke(ctx, lease.ID)
			return true, nil
		}
	}
}

// WatchWorkerEvent watches the online and offline of workers from etcd.
// this function will output the worker event to evCh
func WatchWorkerEvent(cli *clientv3.Client, ctx context.Context, rev int64, evCh chan<- workerEvent) error {
	watcher := clientv3.NewWatcher(cli)
	ch := watcher.Watch(ctx, common.WorkerKeepAliveKeyAdapter.Path(), clientv3.WithPrefix(), clientv3.WithRev(rev))

	log.L().Info("startWatching")
	for {
		select {
		case wresp := <-ch:
			if wresp.Canceled {
				return wresp.Err()
			}

			for _, ev := range wresp.Events {
				log.L().Info("operateKV", zap.String("operation", ev.Type.String()), zap.String("kv", string(ev.Kv.Key)))
				kvs, err := common.WorkerKeepAliveKeyAdapter.Decode(string(ev.Kv.Key))
				if err != nil {
					log.L().Warn("coordinator decode worker keep alive key from etcd failed", zap.String("key", string(ev.Kv.Key)), zap.Error(err))
					continue
				}
				name := kvs[0]
				workerEv := workerEvent{
					eventType:  ev.Type,
					workerName: name,
				}
				if ev.Type == mvccpb.PUT {
					joinTime := string(ev.Kv.Value)
					workerEv.joinTime, err = time.Parse(timeLayout, joinTime)
					if err != nil {
						log.L().Warn("invalid joinTime format. This etcd key might have been used by other process", zap.String("joinTime", joinTime))
						continue
					}
				}
				select {
				case evCh <- workerEv:
				case <-ctx.Done():
					log.L().Info("watch keepalive worker quit due to context canceled")
					return nil
				}
			}
		case <-ctx.Done():
			log.L().Info("watch keepalive worker quit due to context canceled")
			return nil
		}
	}
}

// GetKeepAliveRev gets current revision of keepalive
func GetKeepAliveRev(cli *clientv3.Client) (int64, error) {
	resp, err := cli.Get(cli.Ctx(), common.WorkerKeepAliveKeyAdapter.Path())
	if err != nil {
		return 0, err
	}
	return resp.Header.Revision, nil
}
