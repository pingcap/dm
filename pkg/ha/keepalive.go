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
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/pkg/etcdutil"
	"github.com/pingcap/dm/pkg/log"
)

var (
	// CurrentKeepAliveTTL may be assigned to KeepAliveTTL or RelayKeepAliveTTL
	CurrentKeepAliveTTL int64
	// KeepAliveUpdateCh is used to notify keepalive TTL changing, in order to let watcher not see a DELETE of old key
	KeepAliveUpdateCh = make(chan int64, 10)
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
func KeepAlive(ctx context.Context, cli *clientv3.Client, workerName string, keepAliveTTL int64) error {
	// TTL in KeepAliveUpdateCh has higher priority
	for len(KeepAliveUpdateCh) > 0 {
		keepAliveTTL = <-KeepAliveUpdateCh
	}
	// though in regular routine there's no concurrent KeepAlive, we need to handle tests
	atomic.StoreInt64(&CurrentKeepAliveTTL, keepAliveTTL)

	k := common.WorkerKeepAliveKeyAdapter.Encode(workerName)
	workerEventJSON, err := WorkerEvent{
		WorkerName: workerName,
		JoinTime:   time.Now(),
	}.toJSON()
	if err != nil {
		return err
	}

	grantAndPutKV := func(k, v string, ttl int64) (clientv3.LeaseID, error) {
		cliCtx, cancel := context.WithTimeout(ctx, etcdutil.DefaultRequestTimeout)
		defer cancel()
		lease, err2 := cli.Grant(cliCtx, ttl)
		if err2 != nil {
			return 0, err2
		}
		_, err = cli.Put(cliCtx, k, v, clientv3.WithLease(lease.ID))
		if err != nil {
			return 0, err
		}
		return lease.ID, nil
	}

	leaseID, err := grantAndPutKV(k, workerEventJSON, keepAliveTTL)
	if err != nil {
		return err
	}

	// once we put the key successfully, we should revoke lease before we quit keepalive normally
	defer func() {
		_, err2 := revokeLease(cli, leaseID)
		if err2 != nil {
			log.L().Warn("fail to revoke lease", zap.Error(err))
		}
	}()

	keepAliveCtx, keepAliveCancel := context.WithCancel(ctx)
	defer func() {
		keepAliveCancel()
	}()

	ch, err := cli.KeepAlive(keepAliveCtx, leaseID)
	if err != nil {
		return err
	}
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				log.L().Info("keep alive channel is closed")
				keepAliveCancel() // make go vet happy
				return nil
			}
		case <-ctx.Done():
			log.L().Info("ctx is canceled, keepalive will exit now")
			keepAliveCancel() // make go vet happy
			return nil
		case newTTL := <-KeepAliveUpdateCh:
			// create a new lease with new TTL, and overwrite original KV
			oldLeaseID := leaseID
			leaseID, err = grantAndPutKV(k, workerEventJSON, newTTL)
			if err != nil {
				keepAliveCancel() // make go vet happy
				return err
			}

			oldCancel := keepAliveCancel
			keepAliveCtx, keepAliveCancel = context.WithCancel(ctx)
			ch, err = cli.KeepAlive(keepAliveCtx, leaseID)
			if err != nil {
				log.L().Error("meet error when change keepalive TTL", zap.Error(err))
				keepAliveCancel() // make go vet happy
				return err
			}
			log.L().Info("dynamically changed keepalive TTL to", zap.Int64("ttl in seconds", newTTL))

			// after new keepalive is succeed, we cancel the old keepalive
			oldCancel()
			_, err2 := revokeLease(cli, oldLeaseID)
			if err2 != nil {
				log.L().Warn("fail to revoke lease", zap.Error(err))
			}
		}
	}
}

// ATTENTION!!! we must ensure cli.Ctx() not done when we are exiting worker
// Do not set cfg.Context when creating cli or do not cancel this Context or it's parent context
func revokeLease(cli *clientv3.Client, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRevokeLeaseTimeout)
	defer cancel()
	return cli.Revoke(ctx, id)
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
