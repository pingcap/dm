// Copyright 2021 PingCAP, Inc.
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

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/pkg/etcdutil"
	"github.com/pingcap/dm/pkg/terror"
)

const (
	retryNum = 20
)

// WorkerSourceLoadCount uses to watch load count events.
type WorkerSourceLoadCount struct {
	Worker   string
	Source   string
	Count    int
	IsDelete bool
}

// GetWorkerSourceLoadCount gets the count of tasks in load stage for the worker and source.
// k/v: (worker-name, sourceID) -> tasks.
func GetWorkerSourceLoadCount(cli *clientv3.Client, worker, sourceID string) (int, int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()
	resp, err := cli.Get(ctx, common.WorkerSourceLoadCountKeyAdapter.Encode(worker, sourceID))
	if err != nil {
		return 0, 0, err
	}

	if resp.Count <= 0 {
		return 0, 0, nil
	}

	var count int
	err = json.Unmarshal(resp.Kvs[0].Value, &count)

	return count, resp.Kvs[0].Version, err
}

// GetAllWorkerSourceLoadCount gets the count of tasks in load stage for the worker and source.
// k/v: (worker-name, sourceID) -> tasks.
func GetAllWorkerSourceLoadCount(cli *clientv3.Client) (map[string]map[string]int, int64, error) {
	var (
		count = 0
		wslcm = make(map[string]map[string]int)
	)
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()
	resp, err := cli.Get(ctx, common.WorkerSourceLoadCountKeyAdapter.Path(), clientv3.WithPrefix())
	if err != nil {
		return wslcm, 0, err
	}

	for _, kv := range resp.Kvs {
		keys, err2 := common.WorkerSourceLoadCountKeyAdapter.Decode(string(kv.Key))
		if err2 != nil {
			return nil, 0, err2
		}

		worker := keys[0]
		source := keys[1]

		err = json.Unmarshal(kv.Value, &count)
		if err != nil {
			return wslcm, 0, err
		}

		if _, ok := wslcm[worker]; !ok {
			wslcm[worker] = make(map[string]int)
		}
		wslcm[worker][source] = count
	}

	return wslcm, resp.Header.Revision, err
}

// WatchLoadCount watches PUT & DELETE operations for load count.
// This function should often be called by DM-master.
func WatchLoadCount(ctx context.Context, cli *clientv3.Client, revision int64,
	outCh chan<- WorkerSourceLoadCount, errCh chan<- error) {
	// NOTE: WithPrevKV used to get a valid `ev.PrevKv` for deletion.
	ch := cli.Watch(ctx, common.WorkerSourceLoadCountKeyAdapter.Path(),
		clientv3.WithPrefix(), clientv3.WithRev(revision), clientv3.WithPrevKV())

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
				var (
					wlsc WorkerSourceLoadCount
					err  error
					keys []string
				)

				switch ev.Type {
				case mvccpb.PUT, mvccpb.DELETE:
					keys, err = common.WorkerSourceLoadCountKeyAdapter.Decode(string(ev.Kv.Key))
					if err == nil {
						wlsc.Worker = keys[0]
						wlsc.Source = keys[1]
						wlsc.IsDelete = (ev.Type == mvccpb.DELETE)
						err = json.Unmarshal(ev.Kv.Value, &wlsc.Count)
					}
				default:
					// this should not happen.
					err = fmt.Errorf("unsupported ectd event type %v", ev.Type)
				}

				if err != nil {
					select {
					case errCh <- err:
					case <-ctx.Done():
						return
					}
				} else {
					select {
					case outCh <- wlsc:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}
}

// IncWorkerSourceLoadCount increase the count of tasks in load stage for the worker and source.
// k/v: (worker-name, sourceID) -> count.
func IncWorkerSourceLoadCount(cli *clientv3.Client, worker, sourceID string) (int64, error) {
	for i := 0; i < retryNum; i++ {
		count, version, err := GetWorkerSourceLoadCount(cli, worker, sourceID)
		if err != nil {
			return 0, err
		}
		rev, succ, err := putWorkerSourceLoadCountWithVer(cli, worker, sourceID, count+1, version)
		if err != nil {
			return 0, err
		}
		if succ {
			return rev, nil
		}
	}
	return 0, terror.ErrOperateEtcdLoadCount.Generate("inc")
}

// DecWorkerSourceLoadCount decrease the count of tasks in load stage for the worker and source.
// k/v: (worker-name, sourceID) -> count.
func DecWorkerSourceLoadCount(cli *clientv3.Client, worker, sourceID string) (int64, error) {
	var (
		rev  int64
		succ bool
	)
	for i := 0; i < retryNum; i++ {
		count, version, err := GetWorkerSourceLoadCount(cli, worker, sourceID)
		if err != nil || count <= 0 {
			return 0, err
		}
		if count == 1 {
			rev, succ, err = delWorkerSourceLoadCountWithVer(cli, worker, sourceID, version)
		} else {
			rev, succ, err = putWorkerSourceLoadCountWithVer(cli, worker, sourceID, count-1, version)
		}
		if err != nil {
			return 0, err
		}
		if succ {
			return rev, nil
		}
	}
	return 0, terror.ErrOperateEtcdLoadCount.Generate("dec")
}

// DelWorkerSourceLoadCount del the count of tasks in load stage for the worker and source.
// k/v: (worker-name, sourceID) -> count.
func DelWorkerSourceLoadCount(cli *clientv3.Client, worker, sourceID string) (int64, bool, error) {
	key := common.WorkerSourceLoadCountKeyAdapter.Encode(worker, sourceID)

	resp, rev, err := etcdutil.DoOpsInOneTxnWithRetry(cli, clientv3.OpDelete(key))
	if err != nil {
		return 0, false, err
	}
	return rev, resp.Succeeded, nil
}

// DelWorkerSourceLoadCountByWorker del the count of tasks in load stage for the worker and source.
// k/v: (worker-name, sourceID) -> count.
func DelWorkerSourceLoadCountByWorker(cli *clientv3.Client, worker string) (int64, bool, error) {
	key := common.WorkerSourceLoadCountKeyAdapter.Encode(worker)

	resp, rev, err := etcdutil.DoOpsInOneTxnWithRetry(cli, clientv3.OpDelete(key, clientv3.WithPrefix()))
	if err != nil {
		return 0, false, err
	}
	return rev, resp.Succeeded, nil
}

// putWorkerSourceLoadCount put the count of tasks in load stage for the worker and source.
// k/v: (worker-name, sourceID) -> count.
func putWorkerSourceLoadCountWithVer(cli *clientv3.Client, worker, sourceID string, count int, version int64) (int64, bool, error) {
	data, err := json.Marshal(count)
	if err != nil {
		return 0, false, err
	}
	key := common.WorkerSourceLoadCountKeyAdapter.Encode(worker, sourceID)
	opPut := clientv3.OpPut(key, string(data))
	cmp := clientv3.Compare(clientv3.Version(key), "=", version)

	resp, rev, err := etcdutil.DoOpsInOneCmpsTxnWithRetry(cli, []clientv3.Cmp{cmp}, []clientv3.Op{opPut}, []clientv3.Op{})
	if err != nil {
		return 0, false, err
	}
	return rev, resp.Succeeded, nil
}

// delWorkerSourceLoadCount del the count of tasks in load stage for the worker and source.
// k/v: (worker-name, sourceID) -> count.
func delWorkerSourceLoadCountWithVer(cli *clientv3.Client, worker, sourceID string, version int64) (int64, bool, error) {
	key := common.WorkerSourceLoadCountKeyAdapter.Encode(worker, sourceID)
	opDel := clientv3.OpDelete(key)
	cmp := clientv3.Compare(clientv3.Version(key), "=", version)

	resp, rev, err := etcdutil.DoOpsInOneCmpsTxnWithRetry(cli, []clientv3.Cmp{cmp}, []clientv3.Op{opDel}, []clientv3.Op{})
	if err != nil {
		return 0, false, err
	}
	return rev, resp.Succeeded, nil
}
