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
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/etcdutil"
	"github.com/pingcap/dm/pkg/log"
)

// Stage represents the running stage for a relay or subtask.
type Stage struct {
	Expect pb.Stage `json:"expect"`         // the expectant stage.
	Source string   `json:"source"`         // the source ID of the upstream.
	Task   string   `json:"task,omitempty"` // the task name for subtask; empty for relay.

	// only used to report the caller of the watcher, do not marsh it.
	// if it's true, it means the stage has been deleted in etcd.
	IsDeleted bool `json:"-"`
}

// NewRelayStage creates a new Stage instance for relay.
func NewRelayStage(expect pb.Stage, source string) Stage {
	return newStage(expect, source, "")
}

// NewSubTaskStage creates a new Stage instance for subtask.
func NewSubTaskStage(expect pb.Stage, source, task string) Stage {
	return newStage(expect, source, task)
}

// newStage creates a new Stage instance.
func newStage(expect pb.Stage, source, task string) Stage {
	return Stage{
		Expect: expect,
		Source: source,
		Task:   task,
	}
}

// String implements Stringer interface.
func (s Stage) String() string {
	str, _ := s.toJSON()
	return str
}

// toJSON returns the string of JSON represent.
func (s Stage) toJSON() (string, error) {
	data, err := json.Marshal(s)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// stageFromJSON constructs Stage from its JSON represent.
func stageFromJSON(str string) (s Stage, err error) {
	err = json.Unmarshal([]byte(str), &s)
	return
}

// PutRelayStage puts the stage of the relay into etcd.
// k/v: sourceID -> the running stage of the relay.
func PutRelayStage(cli *clientv3.Client, stages ...Stage) (int64, error) {
	ops, err := putRelayStageOp(stages...)
	if err != nil {
		return 0, err
	}
	return putStage(cli, ops...)
}

// PutSubTaskStage puts the stage of the subtask into etcd.
// k/v: sourceID, task -> the running stage of the subtask.
func PutSubTaskStage(cli *clientv3.Client, stages ...Stage) (int64, error) {
	ops, err := putSubTaskStageOp(stages...)
	if err != nil {
		return 0, err
	}
	return putStage(cli, ops...)
}

// GetRelayStage gets the relay stage for the specified upstream source.
// if the stage for the source not exist, return with `err == nil` and `revision=0`.
func GetRelayStage(cli *clientv3.Client, source string) (Stage, int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	var stage Stage
	resp, err := cli.Get(ctx, common.StageRelayKeyAdapter.Encode(source))
	if err != nil {
		return stage, 0, err
	}

	if resp.Count == 0 {
		return stage, 0, nil
	} else if resp.Count > 1 {
		// TODO(csuzhangxc): add terror.
		// this should not happen.
		return stage, 0, fmt.Errorf("too many relay stage (%d) exist for source %s", resp.Count, source)
	}

	stage, err = stageFromJSON(string(resp.Kvs[0].Value))
	if err != nil {
		return stage, 0, err
	}

	return stage, resp.Header.Revision, nil
}

// GetSubTaskStage gets the subtask stage for the specified upstream source and task name.
// if the stage for the source and task name not exist, return with `err == nil` and `revision=0`.
// if task name is "", it will return all subtasks' stage as a map{task-name: stage} for the source.
// if task name is given, it will return a map{task-name: stage} whose length is 1.
func GetSubTaskStage(cli *clientv3.Client, source, task string) (map[string]Stage, int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	var (
		stm  = make(map[string]Stage)
		resp *clientv3.GetResponse
		err  error
	)
	if task != "" {
		resp, err = cli.Get(ctx, common.StageSubTaskKeyAdapter.Encode(source, task))
	} else {
		resp, err = cli.Get(ctx, common.StageSubTaskKeyAdapter.Encode(source), clientv3.WithPrefix())
	}

	if err != nil {
		return stm, 0, err
	}

	if resp.Count == 0 {
		return stm, 0, nil
	} else if task != "" && resp.Count > 1 {
		return stm, 0, fmt.Errorf("too many stage (%d) exist for subtask {sourceID: %s, task name: %s}", resp.Count, source, task)
	}

	for _, kvs := range resp.Kvs {
		stage, err2 := stageFromJSON(string(kvs.Value))
		if err2 != nil {
			return stm, 0, err2
		}
		stm[stage.Task] = stage
	}

	return stm, resp.Header.Revision, nil
}

// WatchRelayStage watches PUT & DELETE operations for the relay stage.
// for the DELETE stage, it returns an empty stage.
func WatchRelayStage(ctx context.Context, cli *clientv3.Client,
	source string, revision int64, outCh chan<- Stage) {
	stageFromKey := func(key string) (Stage, error) {
		var stage Stage
		ks, err := common.StageRelayKeyAdapter.Decode(key)
		if err != nil {
			return stage, err
		}
		stage.Source = ks[0]
		return stage, nil
	}
	ch := cli.Watch(ctx, common.StageRelayKeyAdapter.Encode(source), clientv3.WithRev(revision))
	watchStage(ctx, ch, stageFromKey, outCh)
}

// WatchSubTaskStage watches PUT & DELETE operations for the subtask stage.
// for the DELETE stage, it returns an empty stage.
func WatchSubTaskStage(ctx context.Context, cli *clientv3.Client,
	source string, revision int64, outCh chan<- Stage) {
	stageFromKey := func(key string) (Stage, error) {
		var stage Stage
		ks, err := common.StageSubTaskKeyAdapter.Decode(key)
		if err != nil {
			return stage, err
		}
		stage.Source = ks[0]
		stage.Task = ks[1]
		return stage, nil
	}
	ch := cli.Watch(ctx, common.StageSubTaskKeyAdapter.Encode(source), clientv3.WithPrefix(), clientv3.WithRev(revision))
	watchStage(ctx, ch, stageFromKey, outCh)
}

// DeleteSubTaskStage deletes the subtask stage.
func DeleteSubTaskStage(cli *clientv3.Client, stages ...Stage) (int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	ops := deleteSubTaskStageOp(stages...)
	resp, err := cli.Txn(ctx).Then(ops...).Commit()
	if err != nil {
		return 0, err
	}
	return resp.Header.Revision, nil
}

// watchStage watches PUT & DELETE operations for the stage.
func watchStage(ctx context.Context, watchCh clientv3.WatchChan,
	stageFromKey func(key string) (Stage, error), outCh chan<- Stage) {
	for {
		select {
		case <-ctx.Done():
			return
		case resp := <-watchCh:
			if resp.Canceled {
				return
			}

			for _, ev := range resp.Events {
				var (
					stage Stage
					err   error
				)
				switch ev.Type {
				case mvccpb.PUT:
					stage, err = stageFromJSON(string(ev.Kv.Value))
					if err != nil {
						// this should not happen.
						log.L().Error("fail to construct stage", zap.ByteString("json", ev.Kv.Value), zap.Error(err))
						continue
					}
				case mvccpb.DELETE:
					stage, err = stageFromKey(string(ev.Kv.Key))
					if err != nil {
						// this should not happen.
						log.L().Error("fail to decode key", zap.ByteString("key", ev.Kv.Key), zap.Error(err))
						continue
					}
					stage.IsDeleted = true
				default:
					// this should not happen.
					log.L().Error("unsupported etcd event type", zap.Reflect("kv", ev.Kv), zap.Reflect("type", ev.Type))
					continue
				}

				select {
				case outCh <- stage:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// putStage puts stages into etcd.
func putStage(cli *clientv3.Client, ops ...clientv3.Op) (int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Txn(ctx).Then(ops...).Commit()
	if err != nil {
		return 0, err
	}
	return resp.Header.Revision, nil
}

// putRelayStageOp returns a list of PUT etcd operation for the relay stage.
// k/v: sourceID -> the running stage of the relay.
func putRelayStageOp(stages ...Stage) ([]clientv3.Op, error) {
	ops := make([]clientv3.Op, 0, len(stages))
	for _, stage := range stages {
		value, err := stage.toJSON()
		if err != nil {
			return ops, err
		}
		key := common.StageRelayKeyAdapter.Encode(stage.Source)
		ops = append(ops, clientv3.OpPut(key, value))
	}
	return ops, nil
}

// putSubTaskStageOp returns a list of PUT etcd operations for the subtask stage.
// k/v: sourceID, task -> the running stage of the subtask.
func putSubTaskStageOp(stages ...Stage) ([]clientv3.Op, error) {
	ops := make([]clientv3.Op, 0, len(stages))
	for _, stage := range stages {
		value, err := stage.toJSON()
		if err != nil {
			return ops, err
		}
		key := common.StageSubTaskKeyAdapter.Encode(stage.Source, stage.Task)
		ops = append(ops, clientv3.OpPut(key, value))
	}
	return ops, nil
}

// deleteRelayStageOp returns a DELETE etcd operation for the relay stage.
func deleteRelayStageOp(source string) clientv3.Op {
	return clientv3.OpDelete(common.StageRelayKeyAdapter.Encode(source))
}

// deleteSubTaskStageOp returns a list of DELETE etcd operation for the subtask stage.
func deleteSubTaskStageOp(stages ...Stage) []clientv3.Op {
	ops := make([]clientv3.Op, 0, len(stages))
	for _, stage := range stages {
		ops = append(ops, clientv3.OpDelete(common.StageSubTaskKeyAdapter.Encode(stage.Source, stage.Task)))
	}
	return ops
}
