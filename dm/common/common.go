// Copyright 2019 PingCAP, Inc.
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

package common

import (
	"encoding/hex"
	"fmt"
	"path"
	"strings"

	"github.com/pingcap/dm/pkg/terror"
)

var (
	useOfClosedErrMsg = "use of closed network connection"
	// ClusterVersionKey is used to store the version of the cluster.
	ClusterVersionKey string = "/dm-cluster/version"
	// WorkerRegisterKeyAdapter is used to encode and decode register key.
	// k/v: Encode(worker-name) -> the information of the DM-worker node.
	WorkerRegisterKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-worker/r/")
	// WorkerKeepAliveKeyAdapter is used to encode and decode keepalive key.
	// k/v: Encode(worker-name) -> time
	WorkerKeepAliveKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-worker/a/")
	// UpstreamConfigKeyAdapter stores all config of which MySQL-task has not stopped.
	// k/v: Encode(source-id) -> config
	UpstreamConfigKeyAdapter KeyAdapter = keyEncoderDecoder("/dm-master/upstream/config/")
	// UpstreamBoundWorkerKeyAdapter is used to store address of worker in which MySQL-tasks which are running.
	// k/v: Encode(worker-name) -> the bound relationship.
	UpstreamBoundWorkerKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/bound-worker/")
	// UpstreamLastBoundWorkerKeyAdapter is used to store address of worker in which MySQL-tasks which are running.
	// different with UpstreamBoundWorkerKeyAdapter, this kv should not be deleted when unbound, to provide a priority
	// k/v: Encode(worker-name) -> the bound relationship.
	UpstreamLastBoundWorkerKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/last-bound-worker/")
	// UpstreamRelayWorkerKeyAdapter is used to store the upstream which this worker needs to pull relay log
	// k/v: Encode(worker-name) -> source-id
	UpstreamRelayWorkerKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/relay-worker/")
	// TaskConfigKeyAdapter is used to store task config string.
	// k/v: Encode(task-name) -> task-config-string
	TaskConfigKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/task/")
	// UpstreamSubTaskKeyAdapter is used to store SubTask which are subscribing data from MySQL source.
	// k/v: Encode(source-id, task-name) -> SubTaskConfig
	UpstreamSubTaskKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/upstream/subtask/")
	// StageRelayKeyAdapter is used to store the running stage of the relay.
	// k/v: Encode(source-id) -> the running stage of the relay.
	StageRelayKeyAdapter KeyAdapter = keyEncoderDecoder("/dm-master/stage/relay/")
	// StageSubTaskKeyAdapter is used to store the running stage of the subtask.
	// k/v: Encode(source-id, task-name) -> the running stage of the subtask.
	StageSubTaskKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/stage/subtask/")

	// ShardDDLPessimismInfoKeyAdapter is used to store shard DDL info in pessimistic model.
	// k/v: Encode(task-name, source-id) -> shard DDL info
	ShardDDLPessimismInfoKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/shardddl-pessimism/info/")
	// ShardDDLPessimismOperationKeyAdapter is used to store shard DDL operation in pessimistic model.
	// k/v: Encode(task-name, source-id) -> shard DDL operation
	ShardDDLPessimismOperationKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/shardddl-pessimism/operation/")

	// ShardDDLOptimismSourceTablesKeyAdapter is used to store INITIAL upstream schema & table names when starting the subtask.
	// In other words, if any Info for this subtask exists, we should obey source tables in the Info.
	// This is because the current upstream tables may not match the tables that the binlog stream has reached.
	// k/v: Encode(task-name, source-id) -> upstream schema & table names.
	ShardDDLOptimismSourceTablesKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/shardddl-optimism/source-tables/")
	// ShardDDLOptimismInfoKeyAdapter is used to store shard DDL info in optimistic model.
	// k/v: Encode(task-name, source-id, upstream-schema-name, upstream-table-name) -> shard DDL info.
	ShardDDLOptimismInfoKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/shardddl-optimism/info/")
	// ShardDDLOptimismOperationKeyAdapter is used to store shard DDL operation in optimistic model.
	// k/v: Encode(task-name, source-id, upstream-schema-name, upstream-table-name) -> shard DDL operation.
	ShardDDLOptimismOperationKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/shardddl-optimism/operation/")
	// ShardDDLOptimismInitSchemaKeyAdapter is used to store the initial schema (before constructed the lock) of merged tables.
	// k/v: Encode(task-name, downstream-schema-name, downstream-table-name) -> table schema.
	ShardDDLOptimismInitSchemaKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/shardddl-optimism/init-schema/")
)

func keyAdapterKeysLen(s KeyAdapter) int {
	switch s {
	case WorkerRegisterKeyAdapter, UpstreamConfigKeyAdapter, UpstreamBoundWorkerKeyAdapter,
		WorkerKeepAliveKeyAdapter, StageRelayKeyAdapter, TaskConfigKeyAdapter,
		UpstreamLastBoundWorkerKeyAdapter, UpstreamRelayWorkerKeyAdapter:
		return 1
	case UpstreamSubTaskKeyAdapter, StageSubTaskKeyAdapter,
		ShardDDLPessimismInfoKeyAdapter, ShardDDLPessimismOperationKeyAdapter,
		ShardDDLOptimismSourceTablesKeyAdapter:
		return 2
	case ShardDDLOptimismInitSchemaKeyAdapter:
		return 3
	case ShardDDLOptimismInfoKeyAdapter, ShardDDLOptimismOperationKeyAdapter:
		return 4

	}
	return -1
}

// IsErrNetClosing checks whether is an ErrNetClosing error
func IsErrNetClosing(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), useOfClosedErrMsg)
}

// KeyAdapter is used to construct etcd key.
type KeyAdapter interface {
	Encode(keys ...string) string
	Decode(key string) ([]string, error)
	Path() string
}

type keyEncoderDecoder string
type keyHexEncoderDecoder string

func (s keyEncoderDecoder) Encode(keys ...string) string {
	t := []string{string(s)}
	t = append(t, keys...)
	return path.Join(t...)
}

func (s keyEncoderDecoder) Decode(key string) ([]string, error) {
	v := strings.TrimPrefix(key, string(s))
	vals := strings.Split(v, "/")
	if l := keyAdapterKeysLen(s); l != len(vals) {
		return nil, terror.ErrDecodeEtcdKeyFail.Generate(fmt.Sprintf("decoder is %s, the key is %s", string(s), key))
	}
	return vals, nil
}

func (s keyEncoderDecoder) Path() string {
	return string(s)
}

func (s keyHexEncoderDecoder) Encode(keys ...string) string {
	t := []string{string(s)}
	for _, key := range keys {
		t = append(t, hex.EncodeToString([]byte(key)))
	}
	return path.Join(t...)
}

func (s keyHexEncoderDecoder) Decode(key string) ([]string, error) {
	v := strings.Split(strings.TrimPrefix(key, string(s)), "/")
	if l := keyAdapterKeysLen(s); l != len(v) {
		return nil, terror.ErrDecodeEtcdKeyFail.Generate(fmt.Sprintf("decoder is %s, the key is %s", string(s), key))
	}
	for i, k := range v {
		dec, err := hex.DecodeString(k)
		if err != nil {
			return nil, terror.ErrDecodeEtcdKeyFail.Generate(err.Error())
		}
		v[i] = string(dec)
	}
	return v, nil
}

func (s keyHexEncoderDecoder) Path() string {
	return string(s)
}
