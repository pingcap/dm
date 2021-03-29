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
	WorkerRegisterKeyAdapter KeyAdapter = keyHexEncoderDecoderV2("/dm-worker/v2/r/")
	// WorkerKeepAliveKeyAdapter is used to encode and decode keepalive key.
	// k/v: Encode(worker-name) -> time
	WorkerKeepAliveKeyAdapter KeyAdapter = keyHexEncoderDecoderV2("/dm-worker/v2/a/")
	// UpstreamConfigKeyAdapter stores all config of which MySQL-task has not stopped.
	// k/v: Encode(source-id) -> config
	UpstreamConfigKeyAdapter KeyAdapter = keyHexEncoderDecoderV2("/dm-master/v2/upstream/config/")
	// UpstreamBoundWorkerKeyAdapter is used to store address of worker in which MySQL-tasks which are running.
	// k/v: Encode(worker-name) -> the bound relationship.
	UpstreamBoundWorkerKeyAdapter KeyAdapter = keyHexEncoderDecoderV2("/dm-master/v2/bound-worker/")
	// UpstreamLastBoundWorkerKeyAdapter is used to store address of worker in which MySQL-tasks which are running.
	// different with UpstreamBoundWorkerKeyAdapter, this kv should not be deleted when unbound, to provide a priority
	// k/v: Encode(worker-name) -> the bound relationship.
	UpstreamLastBoundWorkerKeyAdapter KeyAdapter = keyHexEncoderDecoderV2("/dm-master/v2/last-bound-worker/")
	// UpstreamRelayWorkerKeyAdapter is used to store the upstream which this worker needs to pull relay log
	// k/v: Encode(worker-name) -> source-id
	UpstreamRelayWorkerKeyAdapter KeyAdapter = keyHexEncoderDecoderV2("/dm-master/v2/relay-worker/")
	// TaskConfigKeyAdapter is used to store task config string.
	// k/v: Encode(task-name) -> task-config-string
	TaskConfigKeyAdapter KeyAdapter = keyHexEncoderDecoderV2("/dm-master/v2/task/")
	// UpstreamSubTaskKeyAdapter is used to store SubTask which are subscribing data from MySQL source.
	// k/v: Encode(source-id, task-name) -> SubTaskConfig
	UpstreamSubTaskKeyAdapter KeyAdapter = keyHexEncoderDecoderV2("/dm-master/v2/upstream/subtask/")
	// StageRelayKeyAdapter is used to store the running stage of the relay.
	// k/v: Encode(source-id) -> the running stage of the relay.
	StageRelayKeyAdapter KeyAdapter = keyHexEncoderDecoderV2("/dm-master/v2/stage/relay/")
	// StageSubTaskKeyAdapter is used to store the running stage of the subtask.
	// k/v: Encode(source-id, task-name) -> the running stage of the subtask.
	StageSubTaskKeyAdapter KeyAdapter = keyHexEncoderDecoderV2("/dm-master/v2/stage/subtask/")

	// ShardDDLPessimismInfoKeyAdapter is used to store shard DDL info in pessimistic model.
	// k/v: Encode(task-name, source-id) -> shard DDL info
	ShardDDLPessimismInfoKeyAdapter KeyAdapter = keyHexEncoderDecoderV2("/dm-master/v2/shardddl-pessimism/info/")
	// ShardDDLPessimismOperationKeyAdapter is used to store shard DDL operation in pessimistic model.
	// k/v: Encode(task-name, source-id) -> shard DDL operation
	ShardDDLPessimismOperationKeyAdapter KeyAdapter = keyHexEncoderDecoderV2("/dm-master/v2/shardddl-pessimism/operation/")

	// ShardDDLOptimismSourceTablesKeyAdapter is used to store INITIAL upstream schema & table names when starting the subtask.
	// In other words, if any Info for this subtask exists, we should obey source tables in the Info.
	// This is because the current upstream tables may not match the tables that the binlog stream has reached.
	// k/v: Encode(task-name, source-id) -> upstream schema & table names.
	ShardDDLOptimismSourceTablesKeyAdapter KeyAdapter = keyHexEncoderDecoderV2("/dm-master/v2/shardddl-optimism/source-tables/")
	// ShardDDLOptimismInfoKeyAdapter is used to store shard DDL info in optimistic model.
	// k/v: Encode(task-name, source-id, upstream-schema-name, upstream-table-name) -> shard DDL info.
	ShardDDLOptimismInfoKeyAdapter KeyAdapter = keyHexEncoderDecoderV2("/dm-master/v2/shardddl-optimism/info/")
	// ShardDDLOptimismOperationKeyAdapter is used to store shard DDL operation in optimistic model.
	// k/v: Encode(task-name, source-id, upstream-schema-name, upstream-table-name) -> shard DDL operation.
	ShardDDLOptimismOperationKeyAdapter KeyAdapter = keyHexEncoderDecoderV2("/dm-master/v2/shardddl-optimism/operation/")
	// ShardDDLOptimismInitSchemaKeyAdapter is used to store the initial schema (before constructed the lock) of merged tables.
	// k/v: Encode(task-name, downstream-schema-name, downstream-table-name) -> table schema.
	ShardDDLOptimismInitSchemaKeyAdapter KeyAdapter = keyHexEncoderDecoderV2("/dm-master/v2/shardddl-optimism/init-schema/")
	// ShardDDLOptimismDroppedColumnsKeyAdapter is used to store the columns that are not fully dropped
	// k/v: Encode(task-name, downstream-schema-name, downstream-table-name, column-name, source-id, upstream-schema-name, upstream-table-name) -> empty
	// If we don't identify different upstream tables, we may report an error for tb2 in the following case.
	// Time series: (+a/-a means add/drop column a)
	//	    older ----------------> newer
	// tb1: +a +b +c           -c
	// tb2:                       +a +b +c
	// tb3:          +a +b +c
	ShardDDLOptimismDroppedColumnsKeyAdapter KeyAdapter = keyHexEncoderDecoderV2("/dm-master/v2/shardddl-optimism/dropped-columns/")
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
	case ShardDDLOptimismDroppedColumnsKeyAdapter:
		return 7
	// used in upgrading
	case WorkerRegisterKeyAdapterV1, UpstreamConfigKeyAdapterV1, UpstreamBoundWorkerKeyAdapterV1,
		WorkerKeepAliveKeyAdapterV1, StageRelayKeyAdapterV1, TaskConfigKeyAdapterV1,
		UpstreamLastBoundWorkerKeyAdapterV1, UpstreamRelayWorkerKeyAdapterV1:
		return 1
	case UpstreamSubTaskKeyAdapterV1, StageSubTaskKeyAdapterV1,
		ShardDDLPessimismInfoKeyAdapterV1, ShardDDLPessimismOperationKeyAdapterV1,
		ShardDDLOptimismSourceTablesKeyAdapterV1:
		return 2
	case ShardDDLOptimismInitSchemaKeyAdapterV1:
		return 3
	case ShardDDLOptimismInfoKeyAdapterV1, ShardDDLOptimismOperationKeyAdapterV1:
		return 4
	case ShardDDLOptimismDroppedColumnsKeyAdapterV1:
		return 7
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

// version V2 fixed the bug that XXDecoder.Encode("s1") is the prefix of XXDecoder.Encode("s10"), so watching prefix
// gets unexpected KV
// also, always use keyHexEncoderDecoderV2 to avoid `/` in keys
type keyHexEncoderDecoderV2 string

func quotes(s string) string {
	return "`" + s + "`"
}

func unquotes(s string) string {
	return s[1 : len(s)-1]
}

func (s keyHexEncoderDecoderV2) Encode(keys ...string) string {
	t := []string{string(s)}
	for _, key := range keys {
		// compatible for non-quoted elements when path.Join
		if key == "" {
			continue
		}
		t = append(t, quotes(hex.EncodeToString([]byte(key))))
	}
	return path.Join(t...)
}

func (s keyHexEncoderDecoderV2) Decode(key string) ([]string, error) {
	v := strings.Split(strings.TrimPrefix(key, string(s)), "/")
	if l := keyAdapterKeysLen(s); l != len(v) {
		return nil, terror.ErrDecodeEtcdKeyFail.Generate(fmt.Sprintf("decoder is %s, the key is %s", string(s), key))
	}
	for i, k := range v {
		dec, err := hex.DecodeString(unquotes(k))
		if err != nil {
			return nil, terror.ErrDecodeEtcdKeyFail.Generate(err.Error())
		}
		v[i] = string(dec)
	}
	return v, nil
}

func (s keyHexEncoderDecoderV2) Path() string {
	return string(s)
}

// used in upgrading
var (
	// WorkerRegisterKeyAdapterV1 is used to encode and decode register key.
	// k/v: Encode(worker-name) -> the information of the DM-worker node.
	WorkerRegisterKeyAdapterV1 KeyAdapter = keyHexEncoderDecoder("/dm-worker/r/")
	// WorkerKeepAliveKeyAdapterV1 is used to encode and decode keepalive key.
	// k/v: Encode(worker-name) -> time
	WorkerKeepAliveKeyAdapterV1 KeyAdapter = keyHexEncoderDecoder("/dm-worker/a/")
	// UpstreamConfigKeyAdapterV1 stores all config of which MySQL-task has not stopped.
	// k/v: Encode(source-id) -> config
	UpstreamConfigKeyAdapterV1 KeyAdapter = keyEncoderDecoder("/dm-master/upstream/config/")
	// UpstreamBoundWorkerKeyAdapterV1 is used to store address of worker in which MySQL-tasks which are running.
	// k/v: Encode(worker-name) -> the bound relationship.
	UpstreamBoundWorkerKeyAdapterV1 KeyAdapter = keyHexEncoderDecoder("/dm-master/bound-worker/")
	// UpstreamLastBoundWorkerKeyAdapterV1 is used to store address of worker in which MySQL-tasks which are running.
	// different with UpstreamBoundWorkerKeyAdapterV1, this kv should not be deleted when unbound, to provide a priority
	// k/v: Encode(worker-name) -> the bound relationship.
	UpstreamLastBoundWorkerKeyAdapterV1 KeyAdapter = keyHexEncoderDecoder("/dm-master/last-bound-worker/")
	// UpstreamRelayWorkerKeyAdapterV1 is used to store the upstream which this worker needs to pull relay log
	// k/v: Encode(worker-name) -> source-id
	UpstreamRelayWorkerKeyAdapterV1 KeyAdapter = keyHexEncoderDecoder("/dm-master/relay-worker/")
	// TaskConfigKeyAdapterV1 is used to store task config string.
	// k/v: Encode(task-name) -> task-config-string
	TaskConfigKeyAdapterV1 KeyAdapter = keyHexEncoderDecoder("/dm-master/task/")
	// UpstreamSubTaskKeyAdapterV1 is used to store SubTask which are subscribing data from MySQL source.
	// k/v: Encode(source-id, task-name) -> SubTaskConfig
	UpstreamSubTaskKeyAdapterV1 KeyAdapter = keyHexEncoderDecoder("/dm-master/upstream/subtask/")
	// StageRelayKeyAdapterV1 is used to store the running stage of the relay.
	// k/v: Encode(source-id) -> the running stage of the relay.
	StageRelayKeyAdapterV1 KeyAdapter = keyEncoderDecoder("/dm-master/stage/relay/")
	// StageSubTaskKeyAdapterV1 is used to store the running stage of the subtask.
	// k/v: Encode(source-id, task-name) -> the running stage of the subtask.
	StageSubTaskKeyAdapterV1 KeyAdapter = keyHexEncoderDecoder("/dm-master/stage/subtask/")

	// ShardDDLPessimismInfoKeyAdapterV1 is used to store shard DDL info in pessimistic model.
	// k/v: Encode(task-name, source-id) -> shard DDL info
	ShardDDLPessimismInfoKeyAdapterV1 KeyAdapter = keyHexEncoderDecoder("/dm-master/shardddl-pessimism/info/")
	// ShardDDLPessimismOperationKeyAdapterV1 is used to store shard DDL operation in pessimistic model.
	// k/v: Encode(task-name, source-id) -> shard DDL operation
	ShardDDLPessimismOperationKeyAdapterV1 KeyAdapter = keyHexEncoderDecoder("/dm-master/shardddl-pessimism/operation/")

	// ShardDDLOptimismSourceTablesKeyAdapterV1 is used to store INITIAL upstream schema & table names when starting the subtask.
	// In other words, if any Info for this subtask exists, we should obey source tables in the Info.
	// This is because the current upstream tables may not match the tables that the binlog stream has reached.
	// k/v: Encode(task-name, source-id) -> upstream schema & table names.
	ShardDDLOptimismSourceTablesKeyAdapterV1 KeyAdapter = keyHexEncoderDecoder("/dm-master/shardddl-optimism/source-tables/")
	// ShardDDLOptimismInfoKeyAdapterV1 is used to store shard DDL info in optimistic model.
	// k/v: Encode(task-name, source-id, upstream-schema-name, upstream-table-name) -> shard DDL info.
	ShardDDLOptimismInfoKeyAdapterV1 KeyAdapter = keyHexEncoderDecoder("/dm-master/shardddl-optimism/info/")
	// ShardDDLOptimismOperationKeyAdapterV1 is used to store shard DDL operation in optimistic model.
	// k/v: Encode(task-name, source-id, upstream-schema-name, upstream-table-name) -> shard DDL operation.
	ShardDDLOptimismOperationKeyAdapterV1 KeyAdapter = keyHexEncoderDecoder("/dm-master/shardddl-optimism/operation/")
	// ShardDDLOptimismInitSchemaKeyAdapterV1 is used to store the initial schema (before constructed the lock) of merged tables.
	// k/v: Encode(task-name, downstream-schema-name, downstream-table-name) -> table schema.
	ShardDDLOptimismInitSchemaKeyAdapterV1 KeyAdapter = keyHexEncoderDecoder("/dm-master/shardddl-optimism/init-schema/")
	// ShardDDLOptimismDroppedColumnsKeyAdapterV1 is used to store the columns that are not fully dropped
	// k/v: Encode(task-name, downstream-schema-name, downstream-table-name, column-name, source-id, upstream-schema-name, upstream-table-name) -> empty
	// If we don't identify different upstream tables, we may report an error for tb2 in the following case.
	// Time series: (+a/-a means add/drop column a)
	//	    older ----------------> newer
	// tb1: +a +b +c           -c
	// tb2:                       +a +b +c
	// tb3:          +a +b +c
	ShardDDLOptimismDroppedColumnsKeyAdapterV1 KeyAdapter = keyHexEncoderDecoder("/dm-master/shardddl-optimism/dropped-columns/")
)
