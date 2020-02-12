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
	// WorkerRegisterKeyAdapter used to encode and decode register key.
	// k/v: Encode(name) -> the information of the DM-worker node.
	WorkerRegisterKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-worker/r/")
	// WorkerKeepAliveKeyAdapter used to encode and decode keepalive key.
	// k/v: Encode(name) -> time
	WorkerKeepAliveKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-worker/a/")
	// UpstreamConfigKeyAdapter store all config of which MySQL-task has not stopped.
	// k/v: Encode(source-id) -> config
	UpstreamConfigKeyAdapter KeyAdapter = keyEncoderDecoder("/dm-master/upstream/config/")
	// UpstreamBoundWorkerKeyAdapter used to store address of worker in which MySQL-tasks which are running.
	// k/v: Encode(name) -> the bound relationship.
	UpstreamBoundWorkerKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/bound-worker/")
	// UpstreamSubTaskKeyAdapter used to store SubTask which are subscribing data from MySQL source.
	// k/v: Encode(source-id, task-name) -> SubTaskConfig
	UpstreamSubTaskKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/upstream/subtask/")
	// StageRelayKeyAdapter used to store the running stage of the relay.
	// k/v: Encode(source-id) -> the running stage of the relay.
	StageRelayKeyAdapter KeyAdapter = keyEncoderDecoder("/dm-master/stage/relay/")
	// StageSubTaskKeyAdapter used to store the running stage of the subtask.
	// k/v: Encode(source-id, task-name) -> the running stage of the subtask.
	StageSubTaskKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/stage/subtask/")

	// ShardDDLPessimismInfoKeyAdapter used to store shard DDL info in pessimistic model.
	// k/v: Encode(task-name, source-id) -> shard DDL info
	ShardDDLPessimismInfoKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/shardddl-pessimism/info/")
	// ShardDDLPessimismOperationKeyAdapter used to store shard DDL lock in pessimistic model.
	// k/v: Encode(task-name, source-id) -> shard DDL operation
	ShardDDLPessimismOperationKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/shardddl-pessimism/operation/")
)

func keyAdapterKeysLen(s KeyAdapter) int {
	switch s {
	case WorkerRegisterKeyAdapter, UpstreamConfigKeyAdapter, UpstreamBoundWorkerKeyAdapter,
		WorkerKeepAliveKeyAdapter, StageRelayKeyAdapter:
		return 1
	case UpstreamSubTaskKeyAdapter, StageSubTaskKeyAdapter:
		return 2
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

//KeyAdapter used to counstruct etcd key.
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
