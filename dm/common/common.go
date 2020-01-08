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
	"path"
	"strings"
)

var (
	useOfClosedErrMsg = "use of closed network connection"
	// WorkerRegisterKeyAdapter used to encode and decode register key.
	// k/v: Encode(addr) -> name
	WorkerRegisterKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-worker/r/")
	// WorkerKeepAliveKeyAdapter used to encode and decode keepalive key.
	// k/v: Encode(addr,name) -> time
	WorkerKeepAliveKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-worker/a/")
	// UpstreamConfigKeyAdapter store all config of which MySQL-task has not stopped.
	// k/v: Encode(source-id) -> config
	UpstreamConfigKeyAdapter KeyAdapter = keyEncoderDecoder("/dm-master/upstream/config/")
	// UpstreamBoundWorkerKeyAdapter used to store address of worker in which MySQL-tasks which are running.
	// k/v: Encode(addr) -> source-id
	UpstreamBoundWorkerKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/bound-worker/")
	// UpstreamSubTaskKeyAdapter used to store SubTask which are subscribing data from MySQL source.
	// k/v: Encode(source-id, task-name) -> SubTaskConfig
	UpstreamSubTaskKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/upstream/subtask/")

	// ShardDDLPessimismInfoKeyAdapter used to store shard DDL info in pessimistic model.
	// k/v: Encode(task-name, source-id) -> shard DDL info
	ShardDDLPessimismInfoKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/shardddl-pessimism/info/")
	// ShardDDLPessimismOperationKeyAdapter used to store shard DDL lock in pessimistic model.
	// k/v: Encode(task-name, source-id) -> shard DDL operation
	ShardDDLPessimismOperationKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/shardddl-pessimism/operation/")
)

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
	Decode(key string) []string
	Path() string
}

type keyEncoderDecoder string
type keyHexEncoderDecoder string

func (s keyEncoderDecoder) Encode(keys ...string) string {
	t := []string{string(s)}
	t = append(t, keys...)
	return path.Join(t...)
}

func (s keyEncoderDecoder) Decode(key string) []string {
	v := strings.TrimPrefix(key, string(s))
	return strings.Split(v, "/")
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

func (s keyHexEncoderDecoder) Decode(key string) []string {
	v := strings.Split(strings.TrimPrefix(key, string(s)), "/")
	for i, k := range v {
		dec, err := hex.DecodeString(k)
		if err != nil {
			panic(err)
		}
		v[i] = string(dec)
	}
	return v
}

func (s keyHexEncoderDecoder) Path() string {
	return string(s)
}
