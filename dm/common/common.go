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
	WorkerRegisterKeyAdapter keyHexEncoderDecoder = "/dm-worker/r/"
	// WorkerKeepAliveKeyAdapter used to encode and decode keepalive key.
	// k/v: Encode(addr,name) -> time
	WorkerKeepAliveKeyAdapter keyHexEncoderDecoder = "/dm-worker/a/"
	// UpstreamConfigKeyAdapter the config path of upstream.
	// k/v: Encode(source-id) -> config
	UpstreamConfigKeyAdapter keyEncoderDecoder = "/dm-master/upstream/config/"
	// UpstreamBoundWorkerKeyAdapter the path of worker relationship.
	// k/v: Encode(addr) -> source-id
	UpstreamBoundWorkerKeyAdapter keyHexEncoderDecoder = "/dm-master/bound-worker/"
	// UpstreamSubTaskKeyAdapter the path of the subtask.
	// k/v: Encode(addr) -> config
	UpstreamSubTaskKeyAdapter keyHexEncoderDecoder = "/dm-master/upstream/subtask/"
)

// IsErrNetClosing checks whether is an ErrNetClosing error
func IsErrNetClosing(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), useOfClosedErrMsg)
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
