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
	"testing"

	. "github.com/pingcap/check"
)

func TestCommon(t *testing.T) {
	TestingT(t)
}

type testCommon struct{}

var _ = Suite(&testCommon{})

func (t *testCommon) TestKeyAdapter(c *C) {
	testCases := []struct {
		keys    []string
		adapter KeyAdapter
		want    string
	}{
		{
			keys:    []string{"127.0.0.1:2382"},
			adapter: WorkerRegisterKeyAdapter,
			want:    "/dm-worker/r/3132372e302e302e313a32333832",
		},
		{
			keys:    []string{"worker1"},
			adapter: WorkerKeepAliveKeyAdapter,
			want:    "/dm-worker/a/776f726b657231",
		},
		{
			keys:    []string{"mysql1"},
			adapter: UpstreamConfigKeyAdapter,
			want:    "/dm-master/upstream/config/mysql1",
		},
		{
			keys:    []string{"127.0.0.1:2382"},
			adapter: UpstreamBoundWorkerKeyAdapter,
			want:    "/dm-master/bound-worker/3132372e302e302e313a32333832",
		},
		{
			keys:    []string{"mysql1", "test"},
			adapter: UpstreamSubTaskKeyAdapter,
			want:    "/dm-master/upstream/subtask/6d7973716c31/74657374",
		},
	}

	for _, ca := range testCases {
		encKey := ca.adapter.Encode(ca.keys...)
		c.Assert(encKey, Equals, ca.want)
		decKey, err := ca.adapter.Decode(encKey)
		c.Assert(err, IsNil)
		c.Assert(decKey, DeepEquals, ca.keys)
	}
}
