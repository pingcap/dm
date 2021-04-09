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
	"net"
	"path"
	"strings"
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
			want:    "/dm-master/v2/upstream/config/6d7973716c31",
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
		{
			keys:    []string{"test", "target_db", "target_table"},
			adapter: ShardDDLOptimismInitSchemaKeyAdapter,
			want:    "/dm-master/shardddl-optimism/init-schema/74657374/7461726765745f6462/7461726765745f7461626c65",
		},
		{
			keys:    []string{"test", "mysql_replica_01", "target_db", "target_table"},
			adapter: ShardDDLOptimismInfoKeyAdapter,
			want:    "/dm-master/shardddl-optimism/info/74657374/6d7973716c5f7265706c6963615f3031/7461726765745f6462/7461726765745f7461626c65",
		},
		{
			keys:    []string{"mysql/01"},
			adapter: StageRelayKeyAdapter,
			want:    "/dm-master/v2/stage/relay/6d7973716c2f3031",
		},
		{
			keys:    []string{"mysql1", "中文1🀄️"},
			adapter: UpstreamSubTaskKeyAdapter,
			want:    "/dm-master/upstream/subtask/6d7973716c31/e4b8ade6968731f09f8084efb88f",
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

func (t *testCommon) TestEncodeAsPrefix(c *C) {
	testCases := []struct {
		keys    []string
		adapter KeyAdapter
		want    string
	}{
		{
			keys:    []string{"mysql1"},
			adapter: UpstreamSubTaskKeyAdapter,
			want:    "/dm-master/upstream/subtask/6d7973716c31/",
		},
		{
			keys:    []string{"test", "target_db"},
			adapter: ShardDDLOptimismInitSchemaKeyAdapter,
			want:    "/dm-master/shardddl-optimism/init-schema/74657374/7461726765745f6462/",
		},
	}

	for _, ca := range testCases {
		encKey := ca.adapter.Encode(ca.keys...)
		c.Assert(encKey, Equals, ca.want)
		_, err := ca.adapter.Decode(encKey)
		c.Assert(err, NotNil)
	}

	keys := []string{"test", "target_db", "target_table"}
	fullEncodedKey := ShardDDLOptimismInitSchemaKeyAdapter.Encode(keys...)
	prefixEncodedKey := ShardDDLOptimismInitSchemaKeyAdapter.Encode(keys[:len(keys)-1]...)
	c.Assert(strings.HasPrefix(fullEncodedKey, prefixEncodedKey), IsTrue)

	keys2 := []string{"test", "target_db_2", "target_table_2"}
	fullEncodedKey2 := ShardDDLOptimismInitSchemaKeyAdapter.Encode(keys2...)
	prefixEncodedKey2 := ShardDDLOptimismInitSchemaKeyAdapter.Encode(keys2[:len(keys2)-1]...)

	c.Assert(strings.HasPrefix(fullEncodedKey, prefixEncodedKey2), IsFalse)
	c.Assert(strings.HasPrefix(fullEncodedKey2, prefixEncodedKey), IsFalse)
}

func (t *testCommon) TestIsErrNetClosing(c *C) {
	server, err := net.Listen("tcp", "localhost:0")
	c.Assert(err, IsNil)
	err = server.Close()
	c.Assert(IsErrNetClosing(err), IsFalse)
	_, err = server.Accept()
	c.Assert(IsErrNetClosing(err), IsTrue)
}

func (t *testCommon) TestJoinUseSlash(c *C) {
	// because we use "/" in Encode
	c.Assert(path.Join("a", "b"), Equals, "a/b")
}
