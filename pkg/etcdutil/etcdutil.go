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

// learn from https://github.com/pingcap/pd/blob/v3.0.5/pkg/etcdutil/etcdutil.go.

package etcdutil

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/pingcap/errors"
	"go.etcd.io/etcd/clientv3"
	v3rpc "go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
)

const (
	// DefaultDialTimeout is the maximum amount of time a dial will wait for a
	// connection to setup. 30s is long enough for most of the network conditions.
	DefaultDialTimeout = 30 * time.Second

	// DefaultRequestTimeout 10s is long enough for most of etcd clusters.
	DefaultRequestTimeout = 10 * time.Second
)

// CreateClient creates an etcd client with some default config items.
func CreateClient(endpoints []string, tlsCfg *tls.Config) (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: DefaultDialTimeout,
		TLS:         tlsCfg,
	})
}

// ListMembers returns a list of internal etcd members.
func ListMembers(client *clientv3.Client) (*clientv3.MemberListResponse, error) {
	ctx, cancel := context.WithTimeout(client.Ctx(), DefaultRequestTimeout)
	defer cancel()
	return client.MemberList(ctx)
}

// AddMember adds an etcd member.
func AddMember(client *clientv3.Client, peerAddrs []string) (*clientv3.MemberAddResponse, error) {
	ctx, cancel := context.WithTimeout(client.Ctx(), DefaultRequestTimeout)
	defer cancel()
	return client.MemberAdd(ctx, peerAddrs)
}

// DoOpsInOneTxn do multiple etcd operations in one txn.
func DoOpsInOneTxn(cli *clientv3.Client, ops ...clientv3.Op) (*clientv3.TxnResponse, int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Txn(ctx).Then(ops...).Commit()
	if err != nil {
		return nil, 0, err
	}
	return resp, resp.Header.Revision, nil
}

// IsRetryableError check whether error is retryable error for etcd to build again
func IsRetryableError(err error) bool {
	switch errors.Cause(err) {
	case v3rpc.ErrCompacted, v3rpc.ErrNoLeader, v3rpc.ErrNoSpace:
		return true
	default:
		return false
	}
}
