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
	"time"

	"github.com/pingcap/errors"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
	v3rpc "go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/retry"
)

const (
	// DefaultDialTimeout is the maximum amount of time a dial will wait for a
	// connection to setup. 30s is long enough for most of the network conditions.
	DefaultDialTimeout = 30 * time.Second

	// DefaultRequestTimeout 10s is long enough for most of etcd clusters.
	DefaultRequestTimeout = 10 * time.Second
)

var etcdDefaultTxnRetryParam = retry.Params{
	RetryCount:         5,
	FirstRetryDuration: time.Second,
	BackoffStrategy:    retry.Stable,
	IsRetryableFn: func(retryTime int, err error) bool {
		// only retry for unavailable or resource exhausted errors
		eErr := rpctypes.Error(err)
		if serverErr, ok := eErr.(rpctypes.EtcdError); ok && serverErr.Code() != codes.Unavailable && serverErr.Code() != codes.ResourceExhausted {
			return false
		}
		ev, ok := status.FromError(err)
		if !ok {
			return false
		}
		return ev.Code() == codes.Unavailable || ev.Code() == codes.ResourceExhausted
	},
}

var etcdDefaultTxnStrategy = retry.FiniteRetryStrategy{}

// CreateClient creates an etcd client with some default config items.
func CreateClient(endpoints []string) (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: DefaultDialTimeout,
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

// DoOpsInOneTxnWithRetry do multiple etcd operations in one txn.
func DoOpsInOneTxnWithRetry(cli *clientv3.Client, ops ...clientv3.Op) (*clientv3.TxnResponse, int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), DefaultRequestTimeout)
	defer cancel()
	tctx := tcontext.NewContext(ctx, log.L())
	ret, _, err := etcdDefaultTxnStrategy.Apply(tctx, etcdDefaultTxnRetryParam, func(t *tcontext.Context) (ret interface{}, err error) {
		resp, err := cli.Txn(ctx).Then(ops...).Commit()
		if err != nil {
			return nil, err
		}
		return resp, nil
	})

	if err != nil {
		return nil, 0, err
	}
	resp := ret.(*clientv3.TxnResponse)
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
