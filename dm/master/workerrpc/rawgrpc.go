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

package workerrpc

import (
	"context"
	"sync/atomic"
	"time"

	toolutils "github.com/pingcap/tidb-tools/pkg/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/terror"
)

// GRPCClient stores raw grpc connection and worker client
type GRPCClient struct {
	conn   *grpc.ClientConn
	client pb.WorkerClient
	closed int32
}

// NewGRPCClientWrap initializes a new grpc client from given grpc connection and worker client
func NewGRPCClientWrap(conn *grpc.ClientConn, client pb.WorkerClient) (*GRPCClient, error) {
	return &GRPCClient{
		conn:   conn,
		client: client,
		closed: 0,
	}, nil
}

// NewGRPCClient initializes a new grpc client from worker address
func NewGRPCClient(addr string, securityCfg config.Security) (*GRPCClient, error) {
	tls, err := toolutils.NewTLS(securityCfg.SSLCA, securityCfg.SSLCert, securityCfg.SSLKey, addr, securityCfg.CertAllowedCN)
	if err != nil {
		return nil, terror.ErrMasterGRPCCreateConn.Delegate(err)
	}

	conn, err := grpc.Dial(addr, tls.ToGRPCDialOption(), grpc.WithBackoffMaxDelay(3*time.Second),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  100 * time.Millisecond,
				Multiplier: 1.6, // Default
				Jitter:     0.2, // Default
				MaxDelay:   3 * time.Second,
			},
			MinConnectTimeout: 3 * time.Second,
		}))
	if err != nil {
		return nil, terror.ErrMasterGRPCCreateConn.Delegate(err)
	}
	return NewGRPCClientWrap(conn, pb.NewWorkerClient(conn))
}

// SendRequest implements Client.SendRequest
func (c *GRPCClient) SendRequest(ctx context.Context, req *Request, timeout time.Duration) (*Response, error) {
	if atomic.LoadInt32(&c.closed) != 0 {
		return nil, terror.ErrMasterGRPCSendOnCloseConn.Generate()
	}
	if req.IsStreamAPI() {
		// call stream API and returns a grpc stream client
		return callRPC(ctx, c.client, req)
	}
	// call normal grpc request with a timeout
	ctx1, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return callRPC(ctx1, c.client, req)
}

// Close implements Client.Close
func (c *GRPCClient) Close() error {
	defer func() {
		atomic.CompareAndSwapInt32(&c.closed, 0, 1)
		c.conn = nil
	}()
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	if err != nil {
		return terror.ErrMasterGRPCClientClose.Delegate(err)
	}
	return nil
}

func callRPC(ctx context.Context, client pb.WorkerClient, req *Request) (*Response, error) {
	resp := &Response{}
	resp.Type = req.Type
	var err error
	switch req.Type {
	case CmdStartSubTask:
		resp.StartSubTask, err = client.StartSubTask(ctx, req.StartSubTask)
	case CmdOperateSubTask:
		resp.OperateSubTask, err = client.OperateSubTask(ctx, req.OperateSubTask)
	case CmdUpdateSubTask:
		resp.UpdateSubTask, err = client.UpdateSubTask(ctx, req.UpdateSubTask)
	case CmdQueryStatus:
		resp.QueryStatus, err = client.QueryStatus(ctx, req.QueryStatus)
	case CmdQueryError:
		resp.QueryError, err = client.QueryError(ctx, req.QueryError)
	case CmdQueryWorkerConfig:
		resp.QueryWorkerConfig, err = client.QueryWorkerConfig(ctx, req.QueryWorkerConfig)
	case CmdSwitchRelayMaster:
		resp.SwitchRelayMaster, err = client.SwitchRelayMaster(ctx, req.SwitchRelayMaster)
	case CmdOperateRelay:
		resp.OperateRelay, err = client.OperateRelay(ctx, req.OperateRelay)
	case CmdPurgeRelay:
		resp.PurgeRelay, err = client.PurgeRelay(ctx, req.PurgeRelay)
	case CmdUpdateRelay:
		resp.UpdateRelay, err = client.UpdateRelayConfig(ctx, req.UpdateRelay)
	case CmdMigrateRelay:
		resp.MigrateRelay, err = client.MigrateRelay(ctx, req.MigrateRelay)
	case CmdOperateSchema:
		resp.OperateSchema, err = client.OperateSchema(ctx, req.OperateSchema)
	case CmdOperateV1Meta:
		resp.OperateV1Meta, err = client.OperateV1Meta(ctx, req.OperateV1Meta)
	case CmdHandleError:
		resp.HandleError, err = client.HandleError(ctx, req.HandleError)
	default:
		return nil, terror.ErrMasterGRPCInvalidReqType.Generate(req.Type)
	}
	if err != nil {
		return nil, terror.ErrMasterGRPCRequestError.Delegate(err)
	}
	return resp, nil
}
