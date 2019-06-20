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

	"github.com/pingcap/errors"
	"google.golang.org/grpc"

	"github.com/pingcap/dm/dm/pb"
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
func NewGRPCClient(addr string) (*GRPCClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBackoffMaxDelay(3*time.Second))
	if err != nil {
		return nil, errors.Trace(err)
	}
	return NewGRPCClientWrap(conn, pb.NewWorkerClient(conn))
}

// SendRequest implements Client.SendRequest
func (c *GRPCClient) SendRequest(ctx context.Context, req *Request, timeout time.Duration) (*Response, error) {
	if atomic.LoadInt32(&c.closed) != 0 {
		return nil, errors.New("send request on a closed client")
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
		return errors.Annotatef(err, "close rpc client")
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
	case CmdQueryTaskOperation:
		resp.QueryTaskOperation, err = client.QueryTaskOperation(ctx, req.QueryTaskOperation)
	case CmdQueryWorkerConfig:
		resp.QueryWorkerConfig, err = client.QueryWorkerConfig(ctx, req.QueryWorkerConfig)
	case CmdHandleSubTaskSQLs:
		resp.HandleSubTaskSQLs, err = client.HandleSQLs(ctx, req.HandleSubTaskSQLs)
	case CmdExecDDL:
		resp.ExecDDL, err = client.ExecuteDDL(ctx, req.ExecDDL)
	case CmdBreakDDLLock:
		resp.BreakDDLLock, err = client.BreakDDLLock(ctx, req.BreakDDLLock)
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
	case CmdFetchDDLInfo:
		resp.FetchDDLInfo, err = client.FetchDDLInfo(ctx)
	default:
		return nil, errors.Errorf("invalid request type: %v", req.Type)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}
