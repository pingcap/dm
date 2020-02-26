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
	"time"

	"github.com/pingcap/dm/dm/pb"
)

// CmdType represents the concrete request type in Request or response type in Response.
type CmdType uint16

// CmdType values.
const (
	CmdStartSubTask CmdType = 1 + iota
	CmdOperateSubTask
	CmdUpdateSubTask

	CmdQueryStatus
	CmdQueryError
	CmdQueryTaskOperation
	CmdQueryWorkerConfig

	CmdHandleSubTaskSQLs
	CmdExecDDL
	CmdBreakDDLLock

	CmdSwitchRelayMaster
	CmdOperateRelay
	CmdPurgeRelay
	CmdUpdateRelay
	CmdMigrateRelay

	CmdFetchDDLInfo
)

// Request wraps all dm-worker rpc requests.
type Request struct {
	Type CmdType

	StartSubTask   *pb.StartSubTaskRequest
	OperateSubTask *pb.OperateSubTaskRequest
	UpdateSubTask  *pb.UpdateSubTaskRequest

	QueryStatus       *pb.QueryStatusRequest
	QueryError        *pb.QueryErrorRequest
	QueryWorkerConfig *pb.QueryWorkerConfigRequest

	HandleSubTaskSQLs *pb.HandleSubTaskSQLsRequest

	SwitchRelayMaster *pb.SwitchRelayMasterRequest
	OperateRelay      *pb.OperateRelayRequest
	PurgeRelay        *pb.PurgeRelayRequest
	UpdateRelay       *pb.UpdateRelayRequest
	MigrateRelay      *pb.MigrateRelayRequest
}

// Response wraps all dm-worker rpc responses.
type Response struct {
	Type CmdType

	StartSubTask   *pb.CommonWorkerResponse
	OperateSubTask *pb.OperateSubTaskResponse
	UpdateSubTask  *pb.CommonWorkerResponse

	QueryStatus       *pb.QueryStatusResponse
	QueryError        *pb.QueryErrorResponse
	QueryWorkerConfig *pb.QueryWorkerConfigResponse

	HandleSubTaskSQLs *pb.CommonWorkerResponse
	ExecDDL           *pb.CommonWorkerResponse
	BreakDDLLock      *pb.CommonWorkerResponse

	SwitchRelayMaster *pb.CommonWorkerResponse
	OperateRelay      *pb.OperateRelayResponse
	PurgeRelay        *pb.CommonWorkerResponse
	UpdateRelay       *pb.CommonWorkerResponse
	MigrateRelay      *pb.CommonWorkerResponse
}

// Client is a client that sends RPC.
// It should not be used after calling Close().
type Client interface {
	// SendRequest sends Request
	SendRequest(ctx context.Context, req *Request, timeout time.Duration) (*Response, error)

	// Cloase close client and releases all data
	Close() error
}

// IsStreamAPI checks whether a request is streaming API based on CmdType
func (req *Request) IsStreamAPI() bool {
	return req.Type == CmdFetchDDLInfo
}
