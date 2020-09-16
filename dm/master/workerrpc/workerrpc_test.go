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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/tikv/pd/pkg/tempurl"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/pbmock"
	"github.com/pingcap/dm/pkg/terror"
)

var _ = Suite(&testWorkerRPCSuite{})

type testWorkerRPCSuite struct {
}

func TestWorkerRPC(t *testing.T) {
	TestingT(t)
}

func (t *testWorkerRPCSuite) TestGRPCClient(c *C) {
	timeout := 3 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	// get a random address for DM-worker
	addr := tempurl.Alloc()[len("http://"):]
	// NOTE: we don't wait for the gRPC connection establish now, in other words no need to wait for the DM-worker instance become online.
	rpcCli, err := NewGRPCClient(addr, config.Security{})
	c.Assert(err, IsNil)

	// replace the underlying DM-worker client.
	workerCli := pbmock.NewMockWorkerClient(ctrl)
	rpcCli.client = workerCli

	reqs := []*Request{
		{
			Type:         CmdStartSubTask,
			StartSubTask: &pb.StartSubTaskRequest{Task: "invalid task content"},
		},
		{
			Type:           CmdOperateSubTask,
			OperateSubTask: &pb.OperateSubTaskRequest{Op: pb.TaskOp_Pause},
		},
		{
			Type:          CmdUpdateSubTask,
			UpdateSubTask: &pb.UpdateSubTaskRequest{Task: "another invalid task content"},
		},
		{
			Type:        CmdQueryStatus,
			QueryStatus: &pb.QueryStatusRequest{Name: "test"},
		},
		{
			Type:       CmdQueryError,
			QueryError: &pb.QueryErrorRequest{Name: "test-2"},
		},
		{
			Type:              CmdQueryWorkerConfig,
			QueryWorkerConfig: &pb.QueryWorkerConfigRequest{},
		},
		{
			Type:              CmdSwitchRelayMaster,
			SwitchRelayMaster: &pb.SwitchRelayMasterRequest{},
		},
		{
			Type:         CmdOperateRelay,
			OperateRelay: &pb.OperateRelayRequest{Op: pb.RelayOp_ResumeRelay},
		},
		{
			Type:       CmdPurgeRelay,
			PurgeRelay: &pb.PurgeRelayRequest{Inactive: true},
		},
		{
			Type:        CmdUpdateRelay,
			UpdateRelay: &pb.UpdateRelayRequest{Content: "invalid relay content"},
		},
		{
			Type:         CmdMigrateRelay,
			MigrateRelay: &pb.MigrateRelayRequest{BinlogName: "mysql-bin.000123"},
		},
		{
			Type:          CmdOperateSchema,
			OperateSchema: &pb.OperateWorkerSchemaRequest{Op: pb.SchemaOp_SetSchema},
		},
		{
			Type:          CmdOperateV1Meta,
			OperateV1Meta: &pb.OperateV1MetaRequest{Op: pb.V1MetaOp_RemoveV1Meta},
		},
		{
			Type:        CmdHandleError,
			HandleError: &pb.HandleWorkerErrorRequest{Op: pb.ErrorOp_Replace},
		},
	}

	workerCli.EXPECT().StartSubTask(gomock.Any(), reqs[0].StartSubTask)
	workerCli.EXPECT().OperateSubTask(gomock.Any(), reqs[1].OperateSubTask)
	workerCli.EXPECT().UpdateSubTask(gomock.Any(), reqs[2].UpdateSubTask)
	workerCli.EXPECT().QueryStatus(gomock.Any(), reqs[3].QueryStatus)
	workerCli.EXPECT().QueryError(gomock.Any(), reqs[4].QueryError)
	workerCli.EXPECT().QueryWorkerConfig(gomock.Any(), reqs[5].QueryWorkerConfig)
	workerCli.EXPECT().SwitchRelayMaster(gomock.Any(), reqs[6].SwitchRelayMaster)
	workerCli.EXPECT().OperateRelay(gomock.Any(), reqs[7].OperateRelay)
	workerCli.EXPECT().PurgeRelay(gomock.Any(), reqs[8].PurgeRelay)
	workerCli.EXPECT().UpdateRelayConfig(gomock.Any(), reqs[9].UpdateRelay)
	workerCli.EXPECT().MigrateRelay(gomock.Any(), reqs[10].MigrateRelay)
	workerCli.EXPECT().OperateSchema(gomock.Any(), reqs[11].OperateSchema)
	workerCli.EXPECT().OperateV1Meta(gomock.Any(), reqs[12].OperateV1Meta)
	workerCli.EXPECT().HandleError(gomock.Any(), reqs[13].HandleError)

	// others cmds are not supported.
	// NOTE: update the end cmd in the below `for` loop when adding new cmds.
OUTER:
	for cmd := CmdStartSubTask; cmd <= CmdHandleError; cmd++ {
		for _, req := range reqs {
			if req.Type == cmd {
				// supported cmd
				_, err = rpcCli.SendRequest(ctx, req, timeout)
				c.Assert(err, IsNil)
				continue OUTER
			}
		}
		_, err = rpcCli.SendRequest(ctx, &Request{Type: cmd}, timeout)
		c.Assert(terror.ErrMasterGRPCInvalidReqType.Equal(err), IsTrue)
	}

	// got an error from the underlying RPC.
	err2 := errors.New("mock error")
	workerCli.EXPECT().StartSubTask(gomock.Any(), reqs[0].StartSubTask).Return(nil, err2)
	_, err = rpcCli.SendRequest(ctx, reqs[0], timeout)
	c.Assert(terror.ErrMasterGRPCRequestError.Equal(err), IsTrue)
	c.Assert(errors.Cause(err), Equals, err2)

	// close the cli.
	c.Assert(rpcCli.Close(), IsNil)

	// can't send request any more.
	_, err = rpcCli.SendRequest(ctx, reqs[0], timeout)
	c.Assert(terror.ErrMasterGRPCSendOnCloseConn.Equal(err), IsTrue)
}
