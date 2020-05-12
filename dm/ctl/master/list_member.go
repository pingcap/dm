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

package master

import (
	"context"
	"os"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
)

// NewListMemberCmd creates an ListMember command
func NewListMemberCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list-member master/worker/leader",
		Short: "list member information of master/worker/leader",
		Run:   listMemberFunc,
	}
	return cmd
}

func convertMemberType(t string) pb.MemberType {
	switch t {
	case "master":
		return pb.MemberType_MasterType
	case "worker":
		return pb.MemberType_WorkerType
	case "leader":
		return pb.MemberType_LeaderType
	default:
		return pb.MemberType_InvalidType
	}
}

// listMemberFunc does list member request
func listMemberFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) != 1 {
		cmd.SetOut(os.Stdout)
		cmd.Usage()
		return
	}

	memberType := cmd.Flags().Arg(0)
	member := convertMemberType(memberType)
	if member == pb.MemberType_InvalidType {
		common.PrintLines("invalid arg '%s'", memberType)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli := common.MasterClient()
	var resp proto.Message
	var err error
	switch member {
	case pb.MemberType_MasterType:
		resp, err = cli.ListMemberMaster(ctx, &pb.ListMemberRequest{})
	case pb.MemberType_WorkerType:
		resp, err = cli.ListMemberWorker(ctx, &pb.ListMemberRequest{})
	case pb.MemberType_LeaderType:
		resp, err = cli.ListMemberLeader(ctx, &pb.ListMemberRequest{})
	}

	if err != nil {
		common.PrintLines("list member failed, error:\n%v", errors.ErrorStack(err))
		return
	}
	common.PrettyPrintResponse(resp)
}
