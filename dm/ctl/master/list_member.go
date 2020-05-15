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

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
)

var (
	listMemberFlags = ListMemberFlags{}
)

// ListMemberFlags are flags that used in ListMember command
type ListMemberFlags struct {
	names []string // specify names to list information
}

// Reset clears cache of ListMemberFlags
func (c ListMemberFlags) Reset() {
	c.names = c.names[:0]
}

// NewListMemberCmd creates an ListMember command
func NewListMemberCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list-member [--type leader/master/worker] [--name master-name/worker-name ...]",
		Short: "list member information",
		Run:   listMemberFunc,
	}
	cmd.Flags().StringP("type", "t", "", "specify a member type from master, worker, and leader")
	cmd.Flags().StringSliceVarP(&listMemberFlags.names, "name", "n", []string{}, "specify member names in choosing type")
	return cmd
}

func convertListMemberType(t string) pb.MemberType {
	switch t {
	case "master":
		return pb.MemberType_MasterType
	case "worker":
		return pb.MemberType_WorkerType
	case "leader":
		return pb.MemberType_LeaderType
	case "":
		return pb.MemberType_AllType
	default:
		return pb.MemberType_InvalidType
	}
}

// listMemberFunc does list member request
func listMemberFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) != 0 {
		cmd.SetOut(os.Stdout)
		cmd.Usage()
		return
	}

	member, err := cmd.Flags().GetString("type")
	if err != nil {
		common.PrintLines("%s", errors.ErrorStack(err))
		return
	}

	memType := convertListMemberType(member)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli := common.MasterClient()
	resp, err := cli.ListMember(ctx, &pb.ListMemberRequest{
		MemType: memType,
		Names:   listMemberFlags.names,
	})

	if err != nil {
		common.PrintLines("list member failed, error:\n%v", errors.ErrorStack(err))
		return
	}
	common.PrettyPrintResponse(resp)
}
