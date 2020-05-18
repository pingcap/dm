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
		Use:   "list-member [--leader] [--master] [--worker] [--name master-name/worker-name ...]",
		Short: "list member information",
		Run:   listMemberFunc,
	}
	cmd.Flags().BoolP("leader", "l", false, "only to list leader information")
	cmd.Flags().BoolP("master", "m", false, "only to list master information")
	cmd.Flags().BoolP("worker", "w", false, "only to list worker information")
	cmd.Flags().StringSliceVarP(&listMemberFlags.names, "name", "n", []string{}, "specify member names in choosing type")
	return cmd
}

func convertListMemberType(cmd *cobra.Command) (bool, bool, bool, error) {
	leader, err := cmd.Flags().GetBool("leader")
	if err != nil {
		return false, false, false, err
	}
	master, err := cmd.Flags().GetBool("master")
	if err != nil {
		return false, false, false, err
	}
	worker, err := cmd.Flags().GetBool("worker")
	if err != nil {
		return false, false, false, err
	}
	if !leader && !master && !worker {
		leader = true
		master = true
		worker = true
	}
	return leader, master, worker, nil
}

// listMemberFunc does list member request
func listMemberFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) != 0 {
		cmd.SetOut(os.Stdout)
		cmd.Usage()
		return
	}

	leader, master, worker, err := convertListMemberType(cmd)
	if err != nil {
		common.PrintLines("%s", errors.ErrorStack(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli := common.MasterClient()
	resp, err := cli.ListMember(ctx, &pb.ListMemberRequest{
		Leader: leader,
		Master: master,
		Worker: worker,
		Names:   listMemberFlags.names,
	})

	if err != nil {
		common.PrintLines("list member failed, error:\n%v", errors.ErrorStack(err))
		return
	}
	common.PrettyPrintResponse(resp)
}
