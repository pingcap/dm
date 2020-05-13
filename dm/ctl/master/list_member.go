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

// NewListMemberCmd creates an ListMember command
func NewListMemberCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list-member [--master] [--worker] [--leader]",
		Short: "list member information",
		Run:   listMemberFunc,
	}
	cmd.Flags().BoolP("master", "", false, "only to list master")
	cmd.Flags().BoolP("worker", "", false, "only to list worker")
	cmd.Flags().BoolP("leader", "", false, "only to list leader")
	return cmd
}

func extractListMemberFlag(cmd *cobra.Command) (bool, bool, bool, error) {
	master, err := cmd.Flags().GetBool("master")
	if err != nil {
		return false, false, false, errors.Trace(err)
	}

	worker, err := cmd.Flags().GetBool("worker")
	if err != nil {
		return false, false, false, errors.Trace(err)
	}

	leader, err := cmd.Flags().GetBool("leader")
	if err != nil {
		return false, false, false, errors.Trace(err)
	}

	if !master && !worker && !leader {
		master = true
		worker = true
		leader = true
	}
	return master, worker, leader, nil
}

// listMemberFunc does list member request
func listMemberFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) != 0 {
		cmd.SetOut(os.Stdout)
		cmd.Usage()
		return
	}

	master, worker, leader, err := extractListMemberFlag(cmd)
	if err != nil {
		common.PrintLines("%s", err.Error())
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli := common.MasterClient()
	resp, err := cli.ListMember(ctx, &pb.ListMemberRequest{
		Master: master,
		Worker: worker,
		Leader: leader,
	})

	if err != nil {
		common.PrintLines("list member failed, error:\n%v", errors.ErrorStack(err))
		return
	}
	common.PrettyPrintResponse(resp)
}
