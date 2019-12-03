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

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
)

// NewShowLeaderCmd creates a CheckTask command
func NewShowLeaderCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "show-master-leader",
		Short: "show the DM-master's leader information",
		Run:   showLeaderFunc,
	}
	return cmd
}

// showLeaderFunc does show master leader request
func showLeaderFunc(cmd *cobra.Command, _ []string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// show master leader
	cli := common.MasterClient()
	resp, err := cli.ShowMasterLeader(ctx, &pb.ShowMasterLeaderRequest{})
	if err != nil {
		common.PrintLines("fail to show master leader:\n%v", errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
