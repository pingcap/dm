// Copyright 2020 PingCAP, Inc.
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

	"github.com/spf13/cobra"

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/terror"
)

// NewOperateLeaderCmd creates a OperateLeader command
func NewOperateLeaderCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "operate-leader <operate-type>",
		Short: "operate-type can be 'evict' or 'cancel-evict', used to evict leader or cancel evict",
		Run:   operateLeaderFunc,
	}
	return cmd
}

func convertOpType(op string) pb.LeaderOp {
	switch op {
	case "evict":
		return pb.LeaderOp_EvictLeaderOp
	case "cancel-evict":
		return pb.LeaderOp_CancelEvictLeaderOp
	default:
		return pb.LeaderOp_InvalidLeaderOp
	}
}

// operateLeaderFunc does operate leader request
func operateLeaderFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) != 1 {
		cmd.SetOut(os.Stdout)
		cmd.Usage()
		return
	}

	opType := cmd.Flags().Arg(0)

	op := convertOpType(opType)
	if op == pb.LeaderOp_InvalidLeaderOp {
		common.PrintLines("invalid operate '%s' on leader", opType)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// operate leader
	cli := common.MasterClient()
	resp, err := cli.OperateLeader(ctx, &pb.OperateLeaderRequest{
		Op: op,
	})
	if err != nil {
		common.PrintLines("fail to operate leader:\n%v", terror.Message(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
