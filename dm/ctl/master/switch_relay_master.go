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
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
)

// NewSwitchRelayMasterCmd creates a SwitchRelayMaster command
func NewSwitchRelayMasterCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "switch-relay-master <-w worker ...>",
		Short: "switch the master server of the DM-worker's relay unit",
		Run:   switchRelayMasterFunc,
	}
	return cmd
}

// switchRelayMasterFunc does switch relay master server
func switchRelayMasterFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) > 0 {
		cmd.SetOut(os.Stdout)
		cmd.Usage()
		return
	}

	workers, err := common.GetWorkerArgs(cmd)
	if err != nil {
		common.PrintLines("%v", err)
		return
	}
	if len(workers) == 0 {
		fmt.Println("must specify at least one DM-worker (`-w` / `--worker`)")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli := common.MasterClient()
	resp, err := cli.SwitchWorkerRelayMaster(ctx, &pb.SwitchWorkerRelayMasterRequest{
		Workers: workers,
	})
	if err != nil {
		common.PrintLines("can not switch relay's master server (in workers %v):\n%v", workers, err)
		return
	}

	common.PrettyPrintResponse(resp)
}
