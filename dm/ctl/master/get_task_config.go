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
)

// NewGetTaskCfgCmd creates a getTaskCfg command
func NewGetTaskCfgCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get-task-config [task-name]",
		Short: "get task config",
		Run:   getTaskCfgFunc,
	}
	return cmd
}

// getTaskCfgFunc does get task's config
func getTaskCfgFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) != 1 {
		cmd.SetOut(os.Stdout)
		cmd.Usage()
		return
	}
	taskName := cmd.Flags().Arg(0)

	cli := common.MasterClient()
	ctx, cancel := context.WithTimeout(context.Background(), common.GlobalConfig().RPCTimeout)
	defer cancel()

	resp, err := cli.GetTaskCfg(ctx, &pb.GetTaskCfgRequest{
		Name: taskName,
	})
	if err != nil {
		common.PrintLines("can not get config of task %s:\n%v", taskName, err)
		return
	}
	common.PrettyPrintResponse(resp)
}
