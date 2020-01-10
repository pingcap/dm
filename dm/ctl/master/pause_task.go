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
	"os"

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
)

// NewPauseTaskCmd creates a PauseTask command
func NewPauseTaskCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pause-task [-s source ...] <task-name>",
		Short: "pause a specified running task",
		Run:   pauseTaskFunc,
	}
	return cmd
}

// pauseTaskFunc does pause task request
func pauseTaskFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) != 1 {
		cmd.SetOut(os.Stdout)
		cmd.Usage()
		return
	}
	name := cmd.Flags().Arg(0)

	sources, err := common.GetSourceArgs(cmd)
	if err != nil {
		common.PrintLines("%s", errors.ErrorStack(err))
		return
	}

	resp, err := common.OperateTask(pb.TaskOp_Pause, name, sources)
	if err != nil {
		common.PrintLines("can not pause task %s:\n%v", name, errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
