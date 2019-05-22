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
	"fmt"

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
)

// NewStopTaskCmd creates a StopTask command
func NewStopTaskCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stop-task [-w worker ...] <task_name>",
		Short: "stop a task with name",
		Run:   stopTaskFunc,
	}
	return cmd
}

// stopTaskFunc does stop task request
func stopTaskFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) != 1 {
		fmt.Println(cmd.Usage())
		return
	}
	name := cmd.Flags().Arg(0)

	workers, err := common.GetWorkerArgs(cmd)
	if err != nil {
		common.PrintLines("%s", errors.ErrorStack(err))
		return
	}

	resp, err := common.OperateTask(pb.TaskOp_Stop, name, workers)
	if err != nil {
		common.PrintLines("can not stop task %s:\n%v", name, errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
