// Copyright 2018 PingCAP, Inc.
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

package worker

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/spf13/cobra"
)

// NewStopSubTaskCmd creates a StopSubTask command
// refine it to talk to dm-master later
func NewStopSubTaskCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stop-sub-task <sub_task_name>",
		Short: "stop a running sub task with name",
		Run:   stopSubTaskFunc,
	}
	return cmd
}

// stopSubTaskFunc does stop sub task request
func stopSubTaskFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		fmt.Println(cmd.Usage())
		return
	}
	name := args[0]

	resp, err := operateSubTask(pb.TaskOp_Stop, name)
	if err != nil {
		common.PrintLines("can not stop sub task %s:\n%v", name, errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
