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

// NewResumeTaskCmd creates a ResumeTask command
func NewResumeTaskCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "resume-task [-s source ...] <task-name>",
		Short: "resume a specified paused task",
		Run:   resumeTaskFunc,
	}
	return cmd
}

// resumeTaskFunc does resume task request
func resumeTaskFunc(cmd *cobra.Command, _ []string) {
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

	resp, err := common.OperateTask(pb.TaskOp_Resume, name, sources)
	if err != nil {
		common.PrintLines("can not resume task %s:\n%v", name, errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
