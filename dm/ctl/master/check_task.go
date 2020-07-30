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
	"errors"
	"os"

	"github.com/spf13/cobra"

	"github.com/pingcap/dm/checker"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
)

// NewCheckTaskCmd creates a CheckTask command
func NewCheckTaskCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "check-task <config-file>",
		Short: "check the config file of the task",
		RunE:   checkTaskFunc,
	}
	return cmd
}

// checkTaskFunc does check task request
func checkTaskFunc(cmd *cobra.Command, _ []string) (err error) {
	if len(cmd.Flags().Args()) != 1 {
		cmd.SetOut(os.Stdout)
		cmd.Usage()
		err = errors.New("dummy error to trigger exit code")
		return
	}
	content, err := common.GetFileContent(cmd.Flags().Arg(0))
	if err != nil {
		common.PrintLines("get file content error:\n%v", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// start task
	cli := common.MasterClient()
	resp, err := cli.CheckTask(ctx, &pb.CheckTaskRequest{
		Task: string(content),
	})
	if err != nil {
		common.PrintLines("fail to check task:\n%v", err)
		return
	}

	if !common.PrettyPrintResponseWithCheckTask(resp, checker.ErrorMsgHeader) {
		common.PrettyPrintResponse(resp)
	}
	return
}
