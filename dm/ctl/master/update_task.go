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

// NewUpdateTaskCmd creates a UpdateTask command
func NewUpdateTaskCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update-task [-s source ...] <config-file>",
		Short: "update a task's config for routes, filters, or block-allow-list",
		RunE:  updateTaskFunc,
	}
	return cmd
}

// updateTaskFunc does update task request
func updateTaskFunc(cmd *cobra.Command, _ []string) (err error) {
	if len(cmd.Flags().Args()) != 1 {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		err = errors.New("please check output to see error")
		return
	}
	content, err := common.GetFileContent(cmd.Flags().Arg(0))
	if err != nil {
		return
	}

	sources, err := common.GetSourceArgs(cmd)
	if err != nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// update task
	resp := &pb.UpdateTaskResponse{}
	err = common.SendRequest(
		ctx,
		"UpdateTask",
		&pb.UpdateTaskRequest{
			Task:    string(content),
			Sources: sources,
		},
		&resp,
	)

	if err != nil {
		return
	}

	if !common.PrettyPrintResponseWithCheckTask(resp, checker.ErrorMsgHeader) {
		common.PrettyPrintResponse(resp)
	}
	return
}
