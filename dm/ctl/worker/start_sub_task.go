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
	"golang.org/x/net/context"
)

// NewStartSubTaskCmd creates a StartSubTask command
func NewStartSubTaskCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start-sub-task <config_file>",
		Short: "start a sub task with config file",
		Run:   startSubTaskFunc,
	}
	return cmd
}

// startSubTaskFunc does start sub task request
func startSubTaskFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		fmt.Println(cmd.UsageString())
		return
	}

	content, err := common.GetFileContent(args[0])
	if err != nil {
		common.PrintLines("get file content error:\n%v", errors.ErrorStack(err))
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = checkSubTask(ctx, string(content))
	if err != nil {
		common.PrintLines("precheck failed %s", errors.ErrorStack(err))
		return
	}

	cli := common.WorkerClient()
	resp, err := cli.StartSubTask(ctx, &pb.StartSubTaskRequest{Task: string(content)})
	if err != nil {
		common.PrintLines("can not start sub task:\n%v", errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
