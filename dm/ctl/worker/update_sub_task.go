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

package worker

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
)

// NewUpdateSubTaskCmd creates a UpdateSubTask command
func NewUpdateSubTaskCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update-sub-task <config_file>",
		Short: "update a sub task's config for routes, filters, column-mappings, black-white-list",
		Run:   updateSubTaskFunc,
	}
	return cmd
}

// updateSubTaskFunc does update sub task request
func updateSubTaskFunc(cmd *cobra.Command, args []string) {
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

	// NOTE: do whole check now, refine to do TablesChecker and ShardingTablesCheck ?
	err = checkSubTask(ctx, string(content))
	if err != nil {
		common.PrintLines("precheck failed %s", errors.ErrorStack(err))
		return
	}

	cli := common.WorkerClient()
	resp, err := cli.UpdateSubTask(ctx, &pb.UpdateSubTaskRequest{Task: string(content)})
	if err != nil {
		common.PrintLines("can not update sub task:\n%v", errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
