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

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
)

// NewQueryStatusCmd creates a QueryStatus command
func NewQueryStatusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "query-status [-w worker ...] [task_name]",
		Short: "query task's status",
		Run:   queryStatusFunc,
	}
	return cmd
}

// queryStatusFunc does query task's status
func queryStatusFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) > 1 {
		fmt.Println(cmd.Usage())
		return
	}
	taskName := cmd.Flags().Arg(0) // maybe empty

	workers, err := common.GetWorkerArgs(cmd)
	if err != nil {
		common.PrintLines("%s", errors.ErrorStack(err))
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli := common.MasterClient()
	resp, err := cli.QueryStatus(ctx, &pb.QueryStatusListRequest{
		Name:    taskName,
		Workers: workers,
	})
	if err != nil {
		common.PrintLines("can not query %s task's status(in workers %v):\n%s", taskName, workers, errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
