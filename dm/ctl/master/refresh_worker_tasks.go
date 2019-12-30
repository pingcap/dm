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
	"github.com/pingcap/failpoint"
	"os"

	dmcommon "github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
)

// NewRefreshWorkerTasks creates a RefreshWorkerTasks command
func NewRefreshWorkerTasks() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "refresh-worker-tasks",
		Short: "refresh worker -> tasks mapper",
		Run:   refreshWorkerTasksFunc,
	}
	return cmd
}

// refreshWorkerTasksFunc does refresh workerTasks request
func refreshWorkerTasksFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) > 0 {
		cmd.SetOut(os.Stdout)
		cmd.Usage()
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli := dmcommon.MasterClient()
	resp, err := cli.RefreshWorkerTasks(ctx, &pb.RefreshWorkerTasksRequest{})
	failpoint.Inject("RefreshWorkerTasksFailed", func(_ failpoint.Value) {
		err = errors.New("call RefreshWorkerTasks failed")
	})
	if err != nil {
		common.PrintLines("can not refresh workerTasks:\n%v", errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
