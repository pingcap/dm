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

// NewBreakDDLLockCmd creates a BreakDDLLock command
func NewBreakDDLLockCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "break-ddl-lock <-w worker ...> <task-name> [--remove-id] [--exec] [--skip]",
		Short: "force to break dm-worker's DDL lock",
		Run:   breakDDLLockFunc,
	}
	cmd.Flags().StringP("remove-id", "i", "", "DDLLockInfo's ID which need to remove")
	cmd.Flags().BoolP("exec", "e", false, "whether execute DDL which is blocking")
	cmd.Flags().BoolP("skip", "s", false, "whether skip DDL which in blocking")
	return cmd
}

// breakDDLLockFunc does break DDL lock
func breakDDLLockFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) != 1 {
		fmt.Println(cmd.Usage())
		return
	}
	taskName := cmd.Flags().Arg(0)

	workers, err := common.GetWorkerArgs(cmd)
	if err != nil {
		fmt.Println(errors.ErrorStack(err))
		return
	}
	if len(workers) == 0 {
		fmt.Println("must specify at least one dm-worker (`-w` / `--worker`)")
		return
	}

	removeLockID, err := cmd.Flags().GetString("remove-id")
	if err != nil {
		fmt.Println(errors.ErrorStack(err))
		return
	}

	exec, err := cmd.Flags().GetBool("exec")
	if err != nil {
		fmt.Println(errors.ErrorStack(err))
		return
	}

	skip, err := cmd.Flags().GetBool("skip")
	if err != nil {
		fmt.Println(errors.ErrorStack(err))
		return
	}

	if len(removeLockID) == 0 && !exec && !skip {
		fmt.Println("`remove-id`, `exec`, `skip` must specify at least one")
		return
	}

	if exec && skip {
		fmt.Println("`exec` and `skip` can not specify both at the same time")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli := common.MasterClient()
	resp, err := cli.BreakWorkerDDLLock(ctx, &pb.BreakWorkerDDLLockRequest{
		Workers:      workers,
		Task:         taskName,
		RemoveLockID: removeLockID,
		ExecDDL:      exec,
		SkipDDL:      skip,
	})
	if err != nil {
		common.PrintLines("can not break DDL lock (in workers %v):\n%s", workers, errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
