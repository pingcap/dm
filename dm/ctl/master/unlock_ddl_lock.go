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
	"fmt"
	"os"

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"

	"github.com/spf13/cobra"
)

// NewUnlockDDLLockCmd creates a UnlockDDLLock command
func NewUnlockDDLLockCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "unlock-ddl-lock <lock-ID>",
		Short: "Unlocks DDL lock forcefully.",
		RunE:  unlockDDLLockFunc,
	}
	cmd.Flags().StringP("owner", "o", "", "source to replace the default owner")
	cmd.Flags().BoolP("force-remove", "f", false, "force to remove DDL lock")
	return cmd
}

// unlockDDLLockFunc does unlock DDL lock
func unlockDDLLockFunc(cmd *cobra.Command, _ []string) (err error) {
	if len(cmd.Flags().Args()) != 1 {
		cmd.SetOut(os.Stdout)
		cmd.Usage()
		err = errors.New("please check output to see error")
		return
	}
	owner, err := cmd.Flags().GetString("owner")
	if err != nil {
		common.PrintLines("error in parse `--owner`")
		return
	}

	lockID := cmd.Flags().Arg(0)

	sources, _ := common.GetSourceArgs(cmd)
	if len(sources) > 0 {
		fmt.Println("shoud not specify any sources")
		err = errors.New("please check output to see error")
		return
	}

	forceRemove, err := cmd.Flags().GetBool("force-remove")
	if err != nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli := common.MasterClient()
	resp, err := cli.UnlockDDLLock(ctx, &pb.UnlockDDLLockRequest{
		ID:           lockID,
		ReplaceOwner: owner,
		ForceRemove:  forceRemove,
	})
	if err != nil {
		common.PrintLines("can not unlock DDL lock %s", lockID)
		return
	}

	common.PrettyPrintResponse(resp)
	return
}
