// Copyright 2020 PingCAP, Inc.
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
	"os"

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/shardddl/pessimism"

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
)

// NewSetDDLLockModeCmd creates a SetDDLLockMode command
func NewSetDDLLockModeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set-ddl-lock-mode <mode>",
		Short: "switch between optimistic and pessimistic modes for new DDL locks",
		Run:   setDDLLockModeFunc,
	}
	return cmd
}

// breakDDLLockFunc does break DDL lock
func setDDLLockModeFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) != 1 {
		cmd.SetOut(os.Stdout)
		cmd.Usage()
		return
	}

	var lockMode pessimism.LockMode
	switch lockModeStr := cmd.Flags().Arg(0); lockModeStr {
	case "optimistic":
		lockMode = pessimism.LockModeOptimistic
	case "pessimistic":
		lockMode = pessimism.LockModePessimistic
	default:
		fmt.Println("unknown lock mode", lockModeStr)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli := common.MasterClient()
	resp, err := cli.SetDDLLockMode(ctx, &pb.SetDDLLockModeRequest{
		LockMode: uint32(lockMode),
	})
	if err != nil {
		common.PrintLines("cannot change lock mode:\n%s", errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
