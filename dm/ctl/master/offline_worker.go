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
	"os"

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
)

// NewOfflineWorkerCmd creates an OfflineWorker command
func NewOfflineWorkerCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "offline-worker <name> <address>",
		Short: "offline worker which has been closed",
		Run:   offlineWorkerFunc,
	}
	return cmd
}

// offlineWorkerFunc does migrate relay request
func offlineWorkerFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) != 2 {
		cmd.SetOut(os.Stdout)
		cmd.Usage()
		return
	}

	name := cmd.Flags().Arg(0)
	addr := cmd.Flags().Arg(1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli := common.MasterClient()
	resp, err := cli.OfflineWorker(ctx, &pb.OfflineWorkerRequest{
		Name:    name,
		Address: addr,
	})
	if err != nil {
		common.PrintLines("offline worker failed, error:\n%v", errors.ErrorStack(err))
		return
	}
	if !resp.Result {
		common.PrintLines("offline worker failed:\n%v", resp.Msg)
	}
}
