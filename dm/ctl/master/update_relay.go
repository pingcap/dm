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

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
)

// NewUpdateRelayCmd creates a UpdateRelay command
func NewUpdateRelayCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update-relay [-w worker ...] <config_file>",
		Short: "update dm-worker's relay unit configure",
		Run:   updateRelayFunc,
	}
	return cmd
}

// updateRealyFunc does update relay request
func updateRelayFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) != 1 {
		fmt.Println(cmd.Usage())
		return
	}

	content, err := common.GetFileContent(cmd.Flags().Arg(0))
	if err != nil {
		common.PrintLines("get file content error:\n%v", errors.ErrorStack(err))
		return
	}

	workers, _ := common.GetWorkerArgs(cmd)
	if len(workers) != 1 {
		fmt.Println("must specify one dm-worker (`-w` / `--worker`)")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli := common.MasterClient()
	resp, err := cli.UpdateWorkerRelayConfig(ctx, &pb.UpdateWorkerRelayConfigRequest{
		Config: string(content),
		Worker: workers[0],
	})

	if err != nil {
		common.PrintLines("can not update relay config:\n%v", errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
