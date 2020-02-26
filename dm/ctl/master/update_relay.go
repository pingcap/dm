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
	"os"

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
)

// NewUpdateRelayCmd creates a UpdateRelay command
func NewUpdateRelayCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update-relay [-s source ...] <config-file>",
		Short: "update the relay unit config of the DM-worker",
		Run:   updateRelayFunc,
	}
	return cmd
}

// updateRealyFunc does update relay request
func updateRelayFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) != 1 {
		cmd.SetOut(os.Stdout)
		cmd.Usage()
		return
	}

	content, err := common.GetFileContent(cmd.Flags().Arg(0))
	if err != nil {
		common.PrintLines("get file content error:\n%v", errors.ErrorStack(err))
		return
	}

	sources, _ := common.GetSourceArgs(cmd)
	if len(sources) != 1 {
		fmt.Println("must specify one source (`-s` / `--source`)")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli := common.MasterClient()
	resp, err := cli.UpdateWorkerRelayConfig(ctx, &pb.UpdateWorkerRelayConfigRequest{
		Config: string(content),
		Source: sources[0],
	})

	if err != nil {
		common.PrintLines("can not update relay config:\n%v", errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
