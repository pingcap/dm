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
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
)

// NewPauseRelayCmd creates a PauseRelay command
func NewPauseRelayCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pause-relay <-s source ...>",
		Short: "Pauses DM-worker's relay unit.",
		RunE:  pauseRelayFunc,
	}
	return cmd
}

// pauseRelayFunc does pause relay request
func pauseRelayFunc(cmd *cobra.Command, _ []string) (err error) {
	if len(cmd.Flags().Args()) > 0 {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		err = errors.New("please check output to see error")
		return
	}

	sources, err := common.GetSourceArgs(cmd)
	if err != nil {
		return
	}
	if len(sources) == 0 {
		fmt.Println("must specify at least one source (`-s` / `--source`)")
		err = errors.New("please check output to see error")
		return
	}

	resp, err := common.OperateRelay(pb.RelayOp_PauseRelay, sources)
	if err != nil {
		return
	}

	common.PrettyPrintResponse(resp)
	return
}
