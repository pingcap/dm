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

// NewUpdateMasterConfigCmd creates a UpdateMasterConfig command
func NewUpdateMasterConfigCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update-master-config <config-file>",
		Short: "update the config of the DM-master",
		Run:   updateMasterConfigFunc,
	}
	return cmd
}

func updateMasterConfigFunc(cmd *cobra.Command, _ []string) {
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli := common.MasterClient()
	resp, err := cli.UpdateMasterConfig(ctx, &pb.UpdateMasterConfigRequest{
		Config: string(content),
	})
	if err != nil {
		common.PrintLines("can not update master config:\n%v", errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
