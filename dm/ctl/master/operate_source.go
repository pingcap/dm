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

	"github.com/spf13/cobra"
)

// NewOperateSourceCmd creates a OperateSource command
func NewOperateSourceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "operate-source <operate-type> <config-file> [config-file ...]",
		Short: "create/update/stop upstream MySQL/MariaDB source",
		Run:   operateSourceFunc,
	}
	return cmd
}

func convertCmdType(t string) pb.SourceOp {
	switch t {
	case "create":
		return pb.SourceOp_StartSource
	case "update":
		return pb.SourceOp_UpdateSource
	case "stop":
		return pb.SourceOp_StopSource
	default:
		return pb.SourceOp_InvalidSourceOp
	}
}

// operateMysqlFunc does migrate relay request
func operateSourceFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) < 2 {
		cmd.SetOut(os.Stdout)
		cmd.Usage()
		return
	}

	cmdType := cmd.Flags().Arg(0)
	op := convertCmdType(cmdType)
	if op == pb.SourceOp_InvalidSourceOp {
		common.PrintLines("invalid operate '%s' on worker", cmdType)
		return
	}

	contents := make([]string, len(cmd.Flags().Args())-1)
	for i := 1; i < len(cmd.Flags().Args()); i++ {
		configFile := cmd.Flags().Arg(i)
		content, err := common.GetFileContent(configFile)
		if err != nil {
			common.PrintLines("get file content error:\n%v", err)
			return
		}
		contents[i-1] = string(content)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli := common.MasterClient()
	resp, err := cli.OperateSource(ctx, &pb.OperateSourceRequest{
		Config: contents,
		Op:     op,
	})
	if err != nil {
		common.PrintLines("can not update task:\n%v", err)
		return
	}

	common.PrettyPrintResponse(resp)
}
