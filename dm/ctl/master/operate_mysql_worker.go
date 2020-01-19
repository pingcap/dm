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
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
	"os"
)

// NewOperateMysqlWorkerCmd creates a OperateMysqlWorker command
func NewOperateMysqlWorkerCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "operate-worker <operate-type> <config-file>",
		Short: "create/update/stop mysql task",
		Run:   operateMysqlWorkerFunc,
	}
	return cmd
}

func convertCmdType(t string) pb.WorkerOp {
	switch t {
	case "create":
		return pb.WorkerOp_StartWorker
	case "update":
		return pb.WorkerOp_UpdateConfig
	case "stop":
		return pb.WorkerOp_StopWorker
	default:
		return pb.WorkerOp_InvalidWorkerOp
	}
}

// operateMysqlFunc does migrate relay request
func operateMysqlWorkerFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) != 2 {
		cmd.SetOut(os.Stdout)
		cmd.Usage()
		return
	}

	cmdType := cmd.Flags().Arg(0)
	configFile := cmd.Flags().Arg(1)
	content, err := common.GetFileContent(configFile)
	if err != nil {
		common.PrintLines("get file content error:\n%v", errors.ErrorStack(err))
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	op := convertCmdType(cmdType)
	if op == pb.WorkerOp_InvalidWorkerOp {
		common.PrintLines("invalid operate %s on worker", cmdType)
		return
	}

	cli := common.MasterClient()
	resp, err := cli.OperateMysqlWorker(ctx, &pb.MysqlWorkerRequest{
		Config: string(content),
		Op:     op,
	})
	if err != nil {
		common.PrintLines("can not update task:\n%v", errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
