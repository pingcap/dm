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
	"errors"
	"os"

	"github.com/spf13/cobra"

	"github.com/pingcap/dm/dm/command"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
)

// NewHandleErrorCmd creates a HandleError command
func NewHandleErrorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "handle-error <task-name | task-file> [-s source ...] [-b binlog-pos] <skip/replace/revert> [replace-sql1;replace-sql2;]",
		Short: "`skip`/`replace`/`revert` the current error event or a specific binlog position (binlog-pos) event.",
		RunE:  handleErrorFunc,
	}
	cmd.Flags().StringP("binlog-pos", "b", "", "position used to match binlog event if matched the handler-error operation will be applied. The format like \"mysql-bin|000001.000003:3270\"")
	return cmd
}

func convertOp(t string) pb.ErrorOp {
	switch t {
	case "skip":
		return pb.ErrorOp_Skip
	case "replace":
		return pb.ErrorOp_Replace
	case "revert":
		return pb.ErrorOp_Revert
	default:
		return pb.ErrorOp_InvalidErrorOp
	}
}

// handleErrorFunc does handle error request
func handleErrorFunc(cmd *cobra.Command, _ []string) (err error) {
	if len(cmd.Flags().Args()) < 2 {
		cmd.SetOut(os.Stdout)
		cmd.Usage()
		err = errors.New("please check output to see error")
		return
	}

	taskName := common.GetTaskNameFromArgOrFile(cmd.Flags().Arg(0))
	operation := cmd.Flags().Arg(1)
	var sqls []string

	op := convertOp(operation)
	switch op {
	case pb.ErrorOp_Skip, pb.ErrorOp_Revert:
		if len(cmd.Flags().Args()) > 2 {
			common.PrintLines("replace-sqls can not be used for 'skip/revert' operation")
			err = errors.New("please check output to see error")
			return
		}
	case pb.ErrorOp_Replace:
		if len(cmd.Flags().Args()) <= 2 {
			common.PrintLines("must specify the replace-sqls for replace operation")
			err = errors.New("please check output to see error")
			return
		}

		sqls, err = common.ExtractSQLsFromArgs(cmd.Flags().Args()[2:])
		if err != nil {
			return
		}
	default:
		common.PrintLines("invalid operation '%s', please use `skip`, `replace` or `revert`", operation)
		err = errors.New("please check output to see error")
		return
	}

	binlogPos, err := cmd.Flags().GetString("binlog-pos")
	if err != nil {
		return
	}
	if len(binlogPos) != 0 {
		_, err = command.VerifyBinlogPos(binlogPos)
		if err != nil {
			return
		}
	}

	sources, err := common.GetSourceArgs(cmd)
	if err != nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli := common.MasterClient()

	resp, err := cli.HandleError(ctx, &pb.HandleErrorRequest{
		Op:        op,
		Task:      taskName,
		BinlogPos: binlogPos,
		Sqls:      sqls,
		Sources:   sources,
	})
	if err != nil {
		return
	}

	common.PrettyPrintResponse(resp)
	return
}
