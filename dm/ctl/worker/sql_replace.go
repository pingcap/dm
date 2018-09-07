// Copyright 2018 PingCAP, Inc.
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

package worker

import (
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-enterprise-tools/dm/ctl/common"
	"github.com/pingcap/tidb-enterprise-tools/dm/pb"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
)

// NewSQLReplaceCmd creates a SQLReplace command
func NewSQLReplaceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sql-replace <sub_task_name> <binlog_pos> <sql1;sql2;>",
		Short: "sql-replace replaces sql in specific binlog_pos with other sqls, each sql must ends with semicolon;",
		Run:   sqlReplaceFunc,
	}
	return cmd
}

func sqlReplaceFunc(cmd *cobra.Command, _ []string) {
	subTaskName := cmd.Flags().Arg(0)
	if strings.TrimSpace(subTaskName) == "" {
		common.PrintLines("sub_task_name is empty")
		return
	}
	binlogPos := cmd.Flags().Arg(1)
	if err := common.CheckBinlogPos(binlogPos); err != nil {
		common.PrintLines("check binlog pos err %v", err)
	}

	extraArgs := cmd.Flags().Args()[2:]
	realSQLs, err := common.ExtractSQLsFromArgs(extraArgs)
	if err != nil {
		common.PrintLines("check sqls err %s", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli := common.WorkerClient()
	resp, err := cli.HandleSQLs(ctx, &pb.HandleSubTaskSQLsRequest{
		Name:      subTaskName,
		Op:        pb.SQLOp_REPLACE,
		BinlogPos: binlogPos,
		Args:      realSQLs,
	})
	if err != nil {
		common.PrintLines("%s can not replace sql:\n%v", subTaskName, errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
