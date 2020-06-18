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
	"strings"

	"github.com/spf13/cobra"

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
)

// NewSQLInjectCmd creates a SQLInject command
func NewSQLInjectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sql-inject <-w worker> <task-name> <sql1;sql2;>",
		Short: "inject (limited) SQLs into binlog replication unit as binlog events",
		Run:   sqlInjectFunc,
	}
	return cmd
}

// sqlInjectFunc does sql inject request
func sqlInjectFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) < 2 {
		cmd.SetOut(os.Stdout)
		cmd.Usage()
		return
	}

	workers, err := common.GetWorkerArgs(cmd)
	if err != nil {
		common.PrintLines("%v", err)
		return
	}
	if len(workers) != 1 {
		common.PrintLines("want only one worker, but got %v", workers)
		return
	}

	taskName := cmd.Flags().Arg(0)
	if strings.TrimSpace(taskName) == "" {
		common.PrintLines("task-name is empty")
		return
	}

	extraArgs := cmd.Flags().Args()[1:]
	realSQLs, err := common.ExtractSQLsFromArgs(extraArgs)
	if err != nil {
		common.PrintLines("check sqls err %v", err)
		return
	}
	for _, sql := range realSQLs {
		isDDL, err2 := common.IsDDL(sql)
		if err2 != nil {
			common.PrintLines("check sql err %v", err2)
			return
		}
		if !isDDL {
			common.PrintLines("only support inject DDL currently, but got '%s'", sql)
			return
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli := common.MasterClient()
	resp, err := cli.HandleSQLs(ctx, &pb.HandleSQLsRequest{
		Name:   taskName,
		Op:     pb.SQLOp_INJECT,
		Args:   realSQLs,
		Worker: workers[0],
	})
	if err != nil {
		common.PrintLines("can not inject sql:\n%v", err)
		return
	}

	common.PrettyPrintResponse(resp)
}
