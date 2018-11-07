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

package master

import (
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-enterprise-tools/dm/ctl/common"
	"github.com/pingcap/tidb-enterprise-tools/dm/pb"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
)

// NewSQLSkipCmd creates a SQLSkip command
func NewSQLSkipCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sql-skip <-w worker> <task_name> <binlog_pos>",
		Short: "sql-skip skips specified binlog position",
		Run:   sqlSkipFunc,
	}
	return cmd
}

// sqlSkipFunc does sql skip request
func sqlSkipFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) != 2 {
		fmt.Println(cmd.Usage())
		return
	}
	workers, err := common.GetWorkerArgs(cmd)
	if err != nil {
		common.PrintLines("%s", errors.ErrorStack(err))
		return
	}
	if len(workers) != 1 {
		common.PrintLines("want only one worker, but got %v", workers)
		return
	}

	taskname := cmd.Flags().Arg(0)
	if strings.TrimSpace(taskname) == "" {
		common.PrintLines("taskname is empty")
		return
	}
	binlogPos := cmd.Flags().Arg(1)
	if err2 := common.CheckBinlogPos(binlogPos); err2 != nil {
		common.PrintLines("check binlog pos err %v", err2)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli := common.MasterClient()

	resp, err := cli.HandleSQLs(ctx, &pb.HandleSQLsRequest{
		Name:      taskname,
		Op:        pb.SQLOp_SKIP,
		BinlogPos: binlogPos,
		Worker:    workers[0],
	})
	if err != nil {
		common.PrintLines("can not skip sql:\n%v", errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
