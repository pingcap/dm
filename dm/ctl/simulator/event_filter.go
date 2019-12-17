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

package simulator

import (
	"context"
	"fmt"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/errors"
	_ "github.com/pingcap/tidb/types/parser_driver" // for import parser driver

	"github.com/spf13/cobra"
)

type eventFilterResult struct {
	WillBeFiltered string `json:"will-be-filtered"`
	FilterName     string `json:"filter-name,omitempty"`
}

// NewEventFilterCmd creates a EventFilter command
func NewEventFilterCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "event-filter <config-file> <sql>",
		Short: "check whether the given sql will be filtered by binlog-event-filter",
		Run:   eventFilterFunc,
	}
	return cmd
}

func eventFilterFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) < 2 {
		fmt.Println(cmd.Usage())
		return
	}
	workers, err := common.GetWorkerArgs(cmd)
	if err != nil {
		common.PrintLines("get workers error:\n%v", errors.ErrorStack(err))
		return
	}
	if len(workers) != 1 {
		common.PrintLines("we want 1 worker, but get %v", workers)
		return
	}

	content, err := common.GetFileContent(cmd.Flags().Arg(0))
	if err != nil {
		common.PrintLines("get file content error:\n%v", errors.ErrorStack(err))
		return
	}
	task := string(content)

	cfg := config.NewTaskConfig()
	err = cfg.Decode(task)
	if err != nil {
		common.PrintLines("decode file content to config error:\n%v", errors.ErrorStack(err))
		return
	}

	extraArgs := cmd.Flags().Args()[1:]
	realSQLs, err := common.ExtractSQLsFromArgs(extraArgs)
	if err != nil {
		common.PrintLines("%s", errors.ErrorStack(err))
		return
	}
	if len(realSQLs) != 1 {
		common.PrintLines("we want 1 sql, but get %v", realSQLs)
		return
	}

	cli := common.MasterClient()
	ctx, cancel := context.WithTimeout(context.Background(), common.GlobalConfig().RPCTimeout)
	defer cancel()
	resp, err := cli.SimulateTask(ctx, &pb.SimulationRequest{
		Op:      pb.SimulateOp_EventFilter,
		Workers: workers,
		Task:    task,
		Sql:     realSQLs[0],
	})

	if err = checkResp(err, resp); err != nil {
		common.PrintLines("get simulation result from dm-master failed:\n%s", err)
		return
	}

	if len(resp.SimulationResults) != 1 {
		common.PrintLines("simulation result's length should be 1, but is %+v", resp.SimulationResults)
		return
	}
	var filtered, reason string
	if len(resp.SimulationResults[0].DoTableMap) > 0 {
		filtered = "no"
		reason = resp.SimulationResults[0].DoTableMap[realSQLs[0]].Reasons[0]
	} else {
		filtered = "yes"
		reason = resp.SimulationResults[0].IgnoreTableMap[realSQLs[0]].Reasons[0]
	}
	common.PrettyPrintInterface(eventFilterResult{
		FilterName:     reason,
		WillBeFiltered: filtered,
	})
}
