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

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/utils"

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
)

type tableRouteResult struct {
	Result         bool                           `json:"result"`
	Msg            string                         `json:"msg"`
	Routes         map[string]map[string][]string `json:"routes,omitempty"`
	WillBeFiltered string                         `json:"will-be-filtered,omitempty"`
	MatchRoute     string                         `json:"match-route,omitempty"`
	TargetSchema   string                         `json:"target-schema,omitempty"`
	TargetTable    string                         `json:"target-table,omitempty"`
}

// NewTableRouteCmd creates a TableRoute command
func NewTableRouteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "table-route [-w worker] [-T table] <config-file>",
		Short: "check the routes for all tables or single table",
		Run:   tableRouteFunc,
	}
	cmd.Flags().StringP("table", "T", "", "the table name we want to check for the table route")
	return cmd
}

func tableRouteFunc(cmd *cobra.Command, _ []string) {
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

	workers, err := common.GetWorkerArgs(cmd)
	if err != nil {
		common.PrintLines(errors.ErrorStack(err))
		return
	}
	if len(workers) > 1 {
		common.PrintLines("we want 0 or 1 worker, but get %v", workers)
		return
	}

	result := &tableRouteResult{
		Result: true,
	}
	cli := common.MasterClient()
	ctx, cancel := context.WithTimeout(context.Background(), common.GlobalConfig().RPCTimeout)
	defer cancel()
	// no worker is specified, print all routes
	if len(workers) == 0 {
		resp, err := cli.SimulateTask(ctx, &pb.SimulationRequest{
			Op:   pb.SimulateOp_TableRoute,
			Task: task,
		})
		if err = checkResp(err, resp); err != nil {
			common.PrintLines("get simulation result from dm-master failed:\n%s", err)
			return
		}

		routes := make(map[string]map[string][]string)
		for _, simulationResult := range resp.SimulationResults {
			for targetTable, sourceTableList := range simulationResult.RouteTableMap {
				for _, sourceTable := range sourceTableList.Tables {
					if routes[targetTable] == nil {
						routes[targetTable] = make(map[string][]string)
					}
					routes[targetTable][simulationResult.SourceIP] = append(routes[targetTable][simulationResult.SourceIP], sourceTable)
				}
			}
		}

		result.Routes = routes
	} else {
		tableName, err := getTableFromCMD(cmd)
		resp, err := cli.SimulateTask(ctx, &pb.SimulationRequest{
			Op:         pb.SimulateOp_TableRoute,
			Worker:     workers[0],
			Task:       task,
			TableQuery: tableName,
		})
		if err = checkResp(err, resp); err != nil {
			common.PrintLines("get simulation result from dm-master failed:\n%s", err)
			return
		}
		if resp.Filtered != "" {
			result.WillBeFiltered = resp.Filtered
		} else {
			result.MatchRoute = resp.Reason
			simulationResult := resp.SimulationResults[0]
			if len(simulationResult.RouteTableMap) != 1 {
				common.PrintLines("routes map is supposed to has length 1, but is %v", len(simulationResult.RouteTableMap))
				return
			}
			for targetTable := range simulationResult.RouteTableMap {
				result.TargetSchema, result.TargetTable, err = utils.ExtractTable(targetTable)
				if err != nil {
					common.PrintLines(errors.ErrorStack(err))
					return
				}
			}
		}
	}
	common.PrettyPrintInterface(result)
}
