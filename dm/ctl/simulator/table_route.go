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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/spf13/cobra"
)

var tableList []string

type tableInfo struct {
	Table  string `json:"table"`
	Reason string `json:"reason,omitempty"`
}

type tableRouteResult struct {
	Result       bool                               `json:"result"`
	Msg          string                             `json:"msg"`
	Routes       map[string]map[string][]*tableInfo `json:"routes,omitempty"`
	IgnoreTables []string                           `json:"ignore-tables,omitempty"`
}

// NewTableRouteCmd creates a TableRoute command
func NewTableRouteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "table-route [-w worker] [-T table] <config-file>",
		Short: "check the routes for all tables or single table",
		Run:   tableRouteFunc,
	}
	tableList = tableList[:0]
	cmd.Flags().StringSliceVarP(&tableList, "table", "T", []string{}, "the table name we want to check for the table route")
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
	tables, err := cmd.Flags().GetStringSlice("table")
	if err != nil {
		common.PrintLines(errors.ErrorStack(err))
		return
	}

	result := &tableRouteResult{
		Result: true,
	}
	cli := common.MasterClient()
	ctx, cancel := context.WithTimeout(context.Background(), common.GlobalConfig().RPCTimeout)
	defer cancel()
	resp, err := cli.SimulateTask(ctx, &pb.SimulationRequest{
		Op:        pb.SimulateOp_TableRoute,
		Task:      task,
		Workers:   workers,
		TableList: tables,
	})
	if err := checkResp(err, resp); err != nil {
		common.PrintLines("get simulation result from dm-master failed:\n%s", err)
		return
	}

	// no table is specified, print all routes
	if len(tables) == 0 {
		routes := make(map[string]map[string][]*tableInfo)
		for _, simulationResult := range resp.SimulationResults {
			for targetTable, sourceTableList := range simulationResult.RouteTableMap {
				for _, sourceTable := range sourceTableList.Tables {
					if routes[targetTable] == nil {
						routes[targetTable] = make(map[string][]*tableInfo)
					}
					routes[targetTable][simulationResult.SourceAddr] = append(routes[targetTable][simulationResult.SourceAddr], &tableInfo{Table: sourceTable})
				}
			}
		}

		result.Routes = routes
	} else {
		routes := make(map[string]map[string][]*tableInfo)
		for _, simulationResult := range resp.SimulationResults {
			result.IgnoreTables = make([]string, 0)
			for schema, tableList := range simulationResult.IgnoreTableMap {
				for _, table := range tableList.Tables {
					result.IgnoreTables = append(result.IgnoreTables, dbutil.TableName(schema, table))
				}
			}
			for targetTable, sourceTableList := range simulationResult.RouteTableMap {
				for i, sourceTable := range sourceTableList.Tables {
					if routes[targetTable] == nil {
						routes[targetTable] = make(map[string][]*tableInfo)
					}
					routes[targetTable][simulationResult.SourceAddr] = append(routes[targetTable][simulationResult.SourceAddr], &tableInfo{Table: sourceTable, Reason: sourceTableList.Reasons[i]})
				}
			}
		}

		result.Routes = routes
	}
	common.PrettyPrintInterface(result)
}
