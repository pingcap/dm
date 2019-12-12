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

	"github.com/spf13/cobra"
)

// checkResp checks the result of RPC response
func checkResp(err error, resp *pb.SimulationResponse) error {
	if err != nil {
		return errors.Trace(err)
	} else if !resp.Result {
		return errors.New(resp.Msg)
	}
	return nil
}

func simulateTask4BWListOrTableRoute(cmd *cobra.Command, op pb.SimulateOp) (resp *pb.SimulationResponse) {
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

	cli := common.MasterClient()
	ctx, cancel := context.WithTimeout(context.Background(), common.GlobalConfig().RPCTimeout)
	defer cancel()
	resp, err = cli.SimulateTask(ctx, &pb.SimulationRequest{
		Op:        op,
		Task:      task,
		Workers:   workers,
		TableList: tables,
	})
	if err := checkResp(err, resp); err != nil {
		common.PrintLines("get simulation result from dm-master failed:\n%s", err)
		return
	}
	return resp
}

func dotGraphForRoutes(routes map[string]map[string][]*tableInfo) string {
	result := "digraph routes {\nrankdir=\"LR\"\n"
	for targetTable, sourceTables := range routes {
		result += fmt.Sprintf("subgraph \"cluster_%v\"{\n", targetTable)
		for sourceAddr, tables := range sourceTables {
			nodeName := sourceAddr + targetTable
			result += fmt.Sprintf(`"%s" [label = "%s"]`, nodeName, sourceAddr)
			result += "\n"
			result += fmt.Sprintf(`"%s" -> "%s"`, nodeName, targetTable)
			result += "\n"
			for _, table := range tables {
				name := table.Table + sourceAddr
				result += fmt.Sprintf(`"%s" [label = "%s"]`, name, table.Table)
				result += "\n"
				result += fmt.Sprintf(`"%s" -> "%s"`, name, nodeName)
				result += "\n"
			}
		}
		result += "}\n"
	}
	result += "}\n"
	return result
}
