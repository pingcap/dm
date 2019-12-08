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

type bwListResult struct {
	Result         bool                `json:"result"`
	Msg            string              `json:"msg"`
	DoTables       map[string][]string `json:"do-tables,omitempty"`
	IgnoreTables   map[string][]string `json:"ignore-tables,omitempty"`
	WillBeFiltered string              `json:"will-be-filtered,omitempty"`
}

// NewBWListCmd creates a BWList command
func NewBWListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bw-list [-w worker] [-T table] <config-file>",
		Short: "check the black-white-list info for tables",
		Run:   bwListFunc,
	}
	tableList = tableList[:0]
	cmd.Flags().StringSliceVarP(&tableList, "table", "T", []string{}, "the table name we want to check for the table route")
	return cmd
}

func bwListFunc(cmd *cobra.Command, _ []string) {
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

	result := &bwListResult{
		Result: true,
	}
	cli := common.MasterClient()
	ctx, cancel := context.WithTimeout(context.Background(), common.GlobalConfig().RPCTimeout)
	defer cancel()
	resp, err := cli.SimulateTask(ctx, &pb.SimulationRequest{
		Op:        pb.SimulateOp_BlackWhiteList,
		Task:      task,
		Workers:   workers,
		TableList: tables,
	})
	if err := checkResp(err, resp); err != nil {
		common.PrintLines("get simulation result from dm-master failed:\n%s", err)
		return
	}

	doTableMap := make(map[string][]string, len(resp.SimulationResults))
	ignoreTableMap := make(map[string][]string, len(resp.SimulationResults))
	for _, simulationResult := range resp.SimulationResults {
		doTableList := make([]string, 0)
		ignoreTableList := make([]string, 0)
		for schema, pbTableList := range simulationResult.DoTableMap {
			for _, table := range pbTableList.Tables {
				doTableList = append(doTableList, dbutil.TableName(schema, table))
			}
		}
		for schema, pbTableList := range simulationResult.IgnoreTableMap {
			for _, table := range pbTableList.Tables {
				ignoreTableList = append(ignoreTableList, dbutil.TableName(schema, table))
			}
		}

		doTableMap[simulationResult.SourceAddr] = doTableList
		ignoreTableMap[simulationResult.SourceAddr] = ignoreTableList
	}
	result.DoTables = doTableMap
	result.IgnoreTables = ignoreTableMap

	common.PrettyPrintInterface(result)
}
