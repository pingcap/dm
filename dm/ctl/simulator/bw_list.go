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
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/tidb-tools/pkg/dbutil"

	"github.com/spf13/cobra"
)

type bwListResult struct {
	DoTables     map[string][]string `json:"do-tables,omitempty"`
	IgnoreTables map[string][]string `json:"ignore-tables,omitempty"`
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
	resp := simulateTask4BWListOrTableRoute(cmd, pb.SimulateOp_BlackWhiteList)
	if resp == nil {
		return
	}

	doTableMap := make(map[string][]string, len(resp.SimulationResults))
	ignoreTableMap := make(map[string][]string, len(resp.SimulationResults))
	for _, simulationResult := range resp.SimulationResults {
		var doTableList, ignoreTableList []string
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

		if len(doTableList) > 0 {
			doTableMap[simulationResult.SourceAddr] = doTableList
		}
		if len(ignoreTableList) > 0 {
			ignoreTableMap[simulationResult.SourceAddr] = ignoreTableList
		}
	}

	common.PrettyPrintInterface(bwListResult{
		DoTables:     doTableMap,
		IgnoreTables: ignoreTableMap,
	})
}
