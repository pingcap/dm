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
	"strings"

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
	Routes       map[string]map[string][]*tableInfo `json:"routes,omitempty"`
	IgnoreTables []string                           `json:"ignore-tables,omitempty"`
}

// NewTableRouteCmd creates a TableRoute command
func NewTableRouteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "table-route [-F format] [-w worker] [-T table] <config-file>",
		Short: "check the routes for all tables or single table",
		Run:   tableRouteFunc,
	}
	tableList = tableList[:0]
	cmd.Flags().StringSliceVarP(&tableList, "table", "T", []string{}, "the table name we want to check for the table route")
	cmd.Flags().StringP("format", "F", "json", "the output format of routes")
	return cmd
}

func tableRouteFunc(cmd *cobra.Command, _ []string) {
	format, err := cmd.Flags().GetString("format")
	if err != nil {
		common.PrintLines(errors.ErrorStack(err))
		return
	}
	resp := simulateTask4BWListOrTableRoute(cmd, pb.SimulateOp_TableRoute)
	if resp == nil {
		return
	}

	result := &tableRouteResult{}
	// no table is specified, print all routes
	routes := make(map[string]map[string][]*tableInfo)
	for _, simulationResult := range resp.SimulationResults {
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
				var reason string
				if i < len(sourceTableList.Reasons) {
					reason = sourceTableList.Reasons[i]
				}
				routes[targetTable][simulationResult.SourceAddr] = append(routes[targetTable][simulationResult.SourceAddr], &tableInfo{Table: sourceTable, Reason: reason})
			}
		}
	}

	result.Routes = routes
	if strings.EqualFold(format, "dot") {
		common.PrintLines(dotGraphForRoutes(routes))
		return
	}
	common.PrettyPrintInterface(result)
}
