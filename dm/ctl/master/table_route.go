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
	"fmt"
	"strings"

	"github.com/pingcap/tidb-tools/pkg/filter"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/utils"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/spf13/cobra"
)

type tableRouteResult struct {
	Result       bool                           `json:"result"`
	Msg          string                         `json:"msg"`
	Routes       map[string]map[string][]string `json:"routes,omitempty"`
	MatchRoute   string                         `json:"match-route,omitempty"`
	TargetSchema string                         `json:"target-schema,omitempty"`
	TargetTable  string                         `json:"target-table,omitempty"`
}

// NewTableRouteCmd creates a TableRoute command
func NewTableRouteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "table-route [-w worker] [-T table-name] <config-file>",
		Short: "check the routes for all tables or single table",
		Run:   tableRouteFunc,
	}
	cmd.Flags().StringP("table-name", "T", "", "the table name we want to check for the table route")
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
		fmt.Println(errors.ErrorStack(err))
		return
	}
	if len(workers) > 1 {
		fmt.Println("we want 0 or 1 worker, but get ", workers)
		return
	}

	result := &tableRouteResult{
		Result: true,
	}
	// no worker is specified, print all routes
	if len(workers) == 0 {
		cli := common.MasterClient()
		ctx, cancel := context.WithTimeout(context.Background(), common.GlobalConfig().RPCTimeout)
		defer cancel()
		resp, err := cli.FetchSourceInfo(ctx, &pb.FetchSourceInfoRequest{
			FetchTable: true,
			Task:       task,
		})
		if err = checkResp(err, resp); err != nil {
			common.PrintLines("can not fetch source info from dm-master:\n%s", err)
			return
		}

		bwListMap := make(map[string]string, len(cfg.MySQLInstances))
		for _, inst := range cfg.MySQLInstances {
			bwListMap[inst.SourceID] = inst.BWListName
		}

		routeRulesMap := make(map[string][]*router.TableRule, len(cfg.MySQLInstances))
		for _, inst := range cfg.MySQLInstances {
			routeRules := make([]*router.TableRule, 0, len(inst.RouteRules))
			for _, routeName := range inst.RouteRules {
				routeRules = append(routeRules, cfg.Routes[routeName])
			}
			routeRulesMap[inst.SourceID] = routeRules
		}

		// key: targetTable, value: {key: sourceIP, value: sourceTable}
		routesResultMap := make(map[string]map[string][]string)
		for _, sourceInfo := range resp.SourceInfo {
			r, err := router.NewTableRouter(cfg.CaseSensitive, routeRulesMap[sourceInfo.SourceID])
			if err != nil {
				common.PrintLines("build of router failed:\n%s", errors.ErrorStack(err))
				return
			}

			// apply on black-white filter
			tableList := getFilterTableList(sourceInfo)
			bwListName := bwListMap[sourceInfo.SourceID]
			bwFilter, err := filter.New(cfg.CaseSensitive, cfg.BWList[bwListName])
			if err != nil {
				common.PrintLines("build of black white filter failed:\n%s", errors.ErrorStack(err))
			}
			tableList = bwFilter.ApplyOn(tableList)

			for _, filterTable := range tableList {
				schema, table, err := r.Route(filterTable.Schema, filterTable.Name)
				if err != nil {
					common.PrintLines("routing table %s from MySQL %s failed:\n%s", dbutil.TableName(schema, table), sourceInfo.SourceIP, errors.ErrorStack(err))
					return
				}
				targetTable := dbutil.TableName(schema, table)
				sourceTable := filterTable.String()
				if routesResultMap[targetTable] == nil {
					routesResultMap[targetTable] = make(map[string][]string)
				}
				routesResultMap[targetTable][sourceInfo.SourceIP] = append(routesResultMap[targetTable][sourceInfo.SourceIP], sourceTable)
			}
		}
		result.Routes = routesResultMap
	} else {
		worker := workers[0]
		tableName, err := cmd.Flags().GetString("table-name")
		if err != nil {
			fmt.Println(errors.ErrorStack(err))
			return
		}
		schema, table, err := utils.ExtractTable(tableName)
		if err != nil {
			fmt.Println(errors.ErrorStack(err))
			return
		}

		mysqlInstance, err := getMySQLInstanceThroughWorker(worker, task, cfg)
		if err != nil {
			fmt.Println(errors.ErrorStack(err))
			return
		}

		var routesRules []*router.TableRule
		routedLevel := 0
		for _, routeRuleName := range mysqlInstance.RouteRules {
			routesRules = append(routesRules, cfg.Routes[routeRuleName])
			r, err := router.NewTableRouter(cfg.CaseSensitive, []*router.TableRule{cfg.Routes[routeRuleName]})
			if err != nil {
				common.PrintLines("build of table router failed:\n%s", errors.ErrorStack(err))
				return
			}
			routed, err := checkRoute(r, cfg.CaseSensitive, schema, table)
			if err != nil {
				common.PrintLines("check whether table will be routed failed:\n%s", errors.ErrorStack(err))
				return
			}
			if routed > routedLevel {
				routedLevel = routed
				result.MatchRoute = routeRuleName
			}
		}
		r, err := router.NewTableRouter(cfg.CaseSensitive, routesRules)
		if err != nil {
			common.PrintLines("build of table router failed:\n%s", errors.ErrorStack(err))
			return
		}
		result.TargetSchema, result.TargetTable, err = r.Route(schema, table)
		if err != nil {
			common.PrintLines("route of table %s failed:\n%s", dbutil.TableName(schema, table), errors.ErrorStack(err))
			return
		}
	}
	common.PrettyPrintInterface(result)
}

// checkRoute checks whether schema table can be routed by r
// 0: won't be routed; 1: match schema rule; 2: match table rule
func checkRoute(r *router.Table, caseSensitive bool, schema, table string) (int, error) {
	schemaL, tableL := schema, table
	if !caseSensitive {
		schemaL, tableL = strings.ToLower(schema), strings.ToLower(table)
	}

	rules := r.Match(schemaL, tableL)
	var (
		schemaRules = make([]*router.TableRule, 0, len(rules))
		tableRules  = make([]*router.TableRule, 0, len(rules))
	)
	// classify rules into schema level rules and table level
	// table level rules have highest priority
	for i := range rules {
		rule, ok := rules[i].(*router.TableRule)
		if !ok {
			return 0, errors.NotValidf("table route rule %+v", rules[i])
		}

		if len(rule.TablePattern) == 0 {
			schemaRules = append(schemaRules, rule)
		} else {
			tableRules = append(tableRules, rule)
		}
	}

	if len(table) == 0 || len(tableRules) == 0 {
		if len(schemaRules) > 1 {
			return 0, errors.NotSupportedf("route %s/%s to rule set(%d)", schema, table, len(schemaRules))
		}

		if len(schemaRules) == 1 {
			return 1, nil
		}
	} else {
		if len(tableRules) > 1 {
			return 0, errors.NotSupportedf("route %s/%s to rule set(%d)", schema, table, len(tableRules))
		}

		return 2, nil
	}

	return 0, nil
}
