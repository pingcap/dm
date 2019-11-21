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
	"strings"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/ctl/common"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
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
	// no worker is specified, print all routes
	if len(workers) == 0 {
		resp, err := fetchAllTableInfoFromMaster(task)
		if err != nil {
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
			// apply on black-white filter
			filterTableList := getFilterTableList(sourceInfo)
			bwListName := bwListMap[sourceInfo.SourceID]
			bwFilter, err := filter.New(cfg.CaseSensitive, cfg.BWList[bwListName])
			if err != nil {
				common.PrintLines("build of black white filter for source %s failed:\n%s", sourceInfo.SourceID, errors.ErrorStack(err))
				return
			}
			filterTableList = bwFilter.ApplyOn(filterTableList)

			r, err := router.NewTableRouter(cfg.CaseSensitive, routeRulesMap[sourceInfo.SourceID])
			if err != nil {
				common.PrintLines("build of router for source %s failed:\n%s", sourceInfo.SourceID, errors.ErrorStack(err))
				return
			}

			for _, filterTable := range filterTableList {
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
		schema, table, err := getCheckSchemaTableName(cmd)
		if err != nil {
			common.PrintLines("get check table info failed:\n%s", errors.ErrorStack(err))
			return
		}

		mysqlInstance, err := getMySQLInstanceThroughWorker(worker, task, cfg)
		if err != nil {
			common.PrintLines("get mysql instance config failed:\n%s", errors.ErrorStack(err))
			return
		}

		filtered := checkSingleBWFilter(schema, table, cfg.CaseSensitive, cfg.BWList[mysqlInstance.BWListName])
		if filtered {
			result.WillBeFiltered = "yes"
		} else {
			var routesRules []*router.TableRule
			routeLevel := 0
			for _, routeRuleName := range mysqlInstance.RouteRules {
				routesRules = append(routesRules, cfg.Routes[routeRuleName])
				r, err := router.NewTableRouter(cfg.CaseSensitive, []*router.TableRule{cfg.Routes[routeRuleName]})
				if err != nil {
					common.PrintLines("build of table router for source %s failed:\n%s", mysqlInstance.SourceID, errors.ErrorStack(err))
					return
				}
				currentRouteLevel, err := getRouteLevel(r, cfg.CaseSensitive, schema, table)
				if err != nil {
					common.PrintLines("get currentRouteLevel of table %s for source %s failed:\n%s", mysqlInstance.SourceID, dbutil.TableName(schema, table), errors.ErrorStack(err))
					return
				}
				// route to table > route to schema > doesn't match any route
				if currentRouteLevel > routeLevel {
					routeLevel = currentRouteLevel
					result.MatchRoute = routeRuleName
				}
			}

			r, err := router.NewTableRouter(cfg.CaseSensitive, routesRules)
			if err != nil {
				common.PrintLines("build of total table router failed:\n%s", errors.ErrorStack(err))
				return
			}
			result.TargetSchema, result.TargetTable, err = r.Route(schema, table)
			if err != nil {
				common.PrintLines("route of table %s failed:\n%s", dbutil.TableName(schema, table), errors.ErrorStack(err))
				return
			}
		}
	}
	common.PrettyPrintInterface(result)
}

// getRouteLevel gets route result of whether schema table can be routed by r
// 0: won't be routed; 1: match schema rule; 2: match table rule
func getRouteLevel(r *router.Table, caseSensitive bool, schema, table string) (int, error) {
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
