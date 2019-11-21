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

		bwListMap := make(map[string]*filter.Rules, len(cfg.MySQLInstances))
		for _, inst := range cfg.MySQLInstances {
			bwListMap[inst.SourceID] = cfg.BWList[inst.BWListName]
		}

		routeRulesMap := make(map[string][]*router.TableRule, len(cfg.MySQLInstances))
		for _, inst := range cfg.MySQLInstances {
			routeRules := make([]*router.TableRule, 0, len(inst.RouteRules))
			for _, routeName := range inst.RouteRules {
				routeRules = append(routeRules, cfg.Routes[routeName])
			}
			routeRulesMap[inst.SourceID] = routeRules
		}

		result.Routes, err = getRoutePath(cfg.CaseSensitive, bwListMap, routeRulesMap, resp.SourceInfo)
		if err != nil {
			common.PrintLines(errors.ErrorStack(err))
			return
		}
	} else {
		worker := workers[0]
		schema, table, err := getTableNameFromCMD(cmd)
		if err != nil {
			common.PrintLines("get check table info failed:\n%s", errors.ErrorStack(err))
			return
		}

		mysqlInstance, err := getMySQLInstanceThroughWorker(worker, task, cfg.MySQLInstances)
		if err != nil {
			common.PrintLines("get mysql instance config failed:\n%s", errors.ErrorStack(err))
			return
		}

		filtered := checkSingleBWFilter(schema, table, cfg.CaseSensitive, cfg.BWList[mysqlInstance.BWListName])
		if filtered {
			result.WillBeFiltered = "yes"
		} else {
			routeRuleMap := make(map[string]*router.TableRule, len(mysqlInstance.RouteRules))
			routeRules := make([]*router.TableRule, 0, len(mysqlInstance.RouteRules))
			for _, routeRuleName := range mysqlInstance.RouteRules {
				routeRuleMap[routeRuleName] = cfg.Routes[routeRuleName]
				routeRules = append(routeRules, cfg.Routes[routeRuleName])
			}

			result.MatchRoute, err = getRouteName(cfg.CaseSensitive, schema, table, routeRuleMap)
			if err != nil {
				common.PrintLines(errors.ErrorStack(err))
				return
			}

			r, err := router.NewTableRouter(cfg.CaseSensitive, routeRules)
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
