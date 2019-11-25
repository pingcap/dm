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
	"strings"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/utils"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/spf13/cobra"
)

// checkResp checks the result of RPC response
func checkResp(err error, resp *pb.FetchSourceInfoResponse) error {
	if err != nil {
		return errors.Trace(err)
	} else if !resp.Result {
		return errors.New(resp.Msg)
	}
	return nil
}

func fetchAllTableInfoFromMaster(task string) (*pb.FetchSourceInfoResponse, error) {
	cli := common.MasterClient()
	ctx, cancel := context.WithTimeout(context.Background(), common.GlobalConfig().RPCTimeout)
	defer cancel()
	resp, err := cli.FetchSourceInfo(ctx, &pb.FetchSourceInfoRequest{
		FetchTable: true,
		Task:       task,
	})
	if err = checkResp(err, resp); err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func getFilterTableList(sourceInfo *pb.SourceInfo) []*filter.Table {
	tableList := make([]*filter.Table, 0, len(sourceInfo.Schemas))
	for i := range sourceInfo.Schemas {
		tableList = append(tableList, &filter.Table{
			Schema: sourceInfo.Schemas[i],
			Name:   sourceInfo.Tables[i],
		})
	}
	return tableList
}

func getTableNameFromCMD(cmd *cobra.Command) (string, string, error) {
	tableName, err := cmd.Flags().GetString("table")
	if err != nil {
		return "", "", errors.Annotate(err, "get table arg failed")
	}
	if tableName == "" {
		return "", "", errors.New("argument table is not given. pls check it again")
	}
	schema, table, err := utils.ExtractTable(tableName)
	if err != nil {
		return "", "", errors.Annotate(err, "extract table info failed")
	}
	return schema, table, nil
}

func getMySQLInstanceConfigThroughSourceID(mysqlInstances []*config.MySQLInstance, sourceID string) *config.MySQLInstance {
	var mysqlInstance *config.MySQLInstance
	for _, mysqlInst := range mysqlInstances {
		if mysqlInst.SourceID == sourceID {
			mysqlInstance = mysqlInst
			break
		}
	}
	return mysqlInstance
}

func getMySQLInstanceThroughWorker(worker, task string, mysqlInstances []*config.MySQLInstance) (*config.MySQLInstance, error) {
	cli := common.MasterClient()
	ctx, cancel := context.WithTimeout(context.Background(), common.GlobalConfig().RPCTimeout)
	defer cancel()

	resp, err := cli.FetchSourceInfo(ctx, &pb.FetchSourceInfoRequest{
		Worker:     worker,
		FetchTable: false,
		Task:       task,
	})
	if err = checkResp(err, resp); err != nil {
		return nil, errors.Annotate(err, "can not fetch source info from dm-master")
	}

	if len(resp.SourceInfo) == 0 {
		return nil, errors.New("the source info of specified worker is not found. pls check the worker address")
	}

	sourceID := resp.SourceInfo[0].SourceID
	mysqlInstance := getMySQLInstanceConfigThroughSourceID(mysqlInstances, sourceID)
	if mysqlInstance == nil {
		return nil, errors.New("the mysql instance config is not found. pls check the worker address")
	}
	return mysqlInstance, nil
}

func checkSingleBWFilter(schema, table string, caseSensitive bool, rules *filter.Rules) bool {
	checkTable := []*filter.Table{{Schema: schema, Name: table}}
	bwFilter, err := filter.New(caseSensitive, rules)
	if err != nil {
		common.PrintLines("build of black-white filter failed:\n%s", errors.ErrorStack(err))
	}
	checkTable = bwFilter.ApplyOn(checkTable)
	return len(checkTable) == 0
}

func getDoIgnoreTables(caseSensitive bool, bwListMap map[string]*filter.Rules, sourceInfoList []*pb.SourceInfo) (map[string][]string, map[string][]string, error) {
	doTableStringMap := make(map[string][]string, 0)
	ignoreTableStringMap := make(map[string][]string, 0)
	for _, sourceInfo := range sourceInfoList {
		filterTableList := getFilterTableList(sourceInfo)
		bwFilter, err := filter.New(caseSensitive, bwListMap[sourceInfo.SourceID])
		if err != nil {
			return nil, nil, errors.Annotate(err, "build of black white filter failed")
		}
		doTableList := bwFilter.ApplyOn(filterTableList)

		doTableStringList := make([]string, 0, len(doTableList))
		ignoreTableStringList := make([]string, 0, len(filterTableList)-len(doTableList))

		var i int
		for _, table := range filterTableList {
			if i < len(doTableList) && *doTableList[i] == *table {
				i++
				doTableStringList = append(doTableStringList, table.String())
			} else {
				ignoreTableStringList = append(ignoreTableStringList, table.String())
			}
		}

		doTableStringMap[sourceInfo.SourceIP] = doTableStringList
		ignoreTableStringMap[sourceInfo.SourceIP] = ignoreTableStringList
	}
	return doTableStringMap, ignoreTableStringMap, nil
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

// getRouteName gets route name for specified schema, table
// input routeRuleMap: {key: routeName, value: relative tableRule}
func getRouteName(caseSensitive bool, schema, table string, routeRuleMap map[string]*router.TableRule) (string, error) {
	var (
		routeLevel int
		matchRoute string
	)
	for routeRuleName, tableRule := range routeRuleMap {
		r, err := router.NewTableRouter(caseSensitive, []*router.TableRule{tableRule})
		if err != nil {
			return "", errors.Annotatef(err, "build of table router for rule %s failed", routeRuleName)
		}
		currentRouteLevel, err := getRouteLevel(r, caseSensitive, schema, table)
		if err != nil {
			common.PrintLines("get currentRouteLevel of table %s for rule %s failed:\n%s", dbutil.TableName(schema, table), routeRuleName, errors.ErrorStack(err))
			return "", nil
		}
		// route to table > route to schema > doesn't match any route
		if currentRouteLevel > routeLevel {
			routeLevel = currentRouteLevel
			matchRoute = routeRuleName
		}
	}
	return matchRoute, nil
}

// getRoutePath returns routes path for tables. result map format: {key: targetTable, value: {key: sourceIP, value: sourceTable}}
// input: bwListMap {key: sourceID, value: relative black-white list}, routeRulesMap: {key: sourceID, value: relative tableRule list}
func getRoutePath(caseSensitive bool, bwListMap map[string]*filter.Rules, routeRulesMap map[string][]*router.TableRule, sourceInfoList []*pb.SourceInfo) (map[string]map[string][]string, error) {
	routesResultMap := make(map[string]map[string][]string)
	for _, sourceInfo := range sourceInfoList {
		// apply on black-white filter
		filterTableList := getFilterTableList(sourceInfo)
		bwFilter, err := filter.New(caseSensitive, bwListMap[sourceInfo.SourceID])
		if err != nil {
			return nil, errors.Annotatef(err, "build of black white filter for source %s failed", sourceInfo.SourceID)
		}
		filterTableList = bwFilter.ApplyOn(filterTableList)

		r, err := router.NewTableRouter(caseSensitive, routeRulesMap[sourceInfo.SourceID])
		if err != nil {
			return nil, errors.Annotatef(err, "build of router for source %s failed", sourceInfo.SourceID)
		}

		for _, filterTable := range filterTableList {
			schema, table, err := r.Route(filterTable.Schema, filterTable.Name)
			if err != nil {
				return nil, errors.Annotatef(err, "routing table %s from MySQL %s failed", dbutil.TableName(schema, table), sourceInfo.SourceIP)
			}
			targetTable := dbutil.TableName(schema, table)
			sourceTable := filterTable.String()
			if routesResultMap[targetTable] == nil {
				routesResultMap[targetTable] = make(map[string][]string)
			}
			routesResultMap[targetTable][sourceInfo.SourceIP] = append(routesResultMap[targetTable][sourceInfo.SourceIP], sourceTable)
		}
	}
	return routesResultMap, nil
}

// filterSQL will parse sql, and check whether this sql will be filtered.
// It returns (matched filter name, action, error)
func filterSQL(sql string, filterMap map[string]*bf.BinlogEventRule, caseSensitive bool) (string, bf.ActionType, error) {
	sql = utils.TrimCtrlChars(sql)
	sqlParser := parser.New()
	stmts, _, err := sqlParser.Parse(sql, "", "")
	if err != nil {
		return "", bf.Ignore, errors.Annotate(err, "parsing sql failed")
	}
	if len(stmts) > 1 {
		return "", bf.Ignore, errors.New("invalid sql, length is bigger than 1")
	} else if stmts == nil {
		return "", bf.Ignore, errors.New("invalid sql, length is 0")
	}

	n := stmts[0]
	et := bf.AstToDDLEvent(n)
	table := filter.Table{}
	genSchemaAndTable(&table, n)

	for name, filterContent := range filterMap {
		singleFilter, err := bf.NewBinlogEvent(caseSensitive, []*bf.BinlogEventRule{filterContent})
		if err != nil {
			return "", bf.Ignore, errors.Annotatef(err, "creating singleFilter %s failed", name)
		}

		action, err := singleFilter.Filter(table.Schema, table.Name, et, sql)
		if err != nil {
			return "", bf.Ignore, errors.Annotatef(err, "singleFilter %s failed in filtering table %s", name, table)
		}
		if action == bf.Ignore {
			return name, bf.Ignore, nil
		}
		// TODO: Check whether this table can match any event by this filter. If so, this sql is picked by this filter
	}
	return "", bf.Do, nil
}

func genSchemaAndTable(table *filter.Table, n ast.StmtNode) {
	switch v := n.(type) {
	case *ast.CreateDatabaseStmt:
		setSchemaAndTable(table, v.Name, "")
	case *ast.DropDatabaseStmt:
		setSchemaAndTable(table, v.Name, "")
	case *ast.CreateTableStmt:
		setSchemaAndTable(table, v.Table.Schema.O, v.Table.Name.O)
	case *ast.DropTableStmt:
		setSchemaAndTable(table, v.Tables[0].Schema.O, v.Tables[0].Name.O)
	case *ast.AlterTableStmt:
		setSchemaAndTable(table, v.Table.Schema.O, v.Table.Name.O)
	case *ast.RenameTableStmt:
		setSchemaAndTable(table, v.OldTable.Schema.O, v.OldTable.Name.O)
	case *ast.TruncateTableStmt:
		setSchemaAndTable(table, v.Table.Schema.O, v.Table.Name.O)
	case *ast.CreateIndexStmt:
		setSchemaAndTable(table, v.Table.Schema.O, v.Table.Name.O)
	case *ast.DropIndexStmt:
		setSchemaAndTable(table, v.Table.Schema.O, v.Table.Name.O)
	}
}

func setSchemaAndTable(table *filter.Table, schemaName, tableName string) {
	table.Schema = schemaName
	table.Name = tableName
}
