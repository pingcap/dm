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

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/conn"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/utils"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"go.uber.org/zap"
)

// SimulateTask does simulation on dm-master
func (s *Server) SimulateTask(ctx context.Context, req *pb.SimulationRequest) (*pb.SimulationResponse, error) {
	log.L().Info("", zap.Stringer("payload", req), zap.String("request", "FetchSourceInfo"))

	cfg := config.NewTaskConfig()
	err := cfg.Decode(req.Task)
	if err != nil {
		return &pb.SimulationResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}, nil
	}

	_, stCfgs, err := s.generateSubTask(ctx, req.Task)
	if err != nil {
		return &pb.SimulationResponse{
			Result: false,
			Msg:    errors.ErrorStack(err),
		}, nil
	}

	simulationResultCap := len(s.cfg.DeployMap)
	if req.Worker != "" {
		simulationResultCap = 1
	}
	simulationResults := make([]*pb.SimulationResult, 0, simulationResultCap)
	var resp *pb.SimulationResponse
	switch req.Op {
	case pb.SimulateOp_TableRoute:
		resp, err = simulateTableRoute(req.Worker, req.TableQuery, s.cfg.DeployMap, stCfgs, cfg)
		if err != nil {
			return &pb.SimulationResponse{
				Result: false,
				Msg:    errors.ErrorStack(err),
			}, nil
		}
	case pb.SimulateOp_BlackWhiteList:
		resp, err = simulateBlackWhiteList(req.Worker, req.TableQuery, s.cfg.DeployMap, stCfgs, cfg)
		if err != nil {
			return &pb.SimulationResponse{
				Result: false,
				Msg:    errors.ErrorStack(err),
			}, nil
		}
	case pb.SimulateOp_EventFilter:
		resp, err = simulateEventFilter(req.Worker, req.SQL, s.cfg.DeployMap, stCfgs, cfg)
		if err != nil {
			return &pb.SimulationResponse{
				Result: false,
				Msg:    errors.ErrorStack(err),
			}, nil
		}
	}
	resp.SimulationResults = simulationResults
	return resp, nil
}

func simulateTableRoute(worker, tableQuery string, deployMap map[string]string, stCfgs []*config.SubTaskConfig, cfg *config.TaskConfig) (*pb.SimulationResponse, error) {
	simulationResultCap := len(deployMap)
	if worker != "" {
		simulationResultCap = 1
	}
	simulationResults := make([]*pb.SimulationResult, 0, simulationResultCap)
	resp := &pb.SimulationResponse{Result: true}
	for _, stCfg := range stCfgs {
		if worker != "" && deployMap[stCfg.SourceID] != worker {
			continue
		}
		if tableQuery != "" {
			mysqlInstance := getMySQLInstanceConfigThroughSourceID(cfg.MySQLInstances, stCfg.SourceID)

			relativeRouteMap := make(map[string]*router.TableRule, len(mysqlInstance.RouteRules))
			for _, routeRuleName := range mysqlInstance.RouteRules {
				relativeRouteMap[routeRuleName] = cfg.Routes[routeRuleName]
			}

			filtered, matchRoute, matchTable, err := getSingleTableRouteResult(stCfg.CaseSensitive, tableQuery, stCfg.BWList, relativeRouteMap)
			if err != nil {
				return nil, errors.Annotatef(err, "get single table route result from %s failed", stCfg.SourceID)
			}
			if filtered {
				resp.Filtered = "yes"
			} else {
				resp.Reason = matchRoute
				routeMap := map[string]*pb.TableList{matchTable: {Tables: []string{tableQuery}}}
				simulationResults = append(simulationResults, &pb.SimulationResult{
					SourceID:      mysqlInstance.SourceID,
					SourceIP:      getSourceIPString(stCfg.From),
					RouteTableMap: routeMap,
				})
			}
			break
		} else {
			simulationResult, err := getRoutePath(stCfg)
			if err != nil {
				return nil, errors.Annotatef(err, "get route path from %s failed", stCfg.SourceID)
			}
			simulationResults = append(simulationResults, simulationResult)
			if worker != "" {
				break
			}
		}
	}
	resp.SimulationResults = simulationResults
	return resp, nil
}

func simulateBlackWhiteList(worker, tableQuery string, deployMap map[string]string, stCfgs []*config.SubTaskConfig, cfg *config.TaskConfig) (*pb.SimulationResponse, error) {
	simulationResultCap := len(deployMap)
	if worker != "" {
		simulationResultCap = 1
	}
	simulationResults := make([]*pb.SimulationResult, 0, simulationResultCap)
	resp := &pb.SimulationResponse{Result: true}
	for _, stCfg := range stCfgs {
		if worker != "" && deployMap[stCfg.SourceID] != worker {
			continue
		}
		if tableQuery != "" {
			schema, table, err := utils.ExtractTable(tableQuery)
			if err != nil {
				return nil, errors.Annotate(err, "extract query table failed")
			}
			filtered, err := checkSingleBWFilter(schema, table, cfg.CaseSensitive, stCfg.BWList)
			if err != nil {
				return nil, errors.Annotatef(err, "check single black-white filter failed from source %s", stCfg.SourceID)
			}
			if filtered {
				resp.Filtered = "yes"
			} else {
				resp.Filtered = "no"
			}
			simulationResults = append(simulationResults, &pb.SimulationResult{
				SourceID: stCfg.SourceID,
				SourceIP: getSourceIPString(stCfg.From),
			})
			break
		} else {
			simulationResult, err := getDoIgnoreTables(stCfg)
			if err != nil {
				return nil, errors.Annotatef(err, "get do ignore tables from source %s failed", stCfg.SourceID)
			}
			simulationResults = append(simulationResults, simulationResult)
			if worker != "" {
				break
			}
		}
	}
	return resp, nil
}

func simulateEventFilter(worker, sql string, deployMap map[string]string, stCfgs []*config.SubTaskConfig, cfg *config.TaskConfig) (*pb.SimulationResponse, error) {
	simulationResultCap := len(deployMap)
	if worker != "" {
		simulationResultCap = 1
	}
	simulationResults := make([]*pb.SimulationResult, 0, simulationResultCap)
	resp := &pb.SimulationResponse{Result: true}
	for _, stCfg := range stCfgs {
		if worker != "" && deployMap[stCfg.SourceID] != worker {
			continue
		}
		// get sourceID relative binlog event filter
		mysqlInstance := getMySQLInstanceConfigThroughSourceID(cfg.MySQLInstances, stCfg.SourceID)
		relativeEventFilterMap := make(map[string]*bf.BinlogEventRule, len(mysqlInstance.FilterRules))
		for _, eventFilterName := range mysqlInstance.FilterRules {
			relativeEventFilterMap[eventFilterName] = cfg.Filters[eventFilterName]
		}

		filterName, action, err := filterSQL(sql, relativeEventFilterMap, stCfg.CaseSensitive)
		if err != nil {
			return &pb.SimulationResponse{
				Result: false,
				Msg:    errors.ErrorStack(err),
			}, nil
		}

		resp.Reason = filterName
		if action == bf.Ignore {
			resp.Filtered = "yes"
		} else {
			resp.Filtered = "no"
		}
		simulationResults = append(simulationResults, &pb.SimulationResult{
			SourceID: stCfg.SourceID,
			SourceIP: getSourceIPString(stCfg.From),
		})
		break
	}
	return resp, nil
}

func getSourceIPString(dbCfg config.DBConfig) string {
	return fmt.Sprintf("%s:%d", dbCfg.Host, dbCfg.Port)
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

func checkSingleBWFilter(schema, table string, caseSensitive bool, rules *filter.Rules) (bool, error) {
	checkTable := []*filter.Table{{Schema: schema, Name: table}}
	bwFilter, err := filter.New(caseSensitive, rules)
	if err != nil {
		return false, err
	}
	checkTable = bwFilter.ApplyOn(checkTable)
	return len(checkTable) == 0, nil
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
// input routeRuleMap: {key: routeName, value: tableRule}
func getRouteName(caseSensitive bool, schema, table string, routeRuleMap map[string]*router.TableRule) (string, string, error) {
	var (
		routeLevel int
		matchRoute string
	)
	routeRules := make([]*router.TableRule, 0, len(routeRuleMap))
	for routeRuleName, tableRule := range routeRuleMap {
		routeRules = append(routeRules, tableRule)
		r, err := router.NewTableRouter(caseSensitive, []*router.TableRule{tableRule})
		if err != nil {
			return "", "", errors.Annotatef(err, "build of table router for rule %s failed", routeRuleName)
		}
		currentRouteLevel, err := getRouteLevel(r, caseSensitive, schema, table)
		if err != nil {
			common.PrintLines("get currentRouteLevel of table %s for rule %s failed:\n%s", dbutil.TableName(schema, table), routeRuleName, errors.ErrorStack(err))
			return "", "", nil
		}
		// route to table > route to schema > doesn't match any route
		if currentRouteLevel > routeLevel {
			routeLevel = currentRouteLevel
			matchRoute = routeRuleName
		}
	}
	r, err := router.NewTableRouter(caseSensitive, routeRules)
	if err != nil {
		return "", "", errors.Annotate(err, "build of total table router failed")
	}
	targetSchema, targetTable, err := r.Route(schema, table)
	if err != nil {
		return "", "", errors.Annotatef(err, "route of table %s failed", dbutil.TableName(schema, table))
	}
	return matchRoute, dbutil.TableName(targetSchema, targetTable), nil
}

// getSingleTableRouteResult gets matched route name and target table from one MySQL instance
// returns filtered, matchRoute, matchTable, error
func getSingleTableRouteResult(caseSensitive bool, tableQuery string, bwRules *filter.Rules, routeMap map[string]*router.TableRule) (bool, string, string, error) {
	schema, table, err := utils.ExtractTable(tableQuery)
	if err != nil {
		return false, "", "", errors.Trace(err)
	}
	filtered, err := checkSingleBWFilter(schema, table, caseSensitive, bwRules)
	if err != nil {
		return false, "", "", errors.Trace(err)
	}
	if filtered {
		return true, "", "", nil
	}
	matchRoute, matchTable, err := getRouteName(caseSensitive, schema, table, routeMap)
	if err != nil {
		return false, "", "", errors.Trace(err)
	}

	return false, matchRoute, matchTable, nil
}

// getRoutePath gets table routes from one MySQL instance
func getRoutePath(stCfg *config.SubTaskConfig) (*pb.SimulationResult, error) {
	bwFilter, err := filter.New(stCfg.CaseSensitive, stCfg.BWList)
	if err != nil {
		return nil, errors.Annotatef(err, "build of black white filter for source %s failed", stCfg.SourceID)
	}

	r, err := router.NewTableRouter(stCfg.CaseSensitive, stCfg.RouteRules)
	if err != nil {
		return nil, errors.Annotatef(err, "build of router for source %s failed", stCfg.SourceID)
	}

	sourceDB, err := conn.DefaultDBProvider.Apply(stCfg.From)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer sourceDB.Close()

	fetchedRouteTableMap, err := utils.FetchTargetDoTables(sourceDB.DB, bwFilter, r)
	if err != nil {
		return nil, errors.Annotatef(err, "routing from source %s failed", stCfg.SourceID)
	}

	routeTableMap := make(map[string]*pb.TableList, len(fetchedRouteTableMap))
	// transfer filter.table to string
	for targetName, routeTableList := range fetchedRouteTableMap {
		sourceTableStringList := make([]string, 0, len(routeTableList))
		for _, sourceTable := range routeTableList {
			sourceTableStringList = append(sourceTableStringList, sourceTable.String())
		}
		routeTableMap[targetName] = &pb.TableList{Tables: sourceTableStringList}
	}

	return &pb.SimulationResult{
		SourceID:      stCfg.SourceID,
		SourceIP:      getSourceIPString(stCfg.From),
		RouteTableMap: routeTableMap,
	}, nil
}

// getDoIgnoreTables gets do tables and ignore tables from one MySQL instance
func getDoIgnoreTables(stCfg *config.SubTaskConfig) (*pb.SimulationResult, error) {
	bwFilter, err := filter.New(stCfg.CaseSensitive, stCfg.BWList)
	if err != nil {
		return nil, errors.Trace(err)
	}

	sourceDB, err := conn.DefaultDBProvider.Apply(stCfg.From)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer sourceDB.Close()

	mapping, err := utils.FetchAllDoTables(sourceDB.DB, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	doTableMap := make(map[string]*pb.TableList, len(mapping))
	ignoreTableMap := make(map[string]*pb.TableList, len(mapping))

	for schema, tableList := range mapping {
		doTables := make([]string, 0)
		ignoreTables := make([]string, 0)
		for _, table := range tableList {
			if len(bwFilter.ApplyOn([]*filter.Table{{Schema: schema, Name: table}})) == 0 {
				ignoreTables = append(ignoreTables, table)
			} else {
				doTables = append(doTables, table)
			}
		}
		doTableMap[schema] = &pb.TableList{Tables: doTables}
		ignoreTableMap[schema] = &pb.TableList{Tables: ignoreTables}
	}

	return &pb.SimulationResult{
		SourceID:       stCfg.SourceID,
		SourceIP:       getSourceIPString(stCfg.From),
		DoTableMap:     doTableMap,
		IgnoreTableMap: ignoreTableMap,
	}, nil
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

// genSchemaAndTable generates target schema and table based on the StmtNode
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
