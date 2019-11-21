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

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/utils"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/filter"
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
		Use:   "bw-list [-w worker] [-T table-name] <config-file>",
		Short: "check the black-white-list info for tables",
		Run:   bwListFunc,
	}
	cmd.Flags().StringP("table-name", "T", "", "the table name we want to check for the black white list")
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
	if len(workers) > 1 {
		common.PrintLines("we want 0 or 1 worker, but get ", workers)
		return
	}

	result := &bwListResult{
		Result: true,
	}
	// no worker is specified, print all info
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

		doTableStringMap := make(map[string][]string, 0)
		ignoreTableStringMap := make(map[string][]string, 0)
		for _, sourceInfo := range resp.SourceInfo {
			bwListName := bwListMap[sourceInfo.SourceID]
			filterTableList := getFilterTableList(sourceInfo)
			bwFilter, err := filter.New(cfg.CaseSensitive, cfg.BWList[bwListName])
			if err != nil {
				common.PrintLines("build of black white filter failed:\n%s", errors.ErrorStack(err))
				return
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

		result.DoTables = doTableStringMap
		result.IgnoreTables = ignoreTableStringMap
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

		filtered := checkSingleBlackWhiteFilter(schema, table, cfg, cfg.BWList[mysqlInstance.BWListName])
		if filtered {
			result.WillBeFiltered = "yes"
		} else {
			result.WillBeFiltered = "no"
		}
	}
	common.PrettyPrintInterface(result)
}

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

func getCheckSchemaTableName(cmd *cobra.Command) (string, string, error) {
	tableName, err := cmd.Flags().GetString("table-name")
	if err != nil {
		return "", "", errors.Annotate(err, "get table-name arg failed")
	}
	if tableName == "" {
		return "", "", errors.New("argument table-name is not given. pls check it again.")
	}
	schema, table, err := utils.ExtractTable(tableName)
	if err != nil {
		return "", "", errors.Annotate(err, "extract table info failed")
	}
	return schema, table, nil
}

func getMySQLInstanceConfigThroughSourceID(cfg *config.TaskConfig, sourceID string) *config.MySQLInstance {
	var mysqlInstance *config.MySQLInstance
	for _, mysqlInst := range cfg.MySQLInstances {
		if mysqlInst.SourceID == sourceID {
			mysqlInstance = mysqlInst
			break
		}
	}
	return mysqlInstance
}

func getMySQLInstanceThroughWorker(worker, task string, cfg *config.TaskConfig) (*config.MySQLInstance, error) {
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
	mysqlInstance := getMySQLInstanceConfigThroughSourceID(cfg, sourceID)
	if mysqlInstance == nil {
		return nil, errors.New("the mysql instance config is not found. pls check the worker address")
	}
	return mysqlInstance, nil
}

func checkSingleBlackWhiteFilter(schema, table string, cfg *config.TaskConfig, rules *filter.Rules) bool {
	checkTable := []*filter.Table{{Schema: schema, Name: table}}
	bwFilter, err := filter.New(cfg.CaseSensitive, rules)
	if err != nil {
		common.PrintLines("build of black-white filter failed:\n%s", errors.ErrorStack(err))
	}
	checkTable = bwFilter.ApplyOn(checkTable)
	return len(checkTable) == 0
}
