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
		Use:   "bw-list [-w worker] [-T table] <config-file>",
		Short: "check the black-white-list info for tables",
		Run:   bwListFunc,
	}
	cmd.Flags().StringP("table", "T", "", "the table name we want to check for the black white list")
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
		common.PrintLines("we want 0 or 1 worker, but get %v", workers)
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

		bwListMap := make(map[string]*filter.Rules, len(cfg.MySQLInstances))
		for _, inst := range cfg.MySQLInstances {
			bwListMap[inst.SourceID] = cfg.BWList[inst.BWListName]
		}

		result.DoTables, result.IgnoreTables, err = getDoIgnoreTables(cfg.CaseSensitive, bwListMap, resp.SourceInfo)
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
			result.WillBeFiltered = "no"
		}
	}
	common.PrettyPrintInterface(result)
}
