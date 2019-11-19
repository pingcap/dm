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
	"fmt"
	"strings"

	"github.com/pingcap/dm/pkg/utils"

	"github.com/pingcap/tidb-tools/pkg/filter"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
)

type bwListResult struct {
	Result       bool                           `json:"result"`
	Msg          string                         `json:"msg"`
	Routes       map[string]map[string][]string `json:"routes,omitempty"`
	MatchRoute   string                         `json:"match-route,omitempty"`
	TargetSchema string                         `json:"target-schema,omitempty"`
	TargetTable  string                         `json:"target-table,omitempty"`
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
		fmt.Println(errors.ErrorStack(err))
		return
	}
	if len(workers) > 1 {
		fmt.Println("too many workers are given. We can o nly check one worker and one table at one time.")
		return
	}

	result := bwListResult{}
	// no worker is specified, print all info
	if len(workers) == 0 {

	} else {
		table, err := parseTableName(cmd)
		if err != nil {
			fmt.Println(errors.ErrorStack(err))
		}
		fmt.Println(table)
	}

	common.PrettyPrintInterface(result)
}

// FIXME: a simple implementation to parse table name. Is there any better way to do this? Or we have to separate table-name arg to two args.
func parseTableName(cmd *cobra.Command) (*filter.Table, error) {
	tableName, err := cmd.Flags().GetString("table-name")
	if err != nil {
		return nil, nil
	}

	tableName = utils.TrimCtrlChars(tableName)
	t := strings.Index(tableName, "`.`")
	if strings.Count(tableName, "`.`") > 1 || t == -1 || t <= 1 || len(tableName)-1 <= t+3 {
		return nil, errors.New("invalid table name, please check it again")
	}
	return &filter.Table{
		Schema: tableName[1:t],
		Name:   tableName[t+3 : len(tableName)-1],
	}, nil
}
