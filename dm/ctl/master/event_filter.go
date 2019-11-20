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

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tidb-tools/pkg/filter"
	_ "github.com/pingcap/tidb/types/parser_driver" // for import parser driver
	"github.com/spf13/cobra"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/pkg/utils"
)

type eventFilterResult struct {
	Result     bool   `json:"result"`
	Msg        string `json:"msg"`
	Filtered   bool   `json:"will-be-filtered"`
	FilterName string `json:"filter-name,omitempty"`
}

// NewEventFilterCmd creates a EventFilter command
func NewEventFilterCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "event-filter <config-file> <sql>",
		Short: "check whether the given sql will be filtered by binlog-event-filter",
		Run:   eventFilterFunc,
	}
	return cmd
}

func eventFilterFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) < 2 {
		fmt.Println(cmd.Usage())
		return
	}
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

	extraArgs := cmd.Flags().Args()[1:]
	realSQLs, err := common.ExtractSQLsFromArgs(extraArgs)
	if err != nil {
		common.PrintLines("%s", errors.ErrorStack(err))
		return
	}
	if len(realSQLs) > 1 {
		common.PrintLines("Too many sqls are given. Simunator can only check one sql at one time")
	}
	sql := realSQLs[0]

	filterName, action, err := filterEvent(sql, cfg.Filters, cfg.CaseSensitive)
	if err != nil {
		common.PrintLines("get filter info failed:\n%v", errors.ErrorStack(err))
		return
	}
	result := eventFilterResult{
		Result:     true,
		Msg:        "",
		Filtered:   action == bf.Ignore,
		FilterName: filterName,
	}

	common.PrettyPrintInterface(result)
}

func filterEvent(sql string, filterMap map[string]*bf.BinlogEventRule, caseSensitive bool) (string, bf.ActionType, error) {
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
			return "", bf.Ignore, errors.Annotate(err, "creating single filter failed")
		}

		action, err := singleFilter.Filter(table.Schema, table.Name, et, sql)
		if err != nil {
			return "", bf.Ignore, errors.Annotate(err, "singleFilter failed in filtering table")
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
