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
	_ "github.com/pingcap/tidb/types/parser_driver"
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
		Use:   "event_filter <-s sql> <config-file>",
		Short: "check whether the given sql will be filtered by binlog-event-filter",
		Run:   eventFilterFunc,
	}
	return cmd
}

func eventFilterFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) != 1 {
		fmt.Println(cmd.Usage())
		return
	}
	sql, err := cmd.Flags().GetString("sql")
	if err != nil {
		common.PrintLines("%s", errors.ErrorStack(err))
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

	filterName, action, err := filterEvent(sql, cfg.Filters)
	if err != nil {
		common.PrintLines("get filter's name failed:\n%v", errors.ErrorStack(err))
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

func filterEvent(sql string, filterMap map[string]*bf.BinlogEventRule) (string, bf.ActionType, error) {
	sql = utils.TrimCtrlChars(sql)
	doAction := bf.Do
	sqlParser := parser.New()
	for name, filterContent := range filterMap {
		stmts, _, err := sqlParser.Parse(sql, "", "")
		if len(stmts) > 1 {
			return "", doAction, errors.New("invalid sql, length is bigger than 1")
		} else if stmts == nil {
			return "", doAction, errors.New("invalid sql, length is 0")
		}

		n := stmts[0]
		et := bf.AstToDDLEvent(n)
		table := filter.Table{}
		genSchemaAndTable(&table, n)

		binlogFilter, err := bf.NewBinlogEvent(true, []*bf.BinlogEventRule{filterContent})
		if err != nil {
			return "", doAction, err
		}
		action, err := binlogFilter.Filter(table.Schema, table.Name, et, sql)
		if err != nil {
			return "", doAction, errors.Annotate(err, "filter table fails")
		}
		if action == bf.Ignore {
			return name, action, nil
		}
		// TODO: Check whether this table can match any event by this filter. If so, this sql is picked by this filter
	}
	return "", doAction, nil
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
