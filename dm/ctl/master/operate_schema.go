// Copyright 2020 PingCAP, Inc.
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
	"os"

	"github.com/spf13/cobra"

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
)

// NewOperateSchemaCmd creates a OperateSchema command.
func NewOperateSchemaCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "operate-schema <operate-type> <-s source ...> <task-name> <-d database> <-t table> [schema-file]",
		Short: "get/set/remove the schema for an upstream table",
		Run:   operateSchemaCmd,
	}
	cmd.Flags().StringP("database", "d", "", "database name of the table")
	cmd.Flags().StringP("table", "t", "", "table name")
	return cmd
}

func convertSchemaOpType(t string) pb.SchemaOp {
	switch t {
	case "get":
		return pb.SchemaOp_GetSchema
	case "set":
		return pb.SchemaOp_SetSchema
	case "remove":
		return pb.SchemaOp_RemoveSchema
	default:
		return pb.SchemaOp_InvalidSchemaOp
	}
}

// operateSchemaCmd does the operate schema request.
func operateSchemaCmd(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) < 2 || len(cmd.Flags().Args()) > 3 {
		cmd.SetOut(os.Stdout)
		cmd.Usage()
		return
	}

	opType := cmd.Flags().Arg(0)
	taskName := cmd.Flags().Arg(1)
	schemaFile := cmd.Flags().Arg(2)
	var schemaContent []byte
	op := convertSchemaOpType(opType)
	switch op {
	case pb.SchemaOp_InvalidSchemaOp:
		common.PrintLines("invalid operate '%s' on schema", opType)
		return
	case pb.SchemaOp_SetSchema:
		if schemaFile == "" {
			common.PrintLines("must sepcify schema file for 'set' operation")
			return
		}
		var err error
		schemaContent, err = common.GetFileContent(schemaFile)
		if err != nil {
			common.PrintLines("get file content error:\n%v", err)
			return
		}
	default:
		if schemaFile != "" {
			common.PrintLines("schema file will be ignored for 'get'/'delete' operation")
		}
	}

	sources, err := common.GetSourceArgs(cmd)
	if err != nil {
		common.PrintLines("%v", err)
		return
	} else if len(sources) == 0 {
		common.PrintLines("must specify at least one source (`-s` / `--source`)")
		return
	}
	database, err := cmd.Flags().GetString("database")
	if err != nil {
		common.PrintLines("%v", err)
		return
	} else if database == "" {
		common.PrintLines("must specify 'database'")
		return
	}
	table, err := cmd.Flags().GetString("table")
	if err != nil {
		common.PrintLines("%v", err)
		return
	} else if table == "" {
		common.PrintLines("must specify 'table'")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli := common.MasterClient()
	resp, err := cli.OperateSchema(ctx, &pb.OperateSchemaRequest{
		Op:       op,
		Task:     taskName,
		Sources:  sources,
		Database: database,
		Table:    table,
		Schema:   string(schemaContent),
	})
	if err != nil {
		common.PrintLines("fail to operate schema:\n%v", err)
		return
	}
	common.PrettyPrintResponse(resp)
}
