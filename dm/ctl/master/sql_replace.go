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

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
)

// NewSQLReplaceCmd creates a SQLReplace command
func NewSQLReplaceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sql-replace <-w worker> [-b binlog-pos] [-s sql-pattern] [--sharding] <task-name> <sql1;sql2;>",
		Short: "sql-replace replaces SQL in specific binlog-pos or sql-pattern matched with other SQLs, each SQL must ends with semicolon;",
		Run:   sqlReplaceFunc,
	}
	cmd.Flags().StringP("binlog-pos", "b", "", "position used to match binlog event if matched the sql-replace operation will be applied. The format like \"mysql-bin|000001.000003:3270\"")
	cmd.Flags().StringP("sql-pattern", "s", "", "SQL pattern used to match the DDL converted by optional router-rules if matched the sql-replace operation will be applied. The format like \"~(?i)ALTER\\s+TABLE\\s+`db1`.`tbl1`\\s+ADD\\s+COLUMN\\s+col1\\s+INT\". Whitespace is not supported, and must be replaced by \"\\s\". Staring with ~ as regular expression. This can only be used for DDL (converted by optional router-rules), and if multi DDLs in one binlog event, one of them matched is enough, but all of them will be replaced")
	cmd.Flags().BoolP("sharding", "", false, "whether are handing sharding DDL, which will only take effect on DDL lock's owner")
	return cmd
}

// sqlReplaceFunc does sql replace request
func sqlReplaceFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) < 2 {
		fmt.Println(cmd.Usage())
		return
	}

	binlogPos, sqlPattern, sharding, err := extractBinlogPosSQLPattern(cmd)
	if err != nil {
		common.PrintLines("%s", err.Error())
		return
	}

	var worker string
	workers, err := common.GetWorkerArgs(cmd)
	if err != nil {
		common.PrintLines("%s", errors.ErrorStack(err))
		return
	}
	if sharding {
		if len(workers) != 0 {
			common.PrintLines("--sharding operator always takes effect on DDL lock's owner, specified workers %v arguments will be ignored", workers)
		}
	} else {
		if len(workers) != 1 {
			common.PrintLines("should only specify one worker, but got %v", workers)
			return
		}
		worker = workers[0]
	}

	taskName := cmd.Flags().Arg(0)
	if strings.TrimSpace(taskName) == "" {
		common.PrintLines("must specify the task-name")
		return
	}

	extraArgs := cmd.Flags().Args()[1:]
	realSQLs, err := common.ExtractSQLsFromArgs(extraArgs)
	if err != nil {
		common.PrintLines("check SQLs error: %s", errors.ErrorStack(err))
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli := common.MasterClient()
	resp, err := cli.HandleSQLs(ctx, &pb.HandleSQLsRequest{
		Name:       taskName,
		Worker:     worker,
		Op:         pb.SQLOp_REPLACE,
		Args:       realSQLs,
		BinlogPos:  binlogPos,
		SqlPattern: sqlPattern,
		Sharding:   sharding,
	})
	if err != nil {
		common.PrintLines("can not replace SQL:\n%v", errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
