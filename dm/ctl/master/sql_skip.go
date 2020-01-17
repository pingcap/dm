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
	"os"
	"strings"

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
)

// NewSQLSkipCmd creates a SQLSkip command
func NewSQLSkipCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sql-skip <-s source> [-b binlog-pos] [-p sql-pattern] [--sharding] <task-name>",
		Short: "skip the binlog event matched by a specific binlog position (binlog-pos) or a SQL pattern (sql-pattern)",
		Run:   sqlSkipFunc,
	}
	cmd.Flags().StringP("binlog-pos", "b", "", "position used to match binlog event if matched the sql-skip operation will be applied. The format like \"mysql-bin|000001.000003:3270\"")
	cmd.Flags().StringP("sql-pattern", "p", "", "SQL pattern used to match the DDL converted by optional router-rules if matched the sql-skip operation will be applied. The format like \"~(?i)ALTER\\s+TABLE\\s+`db1`.`tbl1`\\s+ADD\\s+COLUMN\\s+col1\\s+INT\". Whitespace is not supported, and must be replaced by \"\\s\". Staring with ~ as regular expression. This can only be used for DDL (converted by optional router-rules), and if multi DDLs in one binlog event, one of them matched is enough, but all of them will be skipped")
	cmd.Flags().BoolP("sharding", "", false, "whether are handing sharding DDL, which will only take effect on DDL lock's owner")
	return cmd
}

// sqlSkipFunc does sql skip request
func sqlSkipFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) != 1 {
		cmd.SetOut(os.Stdout)
		cmd.Usage()
		return
	}

	binlogPos, sqlPattern, sharding, err := extractBinlogPosSQLPattern(cmd)
	if err != nil {
		common.PrintLines("%s", err.Error())
		return
	}

	var source string
	sources, err := common.GetSourceArgs(cmd)
	if err != nil {
		common.PrintLines("%s", errors.ErrorStack(err))
		return
	}
	if sharding {
		if len(sources) != 0 {
			common.PrintLines("--sharding operator always takes effect on DDL lock's owner, specified sources %v arguments will be ignored", sources)
		}
	} else {
		if len(sources) != 1 {
			common.PrintLines("should only specify one source, but got %v", sources)
			return
		}
		source = sources[0]
	}

	taskName := cmd.Flags().Arg(0)
	if strings.TrimSpace(taskName) == "" {
		common.PrintLines("must specify the task-name")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli := common.MasterClient()

	resp, err := cli.HandleSQLs(ctx, &pb.HandleSQLsRequest{
		Name:       taskName,
		Source:     source,
		Op:         pb.SQLOp_SKIP,
		BinlogPos:  binlogPos,
		SqlPattern: sqlPattern,
		Sharding:   sharding,
	})
	if err != nil {
		common.PrintLines("can not skip SQL:\n%v", errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
