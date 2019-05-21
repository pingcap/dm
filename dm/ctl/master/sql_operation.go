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
	"github.com/pingcap/errors"
	"github.com/spf13/cobra"

	"github.com/pingcap/dm/dm/command"
)

// extractBinlogPosSQLPattern extracts binlog-pos and sql-pattern from command
// return binlog-pos, sql-pattern, sharding, error
func extractBinlogPosSQLPattern(cmd *cobra.Command) (string, string, bool, error) {
	binlogPos, err := cmd.Flags().GetString("binlog-pos")
	if err != nil {
		return "", "", false, errors.Trace(err)
	}

	sqlPattern, err := cmd.Flags().GetString("sql-pattern")
	if err != nil {
		return "", "", false, errors.Trace(err)
	}

	sharding, err := cmd.Flags().GetBool("sharding")
	if err != nil {
		return "", "", false, errors.Trace(err)
	}

	_, _, err = command.VerifySQLOperateArgs(binlogPos, sqlPattern, sharding)
	return binlogPos, sqlPattern, sharding, errors.Trace(err)
}
