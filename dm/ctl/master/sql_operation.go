package master

import (
	"github.com/juju/errors"
	"github.com/spf13/cobra"

	"github.com/pingcap/tidb-enterprise-tools/dm/command"
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
