package master

import (
	"fmt"
	"github.com/prometheus/common/log"
	"strconv"

	"github.com/juju/errors"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"

	"github.com/pingcap/tidb-enterprise-tools/dm/ctl/common"
	"github.com/pingcap/tidb-enterprise-tools/dm/pb"
)

// NewMigrateRelayCmd creates a MigrateRelay command
func NewMigrateRelayCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate-relay <worker> <binlogName> <binlogPos>",
		Short: "migrate dm-worker's relay unit",
		Run:   migrateRelayFunc,
	}
	return cmd
}

// MigrateRealyFunc does migrate relay request
func migrateRelayFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) != 3 {
		fmt.Println(cmd.Usage())
		return
	}

	worker := cmd.Flags().Arg(0)
	binlogName := cmd.Flags().Arg(1)
	binlogPos, err := strconv.Atoi(cmd.Flags().Arg(2))
	if err != nil {
		common.PrintLines(errors.ErrorStack(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli := common.MasterClient()
	resp, err := cli.MigrateWorkerRelay(ctx, &pb.MigrateWorkerRelayRequest{
		BinlogName: string(binlogName),
		BinlogPos:  uint32(binlogPos),
		Worker:     worker,
	})

	if err != nil {
		log.Infof("can not migrate relay config:\n%v", errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
