package master

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-enterprise-tools/dm/ctl/common"
	"github.com/pingcap/tidb-enterprise-tools/dm/pb"
	"github.com/spf13/cobra"
)

// NewPauseRelayCmd creates a PauseRelay command
func NewPauseRelayCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pause-relay <-w worker ...>",
		Short: "pause dm-worker's relay unit",
		Run:   pauseRelayFunc,
	}
	return cmd
}

// pauseRelayFunc does pause relay request
func pauseRelayFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) > 0 {
		fmt.Println(cmd.Usage())
		return
	}

	workers, err := common.GetWorkerArgs(cmd)
	if err != nil {
		common.PrintLines("%s", errors.ErrorStack(err))
		return
	}
	if len(workers) == 0 {
		fmt.Println("must specify at least one dm-worker (`-w` / `--worker`)")
		return
	}

	resp, err := operateRelay(pb.RelayOp_PauseRelay, workers)
	if err != nil {
		common.PrintLines("can not pause relay unit:\n%v", errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
