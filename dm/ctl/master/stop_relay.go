package master

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-enterprise-tools/dm/ctl/common"
	"github.com/pingcap/tidb-enterprise-tools/dm/pb"
	"github.com/spf13/cobra"
)

// NewStopRelayCmd creates a StopRelay command
func NewStopRelayCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stop-relay <-w worker ...>",
		Short: "stop dm-worker's relay unit",
		Run:   stopRelayFunc,
	}
	return cmd
}

// stopRelayFunc does stop relay request
func stopRelayFunc(cmd *cobra.Command, _ []string) {
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

	resp, err := operateRelay(pb.RelayOp_StopRelay, workers)
	if err != nil {
		common.PrintLines("can not stop relay unit:\n%v", errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
