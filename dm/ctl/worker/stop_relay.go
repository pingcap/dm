package worker

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/spf13/cobra"
)

// NewStopRelayCmd creates a StopRelay command
func NewStopRelayCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stop-relay",
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

	resp, err := operateRelay(pb.RelayOp_StopRelay)
	if err != nil {
		common.PrintLines("can not stop relay unit:\n%v", errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
