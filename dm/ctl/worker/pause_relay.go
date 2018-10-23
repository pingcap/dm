package worker

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
		Use:   "pause-relay",
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

	resp, err := operateRelay(pb.RelayOp_PauseRelay)
	if err != nil {
		common.PrintLines("can not pause relay unit:\n%v", errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
