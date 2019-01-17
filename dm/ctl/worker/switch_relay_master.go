package worker

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
)

// NewSwitchRelayMasterCmd creates a SwitchRelayMaster command
func NewSwitchRelayMasterCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "switch-relay-master",
		Short: "switch master server of dm-worker's relay unit",
		Run:   switchRelayMasterFunc,
	}
	return cmd
}

// switchRelayMasterFunc does switch relay master server
func switchRelayMasterFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) > 0 {
		fmt.Println(cmd.Usage())
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli := common.WorkerClient()
	resp, err := cli.SwitchRelayMaster(ctx, &pb.SwitchRelayMasterRequest{})
	if err != nil {
		common.PrintLines("can not switch relay's master server:\n%s", errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
