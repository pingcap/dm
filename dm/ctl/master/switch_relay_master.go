package master

import (
	"context"
	"fmt"

	"github.com/juju/errors"
	"github.com/spf13/cobra"

	"github.com/pingcap/tidb-enterprise-tools/dm/ctl/common"
	"github.com/pingcap/tidb-enterprise-tools/dm/pb"
)

// NewSwitchRelayMasterCmd creates a SwitchRelayMaster command
func NewSwitchRelayMasterCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "switch-relay-master <-w worker ...>",
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

	workers, err := common.GetWorkerArgs(cmd)
	if err != nil {
		fmt.Println(errors.ErrorStack(err))
		return
	}
	if len(workers) == 0 {
		fmt.Println("must specify at least one dm-worker (`-w` / `--worker`)")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli := common.MasterClient()
	resp, err := cli.SwitchWorkerRelayMaster(ctx, &pb.SwitchWorkerRelayMasterRequest{
		Workers: workers,
	})
	if err != nil {
		common.PrintLines("can not switch relay's master server (in workers %v):\n%s", workers, errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
