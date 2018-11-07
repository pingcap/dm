package master

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"

	"github.com/pingcap/tidb-enterprise-tools/dm/ctl/common"
	"github.com/pingcap/tidb-enterprise-tools/dm/pb"
)

// NewUpdateRelayCmd creates a UpdateRelay command
func NewUpdateRelayCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update-relay [-w worker ...] <config_file>",
		Short: "update dm-worker's relay unit configure",
		Run:   updateRelayFunc,
	}
	return cmd
}

// updateRealyFunc does update relay request
func updateRelayFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) != 1 {
		fmt.Println(cmd.Usage())
		return
	}

	content, err := common.GetFileContent(cmd.Flags().Arg(0))
	if err != nil {
		common.PrintLines("get file content error:\n%v", errors.ErrorStack(err))
		return
	}

	workers, err := common.GetWorkerArgs(cmd)
	if len(workers) != 1 {
		fmt.Println("must specify one dm-worker (`-w` / `--worker`)")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli := common.MasterClient()
	resp, err := cli.UpdateWorkerRelayConfig(ctx, &pb.UpdateWorkerRelayConfigRequest{
		Config: string(content),
		Worker: workers[0],
	})

	if err != nil {
		common.PrintLines("can not update relay config:\n%v", errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
