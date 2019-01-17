package master

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/spf13/cobra"
)

// NewResumeRelayCmd creates a ResumeRelay command
func NewResumeRelayCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "resume-relay <-w worker ...>",
		Short: "resume dm-worker's relay unit",
		Run:   resumeRelayFunc,
	}
	return cmd
}

// resumeRelayFunc does resume relay request
func resumeRelayFunc(cmd *cobra.Command, _ []string) {
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

	resp, err := operateRelay(pb.RelayOp_ResumeRelay, workers)
	if err != nil {
		common.PrintLines("can not resume relay unit:\n%v", errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
