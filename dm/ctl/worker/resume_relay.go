package worker

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-enterprise-tools/dm/ctl/common"
	"github.com/pingcap/tidb-enterprise-tools/dm/pb"
	"github.com/spf13/cobra"
)

// NewResumeRelayCmd creates a ResumeRelay command
func NewResumeRelayCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "resume-relay",
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

	resp, err := operateRelay(pb.RelayOp_ResumeRelay)
	if err != nil {
		common.PrintLines("can not resume relay unit:\n%v", errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
