package master

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/spf13/cobra"
)

// NewResumeTaskCmd creates a ResumeTask command
func NewResumeTaskCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "resume-task [-w worker ...] <task_name>",
		Short: "resume a paused task with name",
		Run:   resumeTaskFunc,
	}
	return cmd
}

// resumeTaskFunc does resume task request
func resumeTaskFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) != 1 {
		fmt.Println(cmd.Usage())
		return
	}
	name := cmd.Flags().Arg(0)

	workers, err := common.GetWorkerArgs(cmd)
	if err != nil {
		common.PrintLines("%s", errors.ErrorStack(err))
		return
	}

	resp, err := operateTask(pb.TaskOp_Resume, name, workers)
	if err != nil {
		common.PrintLines("can not resume task %s:\n%v", name, errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
