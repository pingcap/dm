package worker

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/spf13/cobra"
)

// NewResumeSubTaskCmd creates a ResumeSubTask command
func NewResumeSubTaskCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "resume-sub-task <sub_task_name>",
		Short: "resume a paused sub task with name",
		Run:   resumeSubTaskFunc,
	}
	return cmd
}

// resumeSubTaskFunc does resume sub task request
func resumeSubTaskFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		fmt.Println(cmd.Usage())
		return
	}
	name := args[0]

	resp, err := operateSubTask(pb.TaskOp_Resume, name)
	if err != nil {
		common.PrintLines("can not resume sub task %s:\n%v", name, errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
