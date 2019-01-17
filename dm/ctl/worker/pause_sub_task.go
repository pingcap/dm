package worker

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/spf13/cobra"
)

// NewPauseSubTaskCmd creates a PauseSubTask command
func NewPauseSubTaskCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pause-sub-task <sub_task_name>",
		Short: "pause a running sub task with name",
		Run:   pauseSubTaskFunc,
	}
	return cmd
}

// pauseSubTaskFunc does pause sub task request
func pauseSubTaskFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		fmt.Println(cmd.Usage())
		return
	}
	name := args[0]

	resp, err := operateSubTask(pb.TaskOp_Pause, name)
	if err != nil {
		common.PrintLines("can not pause sub task %s:\n%v", name, errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
