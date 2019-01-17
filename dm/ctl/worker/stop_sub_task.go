package worker

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/spf13/cobra"
)

// NewStopSubTaskCmd creates a StopSubTask command
// refine it to talk to dm-master later
func NewStopSubTaskCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stop-sub-task <sub_task_name>",
		Short: "stop a running sub task with name",
		Run:   stopSubTaskFunc,
	}
	return cmd
}

// stopSubTaskFunc does stop sub task request
func stopSubTaskFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		fmt.Println(cmd.Usage())
		return
	}
	name := args[0]

	resp, err := operateSubTask(pb.TaskOp_Stop, name)
	if err != nil {
		common.PrintLines("can not stop sub task %s:\n%v", name, errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
