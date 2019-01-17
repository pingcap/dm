package master

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/spf13/cobra"
)

// NewStopTaskCmd creates a StopTask command
func NewStopTaskCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stop-task [-w worker ...] <task_name>",
		Short: "stop a task with name",
		Run:   stopTaskFunc,
	}
	return cmd
}

// stopTaskFunc does stop task request
func stopTaskFunc(cmd *cobra.Command, _ []string) {
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

	resp, err := operateTask(pb.TaskOp_Stop, name, workers)
	if err != nil {
		common.PrintLines("can not stop task %s:\n%v", name, errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
