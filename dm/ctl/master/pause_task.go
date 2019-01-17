package master

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/spf13/cobra"
)

// NewPauseTaskCmd creates a PauseTask command
func NewPauseTaskCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pause-task [-w worker ...] <task_name>",
		Short: "pause a running task with name",
		Run:   pauseTaskFunc,
	}
	return cmd
}

// pauseTaskFunc does pause task request
func pauseTaskFunc(cmd *cobra.Command, _ []string) {
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

	resp, err := operateTask(pb.TaskOp_Pause, name, workers)
	if err != nil {
		common.PrintLines("can not pause task %s:\n%v", name, errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
