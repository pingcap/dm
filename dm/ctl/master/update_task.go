package master

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
)

// NewUpdateTaskCmd creates a UpdateTask command
func NewUpdateTaskCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update-task [-w worker ...] <config_file>",
		Short: "update a task's config for routes, filters, column-mappings, black-white-list",
		Run:   updateTaskFunc,
	}
	return cmd
}

// updateTaskFunc does update task request
func updateTaskFunc(cmd *cobra.Command, _ []string) {
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
	if err != nil {
		common.PrintLines("%s", errors.ErrorStack(err))
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// update task
	cli := common.MasterClient()
	resp, err := cli.UpdateTask(ctx, &pb.UpdateTaskRequest{
		Task:    string(content),
		Workers: workers,
	})
	if err != nil {
		common.PrintLines("can not update task:\n%v", errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
