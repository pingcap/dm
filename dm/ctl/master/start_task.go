package master

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
)

// NewStartTaskCmd creates a StartTask command
func NewStartTaskCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start-task [-w worker ...] <config_file>",
		Short: "start a task with config file",
		Run:   startTaskFunc,
	}
	return cmd
}

// startTaskFunc does start task request
func startTaskFunc(cmd *cobra.Command, _ []string) {
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

	// start task
	cli := common.MasterClient()
	resp, err := cli.StartTask(ctx, &pb.StartTaskRequest{
		Task:    string(content),
		Workers: workers,
	})
	if err != nil {
		common.PrintLines("can not start task:\n%v", errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
