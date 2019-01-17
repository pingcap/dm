package master

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
)

// NewCheckTaskCmd creates a CheckTask command
func NewCheckTaskCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "check-task <config_file>",
		Short: "check a task with config file",
		Run:   checkTaskFunc,
	}
	return cmd
}

// checkTaskFunc does check task request
func checkTaskFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) != 1 {
		fmt.Println(cmd.Usage())
		return
	}
	content, err := common.GetFileContent(cmd.Flags().Arg(0))
	if err != nil {
		common.PrintLines("get file content error:\n%v", errors.ErrorStack(err))
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// start task
	cli := common.MasterClient()
	resp, err := cli.CheckTask(ctx, &pb.CheckTaskRequest{
		Task: string(content),
	})
	if err != nil {
		common.PrintLines("fail to check task:\n%v", errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
