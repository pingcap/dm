package worker

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
)

// NewUpdateSubTaskCmd creates a UpdateSubTask command
func NewUpdateSubTaskCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update-sub-task <config_file>",
		Short: "update a sub task's config for routes, filters, column-mappings, black-white-list",
		Run:   updateSubTaskFunc,
	}
	return cmd
}

// updateSubTaskFunc does update sub task request
func updateSubTaskFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		fmt.Println(cmd.UsageString())
		return
	}

	content, err := common.GetFileContent(args[0])
	if err != nil {
		common.PrintLines("get file content error:\n%v", errors.ErrorStack(err))
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// NOTE: do whole check now, refine to do TablesChecker and ShardingTablesCheck ?
	err = checkSubTask(ctx, string(content))
	if err != nil {
		common.PrintLines("precheck failed %s", errors.ErrorStack(err))
		return
	}

	cli := common.WorkerClient()
	resp, err := cli.UpdateSubTask(ctx, &pb.UpdateSubTaskRequest{Task: string(content)})
	if err != nil {
		common.PrintLines("can not update sub task:\n%v", errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
