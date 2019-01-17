package master

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
)

// NewRefreshWorkerTasks creates a RefreshWorkerTasks command
func NewRefreshWorkerTasks() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "refresh-worker-tasks",
		Short: "refresh worker -> tasks mapper",
		Run:   refreshWorkerTasksFunc,
	}
	return cmd
}

// refreshWorkerTasksFunc does refresh workerTasks request
func refreshWorkerTasksFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) > 0 {
		fmt.Println(cmd.Usage())
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli := common.MasterClient()
	resp, err := cli.RefreshWorkerTasks(ctx, &pb.RefreshWorkerTasksRequest{})
	if err != nil {
		common.PrintLines("can not refresh workerTasks:\n%v", errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
