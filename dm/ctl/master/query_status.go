package master

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
)

// NewQueryStatusCmd creates a QueryStatus command
func NewQueryStatusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "query-status [-w worker ...] [task_name]",
		Short: "query task's status",
		Run:   queryStatusFunc,
	}
	return cmd
}

// queryStatusFunc does query task's status
func queryStatusFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) > 1 {
		fmt.Println(cmd.Usage())
		return
	}
	taskName := cmd.Flags().Arg(0) // maybe empty

	workers, err := common.GetWorkerArgs(cmd)
	if err != nil {
		common.PrintLines("%s", errors.ErrorStack(err))
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli := common.MasterClient()
	resp, err := cli.QueryStatus(ctx, &pb.QueryStatusListRequest{
		Name:    taskName,
		Workers: workers,
	})
	if err != nil {
		common.PrintLines("can not query %s task's status(in workers %v):\n%s", taskName, workers, errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
