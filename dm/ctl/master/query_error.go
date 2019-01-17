package master

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
)

// NewQueryErrorCmd creates a QueryError command
func NewQueryErrorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "query-error [-w worker ...] [task_name]",
		Short: "query task's error",
		Run:   queryErrorFunc,
	}
	return cmd
}

// queryErrorFunc does query task's error
func queryErrorFunc(cmd *cobra.Command, _ []string) {
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
	resp, err := cli.QueryError(ctx, &pb.QueryErrorListRequest{
		Name:    taskName,
		Workers: workers,
	})
	if err != nil {
		common.PrintLines("dmctl query error failed")
		if taskName != "" {
			common.PrintLines("taskname: %s", taskName)
		}
		if len(workers) > 0 {
			common.PrintLines("workers: %v", workers)
		}
		common.PrintLines("error: %s", errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
