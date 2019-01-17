package worker

import (
	"github.com/juju/errors"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
)

// NewQueryErrorCmd creates a QueryError command
// refine it to talk to dm-master later
func NewQueryErrorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "query-error [sub_task_name]",
		Short: "query task's exec error",
		Run:   queryErrorFunc,
	}
	return cmd
}

// queryErrorFunc does query task's exec error
func queryErrorFunc(_ *cobra.Command, args []string) {
	name := ""
	if len(args) > 0 {
		name = args[0]
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli := common.WorkerClient()
	resp, err := cli.QueryError(ctx, &pb.QueryErrorRequest{Name: name})
	if err != nil {
		common.PrintLines("can not query %s task's error:\n%v", name, errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
