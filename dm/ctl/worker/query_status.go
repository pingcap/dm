package worker

import (
	"github.com/juju/errors"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
)

// NewQueryStatusCmd creates a QueryStatus command
// refine it to talk to dm-master later
func NewQueryStatusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "query-status [sub_task_name]",
		Short: "query task's status",
		Run:   queryStatusFunc,
	}
	return cmd
}

// queryStatusFunc does query task's status
func queryStatusFunc(_ *cobra.Command, args []string) {
	name := ""
	if len(args) > 0 {
		name = args[0]
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli := common.WorkerClient()
	resp, err := cli.QueryStatus(ctx, &pb.QueryStatusRequest{Name: name})
	if err != nil {
		common.PrintLines("can not query %s task's status:\n%v", name, errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
