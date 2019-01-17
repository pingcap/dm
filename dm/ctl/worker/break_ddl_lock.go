package worker

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
)

// NewBreakDDLLockCmd creates a BreakDDLLock command
func NewBreakDDLLockCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "break-ddl-lock <task-name>",
		Short: "force to break dm-worker's DDL lock",
		Run:   breakDDLLockFunc,
	}
	cmd.Flags().StringP("remove-id", "i", "", "DDLLockInfo's ID which need to remove")
	cmd.Flags().BoolP("exec", "e", false, "whether execute DDL which is blocking")
	cmd.Flags().BoolP("skip", "s", false, "whether skip DDL which in blocking")
	return cmd
}

// breakDDLLockFunc does break DDL lock
func breakDDLLockFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) != 1 {
		fmt.Println(cmd.Usage())
		return
	}
	taskName := cmd.Flags().Arg(0)

	removeLockID, err := cmd.Flags().GetString("remove-id")
	if err != nil {
		fmt.Println(errors.ErrorStack(err))
		return
	}

	exec, err := cmd.Flags().GetBool("exec")
	if err != nil {
		fmt.Println(errors.ErrorStack(err))
		return
	}

	skip, err := cmd.Flags().GetBool("skip")
	if err != nil {
		fmt.Println(errors.ErrorStack(err))
		return
	}

	if len(removeLockID) == 0 && !exec && !skip {
		fmt.Println("`remove-id`, `exec`, `skip` must specify at least one")
		return
	}

	if exec && skip {
		fmt.Println("`exec` and `skip` can not specify both at the same time")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli := common.WorkerClient()
	resp, err := cli.BreakDDLLock(ctx, &pb.BreakDDLLockRequest{
		Task:         taskName,
		RemoveLockID: removeLockID,
		ExecDDL:      exec,
		SkipDDL:      skip,
	})
	if err != nil {
		common.PrintLines("can not break DDL lock :\n%s", errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
