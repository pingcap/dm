package master

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
)

// NewUnlockDDLLockCmd creates a UnlockDDLLock command
func NewUnlockDDLLockCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "unlock-ddl-lock [-w worker ...] <lock-ID>",
		Short: "force to unlock DDL lock",
		Run:   unlockDDLLockFunc,
	}
	cmd.Flags().StringP("owner", "o", "", "dm-worker to replace the default owner")
	cmd.Flags().BoolP("force-remove", "f", false, "force to remove DDL lock")
	return cmd
}

// unlockDDLLockFunc does unlock DDL lock
func unlockDDLLockFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) != 1 {
		fmt.Println(cmd.Usage())
		return
	}
	owner, err := cmd.Flags().GetString("owner")
	if err != nil {
		fmt.Println(errors.ErrorStack(err))
		return
	}

	lockID := cmd.Flags().Arg(0)

	workers, err := common.GetWorkerArgs(cmd)
	if err != nil {
		fmt.Println(errors.ErrorStack(err))
		return
	}

	forceRemove, err := cmd.Flags().GetBool("force-remove")
	if err != nil {
		fmt.Println(errors.ErrorStack(err))
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli := common.MasterClient()
	resp, err := cli.UnlockDDLLock(ctx, &pb.UnlockDDLLockRequest{
		ID:           lockID,
		ReplaceOwner: owner,
		Workers:      workers,
		ForceRemove:  forceRemove,
	})
	if err != nil {
		common.PrintLines("can not unlock DDL lock %s (in workers %v):\n%s", lockID, workers, errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
