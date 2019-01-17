package master

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
)

// NewUpdateMasterConfigCmd creates a UpdateMasterConfig command
func NewUpdateMasterConfigCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update-master-config <config_file>",
		Short: "update configure of dm-master",
		Run:   updateMasterConfigFunc,
	}
	return cmd
}

func updateMasterConfigFunc(cmd *cobra.Command, _ []string) {
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

	cli := common.MasterClient()
	resp, err := cli.UpdateMasterConfig(ctx, &pb.UpdateMasterConfigRequest{
		Config: string(content),
	})
	if err != nil {
		common.PrintLines("can not update master config:\n%v", errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
