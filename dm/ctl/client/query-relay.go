package client

import (
	"os"

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/terror"
)

func QueryRelay(config *common.Config) error {
	// TODO
	if len(cmd.Flags().Args()) > 1 {
		cmd.SetOut(os.Stdout)
		cmd.Usage()
		return
	}
	taskName := cmd.Flags().Arg(0) // maybe empty

	sources, err := common.GetSourceArgs(cmd)
	if err != nil {
		common.PrintLines("%s", terror.Message(err))
		return
	}

	cli := common.MasterClient()
	ctx, cancel := context.WithTimeout(context.Background(), common.GlobalConfig().RPCTimeout)
	defer cancel()
	resp, err := cli.QueryStatus(ctx, &pb.QueryStatusListRequest{
		Name:    taskName,
		Sources: sources,
	})
	if err != nil {
		common.PrintLines("can not query %s task's status(in sources %v):\n%s", taskName, sources, terror.Message(err))
		return
	}

	more, err := cmd.Flags().GetBool("more")
	if err != nil {
		common.PrintLines("%s", terror.Message(err))
		return
	}

	if resp.Result && taskName == "" && len(sources) == 0 && !more {
		result := wrapTaskResult(resp)
		common.PrettyPrintInterface(result)
	} else {
		common.PrettyPrintResponse(resp)
	}
	return nil
}
