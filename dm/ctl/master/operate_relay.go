package master

import (
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"golang.org/x/net/context"
)

// operateRelay does operation on relay unit
func operateRelay(op pb.RelayOp, workers []string) (*pb.OperateWorkerRelayResponse, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli := common.MasterClient()
	return cli.OperateWorkerRelayTask(ctx, &pb.OperateWorkerRelayRequest{
		Op:      op,
		Workers: workers,
	})
}
