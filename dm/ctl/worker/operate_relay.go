package worker

import (
	"github.com/pingcap/tidb-enterprise-tools/dm/ctl/common"
	"github.com/pingcap/tidb-enterprise-tools/dm/pb"
	"golang.org/x/net/context"
)

// operateRelay does operation on relay unit
func operateRelay(op pb.RelayOp) (*pb.OperateRelayResponse, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli := common.WorkerClient()
	return cli.OperateRelay(ctx, &pb.OperateRelayRequest{
		Op: op,
	})
}
