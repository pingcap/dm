package master

import (
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"golang.org/x/net/context"
)

// operateTask does operation on task
func operateTask(op pb.TaskOp, name string, workers []string) (*pb.OperateTaskResponse, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli := common.MasterClient()
	return cli.OperateTask(ctx, &pb.OperateTaskRequest{
		Op:      op,
		Name:    name,
		Workers: workers,
	})
}
