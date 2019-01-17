package worker

import (
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"golang.org/x/net/context"
)

// operateSubTask does operation on sub task
func operateSubTask(op pb.TaskOp, name string) (*pb.OperateSubTaskResponse, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli := common.WorkerClient()
	return cli.OperateSubTask(ctx, &pb.OperateSubTaskRequest{
		Op:   op,
		Name: name,
	})
}
