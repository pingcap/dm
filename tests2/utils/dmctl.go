package utils

import (
	"context"
	"io/ioutil"
	"time"

	"github.com/juju/errors"
	"google.golang.org/grpc"

	"github.com/pingcap/tidb-enterprise-tools/dm/pb"
)

func CreateDmCtl(addr string) (pb.MasterClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBackoffMaxDelay(3*time.Second))
	if err != nil {
		return nil, errors.Trace(err)
	}
	return pb.NewMasterClient(conn), nil
}

func StartTask(ctx context.Context, cli pb.MasterClient, configFile string, workers []string) error {
	content, err := ioutil.ReadFile(configFile)
	if err != nil {
		return errors.Trace(err)
	}

	resp, err := cli.StartTask(ctx, &pb.StartTaskRequest{
		Task:    string(content),
		Workers: workers,
	})
	if err != nil {
		return errors.Trace(err)
	}

	for _, wp := range resp.GetWorkers() {
		if !wp.GetResult() {
			return errors.Errorf("fail to start task %v: %s", string(content), wp.GetMsg())
		}
	}

	return nil
}
