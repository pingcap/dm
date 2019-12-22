package common

import (
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"time"
)

var (
	masterClient pb.MasterClient
)

// InitClient initializes dm-master client
func InitClient(addrs []string) error {
	var err error
	var conn *grpc.ClientConn
	for _, addr := range addrs {
		conn, err = grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBackoffMaxDelay(3*time.Second))
		if err == nil {
			break
		}
		log.L().Warn("try to create gRPC connect failed", zap.String("address", addr), zap.Error(err))
	}

	if err != nil {
		masterClient = nil
		return errors.Trace(err)
	}
	masterClient = pb.NewMasterClient(conn)
	return nil
}

// MasterClient returns dm-master client
func MasterClient() pb.MasterClient {
	return masterClient
}
