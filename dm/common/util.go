package common

import (
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"sync/atomic"
	"time"
)

var (
	masterClient atomic.Value
	invalidClient = InvalidClient{}
)

// Stub struct used when no valid masterClient is available
type InvalidClient struct {}

// InitClient initializes dm-master client
func InitClient(addrs []string, block bool) error {
	var err error
	var conn *grpc.ClientConn
	ops := []grpc.DialOption{grpc.WithInsecure()}
	if block {
		ops = append(ops, grpc.WithBlock(), grpc.WithTimeout(1*time.Second))
	} else {
		ops = append(ops, grpc.WithBackoffMaxDelay(3*time.Second))
	}
	for _, addr := range addrs {
		conn, err = grpc.Dial(addr, ops...)
		if err == nil {
			break
		}
		log.L().Warn("try to create gRPC connect failed", zap.String("address", addr), zap.Error(err))
	}

	if err != nil {
		masterClient.Store(&invalidClient)
		return errors.Trace(err)
	}
	masterClient.Store(pb.NewMasterClient(conn))
	return nil
}

// ResetMasterClient reset masterClient when no master is available
func ResetMasterClient() {
	masterClient.Store(&invalidClient)
}

// MasterClient returns dm-master client
func MasterClient() pb.MasterClient {
	if client, ok := masterClient.Load().(pb.MasterClient); ok {
		return client
	}
	return nil
}
