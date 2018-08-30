// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"fmt"
	"io/ioutil"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/tidb-enterprise-tools/dm/pb"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var (
	workerClient pb.WorkerClient
	masterClient pb.MasterClient
)

// InitClient initializes dm-worker client or dm-master client
func InitClient(addr string, isWorkerAddr bool) error {
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBackoffMaxDelay(3*time.Second))
	if err != nil {
		return errors.Trace(err)
	}
	if isWorkerAddr {
		workerClient = pb.NewWorkerClient(conn)
	} else {
		masterClient = pb.NewMasterClient(conn)
	}
	return nil
}

// WorkerClient returns dm-worker client
func WorkerClient() pb.WorkerClient {
	return workerClient
}

// MasterClient returns dm-master client
func MasterClient() pb.MasterClient {
	return masterClient
}

// PrintLines adds a wrap to support `\n` within `chzyer/readline`
func PrintLines(format string, a ...interface{}) {
	fmt.Println(fmt.Sprintf(format, a...))
}

// PrettyPrintResponse prints a PRC response prettily
func PrettyPrintResponse(resp proto.Message) {
	// encoding/json does not support proto Enum well
	mar := jsonpb.Marshaler{EmitDefaults: true, Indent: "    "}
	s, err := mar.MarshalToString(resp)
	if err != nil {
		PrintLines(errors.ErrorStack(err))
	}
	fmt.Println(s)
}

// GetFileContent reads and returns file's content
func GetFileContent(fpath string) ([]byte, error) {
	content, err := ioutil.ReadFile(fpath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return content, nil
}

// GetWorkerArgs extracts workers from cmd
func GetWorkerArgs(cmd *cobra.Command) ([]string, error) {
	return cmd.Flags().GetStringSlice("worker")
}
