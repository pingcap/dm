// Copyright 2019 PingCAP, Inc.
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

package main

import (
	"context"
	"os"
	"time"

	"google.golang.org/grpc"

	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/errors"
)

// use query status request to test DM-worker is online
func main() {
	addr := os.Args[1]
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBackoffMaxDelay(2*time.Second))
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}
	cli := pb.NewWorkerClient(conn)
	req := &pb.QueryStatusRequest{}
	_, err = cli.QueryStatus(context.Background(), req)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}
}
