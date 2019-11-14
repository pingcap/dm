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

package common

import (
	"github.com/pingcap/dm/dm/pb"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
)

// OperateRelay does operation on relay unit
func OperateRelay(op pb.RelayOp, workers []string) (proto.Message, error) {	
	request := &pb.OperateWorkerRelayRequest{
		Op:      op,
		Workers: workers,
	}

	requestBytes, err := request.Marshal()
	if err != nil {
		PrintLines("marshal request error: \n%v", errors.ErrorStack(err))
		// FIXME: use terror
		return nil, errors.New("marshal failed")
	}

	return SendRequest(pb.CommandType_OperateWorkerRelay, requestBytes)
}
