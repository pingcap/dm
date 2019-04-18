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
	"fmt"
	"os"
	"strconv"

	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/tests/utils"
)

func main() {
	if len(os.Args) != 3 {
		utils.ExitWithError(fmt.Errorf("invlid args: %v", os.Args))
	}
	cli, err := utils.CreateDmCtl("127.0.0.1:8261")
	if err != nil {
		utils.ExitWithError(err)
	}
	op, err := strconv.ParseInt(os.Args[1], 10, 64)
	if err != nil {
		utils.ExitWithError(err)
	}
	taskName := os.Args[2]
	err = utils.OperateTask(context.Background(), cli, pb.TaskOp(op), taskName, nil)
	if err != nil {
		utils.ExitWithError(err)
	}
}
