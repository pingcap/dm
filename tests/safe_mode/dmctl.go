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

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/tests/utils"
)

func main() {
	cli, err := utils.CreateDmCtl("127.0.0.1:8261")
	if err != nil {
		utils.ExitWithError(err)
	}
	conf := os.Args[1]
	// sometimes TCP port is available but RPC call to DM-worker is failed, we
	// simply retry here.
	retry := 5
	for i := 0; i < retry; i++ {
		err = utils.StartTask(context.Background(), cli, conf, nil)
		if err != nil {
			log.Infof("start task failed with error: %s, retry\n", err)
			time.Sleep(time.Second)
		} else {
			break
		}
	}
	if err != nil {
		utils.ExitWithError(err)
	}
}
