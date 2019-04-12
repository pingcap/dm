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

package master

import (
	"context"
	"fmt"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/prometheus/common/log"
	"github.com/spf13/cobra"

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
)

// NewMigrateRelayCmd creates a MigrateRelay command
func NewMigrateRelayCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate-relay <worker> <binlogName> <binlogPos>",
		Short: "migrate dm-worker's relay unit",
		Run:   migrateRelayFunc,
	}
	return cmd
}

// MigrateRealyFunc does migrate relay request
func migrateRelayFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) != 3 {
		fmt.Println(cmd.Usage())
		return
	}

	worker := cmd.Flags().Arg(0)
	binlogName := cmd.Flags().Arg(1)
	binlogPos, err := strconv.Atoi(cmd.Flags().Arg(2))
	if err != nil {
		common.PrintLines(errors.ErrorStack(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli := common.MasterClient()
	resp, err := cli.MigrateWorkerRelay(ctx, &pb.MigrateWorkerRelayRequest{
		BinlogName: binlogName,
		BinlogPos:  uint32(binlogPos),
		Worker:     worker,
	})

	if err != nil {
		log.Infof("can not migrate relay config:\n%v", errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
