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
	"errors"
	"os"
	"strconv"

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/spf13/cobra"
)

// NewMigrateRelayCmd creates a MigrateRelay command
func NewMigrateRelayCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate-relay <source> <binlogName> <binlogPos>",
		Short: "migrate DM-worker's relay unit",
		RunE:  migrateRelayFunc,
	}
	return cmd
}

// MigrateRealyFunc does migrate relay request
func migrateRelayFunc(cmd *cobra.Command, _ []string) (err error) {
	if len(cmd.Flags().Args()) != 3 {
		cmd.SetOut(os.Stdout)
		cmd.Usage()
		err = errors.New("please check output to see error")
		return
	}

	source := cmd.Flags().Arg(0)
	binlogName := cmd.Flags().Arg(1)
	binlogPos, err := strconv.Atoi(cmd.Flags().Arg(2))
	if err != nil {
		common.PrintLines("%v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli := common.MasterClient()
	resp, err := cli.MigrateWorkerRelay(ctx, &pb.MigrateWorkerRelayRequest{
		BinlogName: binlogName,
		BinlogPos:  uint32(binlogPos),
		Source:     source,
	})
	if err != nil {
		return
	}

	common.PrettyPrintResponse(resp)
	return
}
