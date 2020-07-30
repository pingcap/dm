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

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"

	"github.com/spf13/cobra"
)

// NewQueryErrorCmd creates a QueryError command
func NewQueryErrorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "query-error [-s source ...] [task-name]",
		Short: "query task error",
		RunE:  queryErrorFunc,
	}
	return cmd
}

// queryErrorFunc does query task's error
func queryErrorFunc(cmd *cobra.Command, _ []string) (err error) {
	if len(cmd.Flags().Args()) > 1 {
		cmd.SetOut(os.Stdout)
		cmd.Usage()
		err = errors.New("please check output to see error")
		return
	}
	taskName := cmd.Flags().Arg(0) // maybe empty

	sources, err := common.GetSourceArgs(cmd)
	if err != nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli := common.MasterClient()
	resp, err := cli.QueryError(ctx, &pb.QueryErrorListRequest{
		Name:    taskName,
		Sources: sources,
	})
	if err != nil {
		common.PrintLines("dmctl query error failed")
		if taskName != "" {
			common.PrintLines("taskname: %s", taskName)
		}
		if len(sources) > 0 {
			common.PrintLines("sources: %v", sources)
		}
		return
	}

	common.PrettyPrintResponse(resp)
	return
}
