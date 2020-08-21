// Copyright 2020 PingCAP, Inc.
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
	"os"

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
)

// NewOfflineMemberCmd creates an OfflineWorker command
func NewOfflineMemberCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "offline-member <--master/--worker> <--name master-name/worker-name>",
		Short: "Offlines member which has been closed.",
		RunE:  offlineMemberFunc,
	}
	cmd.Flags().BoolP("master", "m", false, "to offline a master")
	cmd.Flags().BoolP("worker", "w", false, "to offline a worker")
	cmd.Flags().StringP("name", "n", "", "specify member name for choosing type")
	return cmd
}

func convertOfflineMemberType(cmd *cobra.Command) (string, error) {
	master, err := cmd.Flags().GetBool("master")
	if err != nil {
		return "", err
	}
	worker, err := cmd.Flags().GetBool("worker")
	if err != nil {
		return "", err
	}
	if (master && worker) || (!master && !worker) {
		return "", errors.New("should specify either --master or --worker")
	}
	if master {
		return common.Master, nil
	}
	return common.Worker, nil
}

// offlineMemberFunc does offline member request
func offlineMemberFunc(cmd *cobra.Command, _ []string) (err error) {
	if len(cmd.Flags().Args()) > 0 {
		cmd.SetOut(os.Stdout)
		cmd.Usage()
		err = errors.New("please check output to see error")
		return
	}

	offlineType, err := convertOfflineMemberType(cmd)
	if err != nil {
		common.PrintLines("get offline type failed")
		return
	}
	name, err := cmd.Flags().GetString("name")
	if err != nil {
		common.PrintLines("get offline name failed")
		return
	} else if name == "" {
		common.PrintLines("a member name must be specified")
		err = errors.New("please check output to see error")
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli := common.MasterClient()
	resp, err := cli.OfflineMember(ctx, &pb.OfflineMemberRequest{
		Type: offlineType,
		Name: name,
	})
	if err != nil {
		return
	}
	if !resp.Result {
		common.PrintLines("offline member failed:\n%v", resp.Msg)
		err = errors.New("please check output to see error")
		return
	}
	common.PrettyPrintResponse(resp)
	return
}
