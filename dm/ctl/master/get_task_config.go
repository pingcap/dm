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
	"errors"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/spf13/cobra"

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
)

// NewGetTaskCfgCmd creates a getTaskCfg command
func NewGetTaskCfgCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get-task-config <task-name | task-file> [--file filename]",
		Short: "Gets the task configuration.",
		RunE:  getTaskCfgFunc,
	}
	cmd.Flags().StringP("file", "f", "", "write config to file")
	return cmd
}

// getTaskCfgFunc does get task's config
func getTaskCfgFunc(cmd *cobra.Command, _ []string) (err error) {
	if len(cmd.Flags().Args()) != 1 {
		cmd.SetOut(os.Stdout)
		cmd.Usage()
		err = errors.New("please check output to see error")
		return
	}
	taskName := common.GetTaskNameFromArgOrFile(cmd.Flags().Arg(0))
	filename, err := cmd.Flags().GetString("file")
	if err != nil {
		common.PrintLines("can not get filename")
		return
	}

	cli := common.MasterClient()
	ctx, cancel := context.WithTimeout(context.Background(), common.GlobalConfig().RPCTimeout)
	defer cancel()

	resp, err := cli.GetTaskCfg(ctx, &pb.GetTaskCfgRequest{
		Name: taskName,
	})
	if err != nil {
		common.PrintLines("can not get config of task %s", taskName)
		return
	}

	if resp.Result && len(filename) != 0 {
		err = ioutil.WriteFile(filename, []byte(resp.Cfg), 0644)
		if err != nil {
			common.PrintLines("can not write config to file %s", filename)
			return
		}
		resp.Msg = fmt.Sprintf("write config to file %s succeed", filename)
		resp.Cfg = ""
	}
	common.PrettyPrintResponse(resp)
	return
}
