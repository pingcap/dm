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

// NewGetCfgCmd creates a getCfg command
func NewGetCfgCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get-config <task | master | worker | source> <name> [--file filename]",
		Short: "Gets the configuration.",
		RunE:  getCfgFunc,
	}
	cmd.Flags().StringP("file", "f", "", "write config to file")
	return cmd
}

func convertCfgType(t string) pb.CfgType {
	switch t {
	case "task":
		return pb.CfgType_TaskType
	case "master":
		return pb.CfgType_MasterType
	case "worker":
		return pb.CfgType_WorkerType
	case "source":
		return pb.CfgType_SourceType
	default:
		return pb.CfgType_InvalidType
	}
}

// getCfgFunc gets config
func getCfgFunc(cmd *cobra.Command, _ []string) (err error) {
	if len(cmd.Flags().Args()) != 2 {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		err = errors.New("please check output to see error")
		return
	}

	cfgType := cmd.Flags().Arg(0)
	tp := convertCfgType(cfgType)
	if tp == pb.CfgType_InvalidType {
		common.PrintLinesf("invalid config type '%s'", cfgType)
		err = errors.New("please check output to see error")
		return
	}

	cfgName := cmd.Flags().Arg(1)
	filename, err := cmd.Flags().GetString("file")
	if err != nil {
		common.PrintLinesf("can not get filename")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), common.GlobalConfig().RPCTimeout)
	defer cancel()

	resp := &pb.GetCfgResponse{}
	err = common.SendRequest(
		ctx,
		"GetCfg",
		&pb.GetCfgRequest{
			Type: tp,
			Name: cfgName,
		},
		&resp,
	)
	if err != nil {
		common.PrintLinesf("can not get %s config of %s", cfgType, cfgName)
		return
	}

	if resp.Result && len(filename) != 0 {
		err = ioutil.WriteFile(filename, []byte(resp.Cfg), 0o644)
		if err != nil {
			common.PrintLinesf("can not write config to file %s", filename)
			return
		}
		resp.Msg = fmt.Sprintf("write config to file %s succeed", filename)
		resp.Cfg = ""
	}
	common.PrettyPrintResponse(resp)
	return
}
