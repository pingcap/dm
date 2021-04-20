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
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"

	"github.com/spf13/cobra"
)

// NewOperateSourceCmd creates a OperateSource command.
func NewOperateSourceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "operate-source <operate-type> [config-file ...] [--print-sample-config]",
		Short: "`create`/`update`/`stop`/`show` upstream MySQL/MariaDB source.",
		RunE:  operateSourceFunc,
	}
	cmd.Flags().BoolP("print-sample-config", "p", false, "print sample config file of source")
	return cmd
}

func convertCmdType(t string) pb.SourceOp {
	switch t {
	case "create":
		return pb.SourceOp_StartSource
	case "update":
		return pb.SourceOp_UpdateSource
	case "stop":
		return pb.SourceOp_StopSource
	case "show":
		return pb.SourceOp_ShowSource
	default:
		return pb.SourceOp_InvalidSourceOp
	}
}

// operateMysqlFunc does migrate relay request.
func operateSourceFunc(cmd *cobra.Command, _ []string) error {
	printSampleConfig, err := cmd.Flags().GetBool("print-sample-config")
	if err != nil {
		common.PrintLinesf("error in parse `--print-sample-config`")
		return err
	}

	if printSampleConfig {
		if strings.TrimSpace(config.SampleConfigFile) == "" {
			fmt.Println("sample config file of source is empty")
		} else {
			var rawConfig []byte
			rawConfig, err = base64.StdEncoding.DecodeString(config.SampleConfigFile)
			if err != nil {
				fmt.Println("base64 decode config error")
			} else {
				fmt.Println(string(rawConfig))
			}
		}
		return nil
	}

	if len(cmd.Flags().Args()) < 1 {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.New("please check output to see error")
	}

	cmdType := cmd.Flags().Arg(0)
	op := convertCmdType(cmdType)
	if op == pb.SourceOp_InvalidSourceOp {
		common.PrintLinesf("invalid operate '%s' on worker", cmdType)
		return errors.New("please check output to see error")
	}
	if op != pb.SourceOp_ShowSource && len(cmd.Flags().Args()) == 1 {
		common.PrintLinesf("operate-source create/update/stop should specify config-file(s)")
		return errors.New("please check output to see error")
	}

	contents := make([]string, 0, len(cmd.Flags().Args())-1)
	sourceID := make([]string, 0, len(cmd.Flags().Args())-1)
	sources, err := common.GetSourceArgs(cmd)
	if err != nil {
		return err
	}
	sourceID = append(sourceID, sources...)

	for i := 1; i < len(cmd.Flags().Args()); i++ {
		arg := cmd.Flags().Arg(i)
		var content []byte
		content, err = common.GetFileContent(arg)
		if err != nil {
			if op == pb.SourceOp_StopSource {
				sourceID = append(sourceID, arg)
				continue
			}
			return err
		}
		contents = append(contents, string(content))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp := &pb.OperateSourceResponse{}
	err = common.SendRequest(
		ctx,
		"OperateSource",
		&pb.OperateSourceRequest{
			Config:   contents,
			Op:       op,
			SourceID: sourceID,
		},
		&resp,
	)

	if err != nil {
		return err
	}

	common.PrettyPrintResponse(resp)
	return nil
}
