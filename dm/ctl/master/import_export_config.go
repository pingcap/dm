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
	"io/ioutil"
	"os"
	"path"

	"github.com/spf13/cobra"

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
)

var (
	taskDirname   = "tasks"
	sourceDirname = "sources"
	yamlSuffix    = ".yaml"
)

// NewExportCfgCmd creates a exportCfg command.
func NewExportCfgCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "export-config [--dir directory]",
		Short: "Export the configurations of sources and tasks.",
		RunE:  exportCfgFunc,
	}
	cmd.Flags().StringP("dir", "d", "configs", "specify the destinary directory, default is `./configs`")
	return cmd
}

// exportCfgFunc gets config.
func exportCfgFunc(cmd *cobra.Command, args []string) error {
	dir, err := cmd.Flags().GetString("dir")
	if err != nil {
		common.PrintLinesf("can not get directory")
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), common.GlobalConfig().RPCTimeout)
	defer cancel()

	resp := &pb.ImportExportCfgsResponse{}
	err = common.SendRequest(
		ctx,
		"ImportExportCfgs",
		&pb.ImportExportCfgsRequest{
			Op:  pb.CfgsOp_Export,
			Dir: dir,
		},
		&resp,
	)

	if err != nil {
		common.PrintLinesf("can not export configs to %s", dir)
		return err
	}

	if !resp.Result {
		common.PrettyPrintResponse(resp)
		return nil
	}

	if err = os.MkdirAll(dir, 0o755); err != nil {
		common.PrintLinesf("can not create directory %s", dir)
		return err
	}

	taskDir := path.Join(dir, taskDirname)
	if err = os.MkdirAll(taskDir, 0o755); err != nil {
		common.PrintLinesf("can not create directory of task configs %s", taskDir)
		return err
	}

	sourceDir := path.Join(dir, sourceDirname)
	if err = os.MkdirAll(sourceDirname, 0o755); err != nil {
		common.PrintLinesf("can not create directory of source configs %s", path.Join(dir+sourceDir))
		return err
	}

	for _, taskCfg := range resp.Tasks {
		taskFile := path.Join(taskDir, taskCfg.Name)
		taskFile += yamlSuffix
		err = ioutil.WriteFile(taskFile, []byte(taskCfg.Content), 0o644)
		if err != nil {
			common.PrintLinesf("can not write task config to file %s", taskFile)
			return err
		}
	}

	for _, sourceCfg := range resp.Sources {
		sourceFile := path.Join(sourceDir, sourceCfg.Name, ".yaml")
		sourceFile += yamlSuffix
		err = ioutil.WriteFile(sourceFile, []byte(sourceCfg.Content), 0o644)
		if err != nil {
			common.PrintLinesf("can not write source config to file %s", sourceFile)
			return err
		}
	}

	return nil
}
