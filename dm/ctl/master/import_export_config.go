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
	"io/ioutil"
	"os"
	"path"
	"sort"

	"github.com/spf13/cobra"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/pkg/ha"
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

	// get all source cfgs
	sourceCfgsMap, _, err := ha.GetSourceCfg(common.GlobalCtlClient.EtcdClient, "", 0)
	if err != nil {
		common.PrintLinesf("can not get source configs from etcd")
		return err
	}
	// try to get all source cfgs before v2.0.2
	if len(sourceCfgsMap) == 0 {
		sourceCfgsMap, _, err = ha.GetAllSourceCfgBeforeV202(common.GlobalCtlClient.EtcdClient)
		if err != nil {
			common.PrintLinesf("can not get source configs from etcd")
			return err
		}
	}

	// get all task configs.
	subTaskCfgsMap, _, err := ha.GetAllSubTaskCfg(common.GlobalCtlClient.EtcdClient)
	if err != nil {
		common.PrintLinesf("can not get subtask configs from etcd")
		return err
	}

	taskCfgsMap := make(map[string]string, len(subTaskCfgsMap))
	subTaskCfgsListMap := make(map[string][]*config.SubTaskConfig, len(subTaskCfgsMap))

	// from source => task => subtask to task => source => subtask
	for _, subTaskCfgs := range subTaskCfgsMap {
		for task, subTaskCfg := range subTaskCfgs {
			clone := subTaskCfg
			if subTaskCfgList, ok := subTaskCfgsListMap[task]; ok {
				subTaskCfgsListMap[task] = append(subTaskCfgList, &clone)
			} else {
				subTaskCfgsListMap[task] = []*config.SubTaskConfig{&clone}
			}
		}
	}
	// from task => source => subtask to task => taskCfg
	for task, subTaskCfgs := range subTaskCfgsListMap {
		sort.Slice(subTaskCfgs, func(i, j int) bool {
			return subTaskCfgs[i].SourceID < subTaskCfgs[j].SourceID
		})
		taskCfg := config.FromSubTaskConfigs(subTaskCfgs...)
		taskCfgsMap[task] = taskCfg.String()
	}

	// create directory
	if err = os.MkdirAll(dir, 0o755); err != nil {
		common.PrintLinesf("can not create directory `%s`", dir)
		return err
	}
	taskDir := path.Join(dir, taskDirname)
	if err = os.MkdirAll(taskDir, 0o755); err != nil {
		common.PrintLinesf("can not create directory of task configs `%s`", taskDir)
		return err
	}
	sourceDir := path.Join(dir, sourceDirname)
	if err = os.MkdirAll(sourceDir, 0o755); err != nil {
		common.PrintLinesf("can not create directory of source configs `%s`", sourceDir)
		return err
	}

	// write sourceCfg files
	for source, sourceCfg := range sourceCfgsMap {
		sourceFile := path.Join(sourceDir, source)
		sourceFile += yamlSuffix
		fileContent, err2 := sourceCfg.Yaml()
		if err2 != nil {
			common.PrintLinesf("fail to marshal source config of `%s`", source)
			return err2
		}
		err = ioutil.WriteFile(sourceFile, []byte(fileContent), 0o644)
		if err != nil {
			common.PrintLinesf("fail to write source config to file `%s`", sourceFile)
			return err
		}
	}

	// write taskCfg files
	for task, taskCfg := range taskCfgsMap {
		taskFile := path.Join(taskDir, task)
		taskFile += yamlSuffix
		err = ioutil.WriteFile(taskFile, []byte(taskCfg), 0o644)
		if err != nil {
			common.PrintLinesf("can not write task config to file `%s`", taskFile)
			return err
		}
	}

	common.PrintLinesf("export configs to directory `%s` succeed", dir)
	return nil
}
