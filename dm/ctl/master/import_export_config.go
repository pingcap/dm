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
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/pingcap/errors"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/ha"
	"github.com/pingcap/dm/pkg/utils"
)

var (
	taskDirname          = "tasks"
	sourceDirname        = "sources"
	relayWorkersFilename = "relay_workers.json"
	yamlSuffix           = ".yaml"
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

// NewImportCfgCmd creates a importCfg command.
func NewImportCfgCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "import-config [--dir directory]",
		Short: "Import the configurations of sources and tasks.",
		RunE:  importCfgFunc,
	}
	cmd.Flags().StringP("dir", "d", "configs", "specify the configs directory, default is `./configs`")
	return cmd
}

// exportCfgFunc exports configs.
func exportCfgFunc(cmd *cobra.Command, args []string) error {
	dir, err := cmd.Flags().GetString("dir")
	if err != nil {
		common.PrintLinesf("can not get directory")
		return err
	}

	cli := common.GlobalCtlClient.EtcdClient
	// get all source cfgs
	sourceCfgsMap, _, err := ha.GetSourceCfg(cli, "", 0)
	if err != nil {
		common.PrintLinesf("can not get source configs from etcd")
		return err
	}
	// try to get all source cfgs before v2.0.2
	if len(sourceCfgsMap) == 0 {
		sourceCfgsMap, _, err = ha.GetAllSourceCfgBeforeV202(cli)
		if err != nil {
			common.PrintLinesf("can not get source configs from etcd")
			return err
		}
	}

	// get all task configs.
	subTaskCfgsMap, _, err := ha.GetAllSubTaskCfg(cli)
	if err != nil {
		common.PrintLinesf("can not get subtask configs from etcd")
		return err
	}

	// get all relay configs.
	relayWorkers, _, err := ha.GetAllRelayConfig(cli)
	if err != nil {
		common.PrintLinesf("can not get relay workers from etcd")
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

	if len(relayWorkers) > 0 {
		relayWorkers, err := json.Marshal(relayWorkers)
		if err != nil {
			common.PrintLinesf("fail to marshal relay workers")
			return err
		}

		relayWorkersFile := path.Join(dir, relayWorkersFilename)
		err = ioutil.WriteFile(relayWorkersFile, relayWorkers, 0o644)
		if err != nil {
			common.PrintLinesf("can not write relay workers to file `%s`", relayWorkersFile)
			return err
		}
	}

	common.PrintLinesf("export configs to directory `%s` succeed", dir)
	return nil
}

// importCfgFunc imports configs.
func importCfgFunc(cmd *cobra.Command, args []string) error {
	dir, err := cmd.Flags().GetString("dir")
	if err != nil {
		common.PrintLinesf("can not get directory")
		return err
	}

	// get all source cfgs
	if !utils.IsDirExists(dir) {
		return errors.Errorf("config directory `%s` not exists", dir)
	}

	var (
		sourceCfgs        []string
		taskCfgs          []string
		relayWorkers      map[string]map[string]struct{}
		taskDir           = path.Join(dir, taskDirname)
		taskDirExist      = utils.IsDirExists(taskDir)
		sourceDir         = path.Join(dir, sourceDirname)
		sourceDirExist    = utils.IsDirExists(sourceDir)
		relayWorkersFile  = path.Join(dir, relayWorkersFilename)
		relayWorkersExist = utils.IsFileExists(relayWorkersFile)
		sourceResp        = &pb.OperateSourceResponse{}
		taskResp          = &pb.StartTaskResponse{}
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if sourceDirExist {
		if sourceCfgs, err = collectDirCfgs(sourceDir); err != nil {
			common.PrintLinesf("fail to collect source config files from source configs directory `%s`", sourceDir)
			return err
		}
	}
	if taskDirExist {
		if taskCfgs, err = collectDirCfgs(taskDir); err != nil {
			common.PrintLinesf("fail to collect task config files from task configs directory `%s`", taskDir)
			return err
		}
	}
	if relayWorkersExist {
		content, err2 := common.GetFileContent(relayWorkersFile)
		if err2 != nil {
			common.PrintLinesf("fail to read relay workers config `%s`", relayWorkersFile)
			return err2
		}
		err = json.Unmarshal(content, &relayWorkers)
		if err != nil {
			common.PrintLinesf("fail to unmarshal relay workers config `%s`", relayWorkersFile)
			return err
		}
	}

	if len(sourceCfgs) > 0 {
		common.PrintLinesf("start creating sources")
	}

	// Do not use batch for `operate-source start source1, source2` if we want to support idemponent import-config.
	// Because `operate-source start` will revert all batch sources if any source error.
	// e.g. ErrSchedulerSourceCfgExist
	for _, sourceCfg := range sourceCfgs {
		err = common.SendRequest(
			ctx,
			"OperateSource",
			&pb.OperateSourceRequest{
				Config: []string{sourceCfg},
				Op:     pb.SourceOp_StartSource,
			},
			&sourceResp,
		)

		if err != nil {
			common.PrintLinesf("fail to create sources")
			return err
		}

		if !sourceResp.Result && !strings.Contains(sourceResp.Msg, "already exist") {
			common.PrettyPrintResponse(sourceResp)
			return errors.Errorf("fail to create sources")
		}
	}

	if len(taskCfgs) > 0 {
		common.PrintLinesf("start creating tasks")
	}

	for _, taskCfg := range taskCfgs {
		err = common.SendRequest(
			ctx,
			"StartTask",
			&pb.StartTaskRequest{
				Task: taskCfg,
			},
			&taskResp,
		)

		if err != nil {
			common.PrintLinesf("fail to create tasks")
			return err
		}

		if !taskResp.Result && !strings.Contains(taskResp.Msg, "already exist") {
			common.PrettyPrintResponse(taskResp)
			return errors.Errorf("fail to create tasks")
		}
	}

	if len(relayWorkers) > 0 {
		common.PrintLinesf("start creating relay workers")
	}

	for source, workerSet := range relayWorkers {
		workers := make([]string, 0, len(workerSet))
		for worker := range workerSet {
			workers = append(workers, worker)
		}
		resp := &pb.OperateRelayResponse{}
		err = common.SendRequest(
			ctx,
			"OperateRelay",
			&pb.OperateRelayRequest{
				Op:     pb.RelayOpV2_StartRelayV2,
				Source: source,
				Worker: workers,
			},
			&resp,
		)

		if err != nil {
			return err
		}
	}

	common.PrintLinesf("import configs from directory `%s` succeed", dir)
	return nil
}

func collectDirCfgs(dir string) ([]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	cfgs := make([]string, 0, len(files))
	for _, f := range files {
		cfg, err2 := common.GetFileContent(path.Join(dir, f.Name()))
		if err2 != nil {
			return nil, err2
		}
		cfgs = append(cfgs, string(cfg))
	}
	return cfgs, nil
}
