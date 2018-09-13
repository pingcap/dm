// Copyright 2018 PingCAP, Inc.
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
	"fmt"
	"io/ioutil"

	"github.com/pingcap/tidb-enterprise-tools/configuration"
	yaml "gopkg.in/yaml.v2"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-enterprise-tools/dm/config"
	"github.com/pingcap/tidb-enterprise-tools/dm/ctl/common"
	"github.com/spf13/cobra"
)

// NewGenerateTaskConfigCmd creates a GenerateTaskConfig command
func NewGenerateTaskConfigCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "generate-task-config <config_file> <output-file>",
		Short: "generate a task config with config file",
		Run:   generateTaskConfigFunc,
	}
	return cmd
}

// generateTaskConfigFunc does generate task config
func generateTaskConfigFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) != 2 {
		fmt.Println(cmd.Usage())
		return
	}

	content, err := common.GetFileContent(cmd.Flags().Arg(0))
	if err != nil {
		common.PrintLines("get file content error:\n%v", errors.ErrorStack(err))
		return
	}

	outputFile := cmd.Flags().Arg(1)

	dmConfig := configuration.NewDataMigrationConfig()
	err = dmConfig.Decode(string(content))
	if err != nil {
		common.PrintLines("cant decode data migration config:\n%v", errors.ErrorStack(err))
		return
	}

	taskConfig, err := dmConfig.GenerateDMTask()
	if err != nil {
		common.PrintLines("fail to generate task config:\n%v", errors.ErrorStack(err))
		return
	}

	rawTaskByte, err := yaml.Marshal(taskConfig)
	if err != nil {
		common.PrintLines("fail to marshal task config:\n%v", errors.ErrorStack(err))
		return
	}

	err = ioutil.WriteFile(outputFile, rawTaskByte, 0700)
	if err != nil {
		common.PrintLines("fail to write task config file %s:\n%v", outputFile, errors.ErrorStack(err))
		return
	}

	common.PrintLines("generate task config file %s", outputFile)
}

// NewRestoreDataMigrationConfigCmd creates a restoreDataMigrationConfig command
func NewRestoreDataMigrationConfigCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "restore-dm-config <config_file> <output-file>",
		Short: "restore data migration config with task config file",
		Run:   restoreDataMigrationConfig,
	}
	return cmd
}

// restoreDataMigrationConfig does restore a data migration config
func restoreDataMigrationConfig(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) != 2 {
		fmt.Println(cmd.Usage())
		return
	}

	content, err := common.GetFileContent(cmd.Flags().Arg(0))
	if err != nil {
		common.PrintLines("get file content error:\n%v", errors.ErrorStack(err))
		return
	}

	outputFile := cmd.Flags().Arg(1)

	taskConfig := config.NewTaskConfig()
	err = taskConfig.Decode(string(content))
	if err != nil {
		common.PrintLines("cant decode task config:\n%v", errors.ErrorStack(err))
		return
	}

	dmConfig := configuration.NewDataMigrationConfig()
	err = dmConfig.DecodeFromTask(taskConfig)
	if err != nil {
		common.PrintLines("fail to generate data migration config:\n%v", errors.ErrorStack(err))
		return
	}

	rawDMByte, err := yaml.Marshal(dmConfig)
	if err != nil {
		common.PrintLines("fail to marshal data migration config:\n%v", errors.ErrorStack(err))
		return
	}

	err = ioutil.WriteFile(outputFile, rawDMByte, 0700)
	if err != nil {
		common.PrintLines("fail to write task config file %s:\n%v", outputFile, errors.ErrorStack(err))
		return
	}

	common.PrintLines("generate data migration config file %s", outputFile)
}
