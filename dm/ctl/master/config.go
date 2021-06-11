// Copyright 2021 PingCAP, Inc.
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
	"github.com/pingcap/dm/dm/pb"

	"github.com/spf13/cobra"
)

// NewConfigCmd creates a OperateSchema command.
func NewConfigCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config <command>",
		Short: "manage config operations",
	}
	cmd.AddCommand(
		newConfigTaskCmd(),
		newConfigSourceCmd(),
		newConfigMasterCmd(),
		newConfigWorkerCmd(),
	)
	cmd.PersistentFlags().StringP("output", "o", "", "write config to file")
	return cmd
}

func newConfigTaskCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "task [task-name]",
		Short: "manage or show task configs",
		RunE:  configTaskList,
	}
	cmd.AddCommand(
		newConfigTaskUpdateCmd(),
	)
	return cmd
}

func configTaskList(cmd *cobra.Command, args []string) error {
	if len(args) == 0 || len(args) > 1 {
		return cmd.Help()
	}
	name := args[0]
	output, err := cmd.Flags().GetString("output")
	if err != nil {
		return err
	}
	return sendGetConfigRequest(pb.CfgType_TaskType, name, output)
}

// FIXME: implement this later.
func newConfigTaskUpdateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update <command>",
		Short: "update config task",
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	}
	return cmd
}

func newConfigSourceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "source [source-name]",
		Short: "manage or show source config",
		RunE:  configSourceList,
	}
	cmd.AddCommand(
		newConfigSourceUpdateCmd(),
	)
	return cmd
}

func configSourceList(cmd *cobra.Command, args []string) error {
	if len(args) == 0 || len(args) > 1 {
		return cmd.Help()
	}
	name := args[0]
	output, err := cmd.Flags().GetString("output")
	if err != nil {
		return err
	}
	return sendGetConfigRequest(pb.CfgType_SourceType, name, output)
}

// FIXME: implement this later.
func newConfigSourceUpdateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update <command>",
		Short: "update config source",
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	}
	return cmd
}

func newConfigMasterCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "master [master-name]",
		Short: "manage or show master configs",
		RunE:  configMasterList,
	}
	return cmd
}

func configMasterList(cmd *cobra.Command, args []string) error {
	if len(args) == 0 || len(args) > 1 {
		return cmd.Help()
	}
	name := args[0]
	output, err := cmd.Flags().GetString("output")
	if err != nil {
		return err
	}
	return sendGetConfigRequest(pb.CfgType_MasterType, name, output)
}

func newConfigWorkerCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "worker [worker-name]",
		Short: "manage or show worker configs",
		RunE:  configWorkerList,
	}
	return cmd
}

func configWorkerList(cmd *cobra.Command, args []string) error {
	if len(args) == 0 || len(args) > 1 {
		return cmd.Help()
	}
	name := args[0]
	output, err := cmd.Flags().GetString("output")
	if err != nil {
		return err
	}
	return sendGetConfigRequest(pb.CfgType_WorkerType, name, output)
}
