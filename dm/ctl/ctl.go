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

package ctl

import (
	"fmt"

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/ctl/master"
	"github.com/pingcap/dm/pkg/log"

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
	"go.uber.org/zap/zapcore"
)

var (
	commandMasterFlags = CommandMasterFlags{}
	rootCmd            = &cobra.Command{
		Use:   "dmctl",
		Short: "DM control",
	}
)

// CommandMasterFlags are flags that used in all commands for dm-master
type CommandMasterFlags struct {
	workers []string // specify workers to control on these dm-workers
}

func init() {
	// --worker worker1 -w worker2 --worker=worker3,worker4 -w=worker5,worker6
	rootCmd.PersistentFlags().StringSliceVarP(&commandMasterFlags.workers, "worker", "w", []string{}, "DM-worker ID")
	rootCmd.AddCommand(
		master.NewStartTaskCmd(),
		master.NewStopTaskCmd(),
		master.NewPauseTaskCmd(),
		master.NewResumeTaskCmd(),
		master.NewCheckTaskCmd(),
		master.NewUpdateTaskCmd(),
		master.NewQueryStatusCmd(),
		master.NewQueryErrorCmd(),
		master.NewRefreshWorkerTasks(),
		master.NewSQLReplaceCmd(),
		master.NewSQLSkipCmd(),
		master.NewSQLInjectCmd(),
		master.NewShowDDLLocksCmd(),
		master.NewUnlockDDLLockCmd(),
		master.NewBreakDDLLockCmd(),
		master.NewSwitchRelayMasterCmd(),
		master.NewPauseRelayCmd(),
		master.NewResumeRelayCmd(),
		master.NewUpdateMasterConfigCmd(),
		master.NewUpdateRelayCmd(),
		master.NewPurgeRelayCmd(),
		master.NewMigrateRelayCmd(),
	)
}

// Init initializes dm-control
func Init(cfg *common.Config) error {
	// set the log level temporarily
	log.SetLevel(zapcore.InfoLevel)

	return errors.Trace(common.InitUtils(cfg))
}

// PrintUsage prints usage
func PrintUsage() {
	fmt.Println("Available Commands:")
	for _, cmd := range rootCmd.Commands() {
		fmt.Printf("\t%s\t%s\n", cmd.Name(), cmd.Use)
	}
}

// HasCommand represent whether rootCmd has this command
func HasCommand(name string) bool {
	for _, cmd := range rootCmd.Commands() {
		if name == cmd.Name() {
			return true
		}
	}
	return false
}

// Start starts running a command
func Start(args []string) {
	rootCmd.SetArgs(args)
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(rootCmd.UsageString())
	}
}
