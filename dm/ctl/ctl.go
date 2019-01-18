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

	"github.com/juju/errors"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/ctl/master"
	"github.com/pingcap/dm/dm/ctl/worker"
	"github.com/pingcap/dm/pkg/log"
	"github.com/spf13/cobra"
)

var (
	mode               common.DmctlMode
	commandMasterFlags = CommandMasterFlags{}
)

// CommandMasterFlags are flags that used in all commands for dm-master
type CommandMasterFlags struct {
	workers []string // specify workers to control on these dm-workers
}

// Init initializes dm-control
func Init(cfg *common.Config) error {
	// set the log level temporarily
	log.SetLevelByString("info")
	mode = cfg.Mode
	return errors.Trace(common.InitClient(cfg.ServerAddr, cfg.Mode))
}

// Start starts running a command
func Start(args []string) {
	rootCmd := &cobra.Command{
		Use:   "dmctl",
		Short: "DM control",
	}

	switch mode {
	case common.WorkerMode:
		rootCmd.AddCommand(
			worker.NewStartSubTaskCmd(),
			worker.NewStopSubTaskCmd(),
			worker.NewPauseSubTaskCmd(),
			worker.NewResumeSubTaskCmd(),
			worker.NewUpdateSubTaskCmd(),
			worker.NewQueryStatusCmd(),
			worker.NewQueryErrorCmd(),
			worker.NewBreakDDLLockCmd(),
			worker.NewSwitchRelayMasterCmd(),
			worker.NewPauseRelayCmd(),
			worker.NewResumeRelayCmd(),
			//worker.NewStopRelayCmd(),
		)
	case common.MasterMode:
		// --worker worker1 -w worker2 --worker=worker3,worker4 -w=worker5,worker6
		rootCmd.PersistentFlags().StringSliceVarP(&commandMasterFlags.workers, "worker", "w", []string{}, "dm-worker ID")
		rootCmd.AddCommand(
			master.NewStartTaskCmd(),
			master.NewStopTaskCmd(),
			master.NewPauseTaskCmd(),
			master.NewResumeTaskCmd(),
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
			//master.NewStopRelayCmd(),
			master.NewUpdateMasterConfigCmd(),
			master.NewUpdateRelayCmd(),
			master.NewPurgeRelayCmd(),
		)
	case common.OfflineMode:
		rootCmd.AddCommand(
			master.NewCheckTaskCmd(),
		)
	}

	rootCmd.SetArgs(args)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(rootCmd.UsageString())
	}
}
