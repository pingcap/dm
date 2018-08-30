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

package ctl

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-enterprise-tools/dm/ctl/common"
	"github.com/pingcap/tidb-enterprise-tools/dm/ctl/master"
	"github.com/pingcap/tidb-enterprise-tools/dm/ctl/worker"
	"github.com/spf13/cobra"
)

var (
	isWorkerCtl        bool
	commandMasterFlags = CommandMasterFlags{}
)

// CommandMasterFlags are flags that used in all commands for dm-master
type CommandMasterFlags struct {
	workers []string // specify workers to control on these dm-workers
}

// Init initializes dm-control
func Init(cfg *common.Config) error {
	isWorkerCtl = cfg.IsWorkerAddr
	return errors.Trace(common.InitClient(cfg.ServerAddr, cfg.IsWorkerAddr))
}

// Start starts running a command
func Start(args []string) {
	rootCmd := &cobra.Command{
		Use:   "dmctl",
		Short: "DM control",
	}

	if isWorkerCtl {
		rootCmd.AddCommand(
			worker.NewStartSubTaskCmd(),
			worker.NewStopSubTaskCmd(),
			worker.NewPauseSubTaskCmd(),
			worker.NewResumeSubTaskCmd(),
			worker.NewQueryStatusCmd(),
		)
	} else {
		// --worker worker1 -w worker2 --worker=worker3,worker4 -w=worker5,worker6
		rootCmd.PersistentFlags().StringSliceVarP(&commandMasterFlags.workers, "worker", "w", []string{}, "dm-worker ID")
		rootCmd.AddCommand(
			master.NewStartTaskCmd(),
			master.NewStopTaskCmd(),
			master.NewPauseTaskCmd(),
			master.NewResumeTaskCmd(),
			master.NewQueryStatusCmd(),
			master.NewRefreshWorkerTasks(),
		)
	}

	rootCmd.SetArgs(args)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(rootCmd.UsageString())
	}
}
