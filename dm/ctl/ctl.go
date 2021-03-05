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

package ctl

import (
	"fmt"
	"os"

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/ctl/master"
	"github.com/pingcap/dm/pkg/log"

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
	"go.uber.org/zap/zapcore"
)

var (
	commandMasterFlags = CommandMasterFlags{}
	rootCmd            *cobra.Command
)

// CommandMasterFlags are flags that used in all commands for dm-master
type CommandMasterFlags struct {
	workers []string // specify workers to control on these dm-workers
}

// Reset clears cache of CommandMasterFlags
func (c CommandMasterFlags) Reset() {
	c.workers = c.workers[:0]
}

func init() {
	rootCmd = NewRootCmd()
}

// NewRootCmd generates a new rootCmd
func NewRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "dmctl",
		Short:        "DM control",
		SilenceUsage: true,
	}
	// --worker worker1 -w worker2 --worker=worker3,worker4 -w=worker5,worker6
	cmd.PersistentFlags().StringSliceVarP(&commandMasterFlags.workers, "source", "s", []string{}, "MySQL Source ID.")
	cmd.AddCommand(
		master.NewStartTaskCmd(),
		master.NewStopTaskCmd(),
		master.NewPauseTaskCmd(),
		master.NewResumeTaskCmd(),
		master.NewCheckTaskCmd(),
		//	master.NewUpdateTaskCmd(),
		master.NewQueryStatusCmd(),
		master.NewShowDDLLocksCmd(),
		master.NewUnlockDDLLockCmd(),
		master.NewPauseRelayCmd(),
		master.NewResumeRelayCmd(),
		master.NewPurgeRelayCmd(),
		master.NewOperateSourceCmd(),
		master.NewOfflineMemberCmd(),
		master.NewOperateLeaderCmd(),
		master.NewListMemberCmd(),
		master.NewOperateSchemaCmd(),
		master.NewGetCfgCmd(),
		master.NewHandleErrorCmd(),
	)
	// copied from (*cobra.Command).InitDefaultHelpCmd
	helpCmd := &cobra.Command{
		Use:   "help [command]",
		Short: "Gets help about any command.",
		Long: `Help provides help for any command in the application.
Simply type ` + cmd.Name() + ` help [path to command] for full details.`,

		Run: func(c *cobra.Command, args []string) {
			cmd, _, e := c.Root().Find(args)
			if cmd == nil || e != nil {
				c.Printf("Unknown help topic %#q\n", args)
				_ = c.Root().Usage()
			} else {
				cmd.InitDefaultHelpFlag() // make possible 'help' flag to be shown
				_ = cmd.Help()
			}
		},
	}
	cmd.SetHelpCommand(helpCmd)
	return cmd
}

// Init initializes dm-control
func Init(cfg *common.Config) error {
	// set the log level temporarily
	log.SetLevel(zapcore.InfoLevel)

	return errors.Trace(common.InitUtils(cfg))
}

// PrintUsage prints usage
func PrintUsage() {
	maxCmdLen := 0
	for _, cmd := range rootCmd.Commands() {
		if maxCmdLen < len(cmd.Name()) {
			maxCmdLen = len(cmd.Name())
		}
	}
	fmt.Println("Available Commands:")
	for _, cmd := range rootCmd.Commands() {
		format := fmt.Sprintf("  %%-%ds\t%%s\n", maxCmdLen)
		fmt.Printf(format, cmd.Name(), cmd.Use)
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

// PrintHelp print help message for special subCommand
func PrintHelp(args []string) {
	cmd, _, err := rootCmd.Find(args)
	if err != nil {
		fmt.Println(err)
		rootCmd.SetOut(os.Stdout)
		common.PrintCmdUsage(rootCmd)
		return
	}
	cmd.SetOut(os.Stdout)
	common.PrintCmdUsage(cmd)
}

// Start starts running a command
func Start(args []string) (err error) {
	commandMasterFlags.Reset()
	rootCmd = NewRootCmd()
	rootCmd.SetArgs(args)
	return rootCmd.Execute()
}
