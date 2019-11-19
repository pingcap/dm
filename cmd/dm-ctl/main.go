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

package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/chzyer/readline"
	"github.com/pingcap/errors"

	"github.com/pingcap/dm/dm/ctl"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/utils"
)

// output:
// Usage: dmctl [global options] command [command options] [arguments...]
//
//Available Commands:
//  break-ddl-lock        break-ddl-lock <-w worker ...> <task-name> [--remove-id] [--exec] [--skip]
//  check-task            check-task <config-file>
//  migrate-relay         migrate-relay <worker> <binlogName> <binlogPos>
//  pause-relay           pause-relay <-w worker ...>
//  pause-task            pause-task [-w worker ...] <task-name>
//  purge-relay           purge-relay <-w worker> [--filename] [--sub-dir]
//  query-error           query-error [-w worker ...] [task-name]
//  query-status          query-status [-w worker ...] [task-name]
//  refresh-worker-tasks  refresh-worker-tasks
//  resume-relay          resume-relay <-w worker ...>
//  resume-task           resume-task [-w worker ...] <task-name>
//  show-ddl-locks        show-ddl-locks [-w worker ...] [task-name]
//  sql-inject            sql-inject <-w worker> <task-name> <sql1;sql2;>
//  sql-replace           sql-replace <-w worker> [-b binlog-pos] [-s sql-pattern] [--sharding] <task-name> <sql1;sql2;>
//  sql-skip              sql-skip <-w worker> [-b binlog-pos] [-s sql-pattern] [--sharding] <task-name>
//  start-task            start-task [-w worker ...] <config-file>
//  stop-task             stop-task [-w worker ...] <task-name>
//  switch-relay-master   switch-relay-master <-w worker ...>
//  unlock-ddl-lock       unlock-ddl-lock [-w worker ...] <lock-ID>
//  update-master-config  update-master-config <config-file>
//  update-relay          update-relay [-w worker ...] <config-file>
//  update-task           update-task [-w worker ...] <config-file>
//
//Special Commands:
//  --encrypt encrypt plaintext to ciphertext
//
//Global Options:
//  --V prints version and exit
//  --config path to config file
//  --master-addr master API server addr
//  --rpc-timeout rpc timeout, default is 10m
func helpUsage(cfg *common.Config) {
	fmt.Println("Usage: dmctl [global options] command [command options] [arguments...]")
	fmt.Println()
	ctl.PrintUsage()
	fmt.Println()
	fmt.Println("Special Commands:")
	f := cfg.FlagSet.Lookup(common.EncryptCmdName)
	fmt.Println(fmt.Sprintf("  --%s %s", f.Name, f.Usage))
	fmt.Println()
	fmt.Println("Global Options:")
	cfg.FlagSet.VisitAll(func(flag2 *flag.Flag) {
		if flag2.Name == common.EncryptCmdName {
			return
		}
		fmt.Println(fmt.Sprintf("  --%s %s", flag2.Name, flag2.Usage))
	})
}

func main() {
	cfg := common.NewConfig()
	args := os.Args[1:]

	// no arguments: print help message about dmctl
	if len(args) == 0 {
		helpUsage(cfg)
		os.Exit(0)
	}

	// now, we use checker in dmctl while it using some pkg which log some thing when running
	// to make dmctl output more clear, simply redirect log to file rather output to stdout
	err := log.InitLogger(&log.Config{
		File:  "dmctl.log",
		Level: "info",
	})
	if err != nil {
		fmt.Printf("init logger error %v", errors.ErrorStack(err))
		os.Exit(2)
	}

	// try to split one task operation from dmctl command
	// because we allow user put task operation at last with two restrictions
	// 1. one command one task operation
	// 2. put task operation at last
	cmdArgs := extractSubCommand(args)
	lenArgs := len(args)
	lenCmdArgs := len(cmdArgs)
	if lenCmdArgs > 0 {
		lenArgs = lenArgs - lenCmdArgs
	}

	finished, err := cfg.Parse(args[:lenArgs])
	if finished {
		os.Exit(0)
	}

	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		if lenCmdArgs > 0 {
			// print help message about special subCommand
			ctl.PrintHelp(cmdArgs)
		} else {
			// print help message about dmctl
			helpUsage(cfg)
		}
		os.Exit(0)
	default:
		fmt.Printf("parse cmd flags err: %s", err)
		os.Exit(2)
	}

	err = cfg.Validate()
	if err != nil {
		fmt.Printf("flags are not validate: %s", err)
		os.Exit(2)
	}

	err = ctl.Init(cfg)
	if err != nil {
		fmt.Printf("init control error %v", errors.ErrorStack(err))
		os.Exit(2)
	}
	if lenCmdArgs > 0 {
		commandMode(cmdArgs)
	} else {
		interactionMode()
	}
}

func extractSubCommand(args []string) []string {
	collectedArgs := make([]string, 0, len(args))
	subCommand := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		// check whether has multiple commands
		if ctl.HasCommand(strings.ToLower(args[i])) {
			subCommand = append(subCommand, args[i])
		}
	}
	if len(subCommand) == 0 {
		return collectedArgs
	}
	if len(subCommand) > 1 {
		fmt.Printf("command mode only support one command at a time, find %d:", len(subCommand))
		fmt.Println(subCommand)
		os.Exit(1)
	}

	for i := 0; i < len(args); i++ {
		if ctl.HasCommand(strings.ToLower(args[i])) {
			collectedArgs = append(collectedArgs, args[i:]...)
			break
		}
	}
	return collectedArgs
}

func commandMode(args []string) {
	ctl.Start(args)
}

func interactionMode() {
	utils.PrintInfo2("dmctl")
	fmt.Println() // print a separater

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		fmt.Printf("got signal [%v] to exit", sig)
		switch sig {
		case syscall.SIGTERM:
			os.Exit(0)
		default:
			os.Exit(1)
		}
	}()

	loop()

	fmt.Println("dmctl exit")
}

func loop() {
	l, err := readline.NewEx(&readline.Config{
		Prompt:          "\033[31m»\033[0m ",
		HistoryFile:     "/tmp/dmctlreadline.tmp",
		InterruptPrompt: "^C",
		EOFPrompt:       "^D",
	})
	if err != nil {
		panic(err)
	}
	defer l.Close()

	for {
		line, err := l.Readline()
		if err != nil {
			if err == readline.ErrInterrupt {
				break
			} else if err == io.EOF {
				break
			}
			continue
		}
		if line == "exit" {
			os.Exit(0)
		} else if line == "" {
			continue
		}

		args := strings.Fields(line)
		ctl.Start(args)

		syncErr := log.L().Sync()
		if syncErr != nil {
			fmt.Fprintln(os.Stderr, "sync log failed", syncErr)
		}
	}
}
