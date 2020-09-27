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
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

// output:
// Usage: dmctl [global options] command [command options] [arguments...]
//
//Available Commands:
//  ...
//  query-status          query-status [-s source ...] [task-name]
//  ...
//
//Special Commands:
//  --encrypt encrypt plaintext to ciphertext
//  ...
//
//Global Options:
//  --V prints version and exit
//  ...
func helpUsage(cfg *common.Config) {
	fmt.Println("Usage: dmctl [global options] command [command options] [arguments...]")
	fmt.Println()
	ctl.PrintUsage()
	fmt.Println()
	fmt.Println("Special Commands:")
	f := cfg.FlagSet.Lookup(common.EncryptCmdName)
	fmt.Printf("  --%s %s\n", f.Name, f.Usage)
	f = cfg.FlagSet.Lookup(common.DecryptCmdName)
	fmt.Printf("  --%s %s\n", f.Name, f.Usage)
	fmt.Println()
	fmt.Println("Global Options:")
	cfg.FlagSet.VisitAll(func(flag2 *flag.Flag) {
		if flag2.Name == common.EncryptCmdName || flag2.Name == common.DecryptCmdName {
			return
		}
		fmt.Printf("  --%s %s\n", flag2.Name, flag2.Usage)
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
		common.PrintLines("init logger error %s", terror.Message(err))
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
		common.PrintLines("parse cmd flags err: %s", terror.Message(err))
		os.Exit(2)
	}

	err = cfg.Validate()
	if err != nil {
		common.PrintLines("flags are not validate: %s", terror.Message(err))
		os.Exit(2)
	}

	err = ctl.Init(cfg)
	if err != nil {
		common.PrintLines("%v", terror.Message(err))
		os.Exit(2)
	}
	if lenCmdArgs > 0 {
		if err = commandMode(cmdArgs); err != nil {
			os.Exit(2)
		}
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

func commandMode(args []string) error {
	return ctl.Start(args)
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
		err = ctl.Start(args)
		if err != nil {
			fmt.Println("fail to run:", args)
		}

		syncErr := log.L().Sync()
		if syncErr != nil {
			fmt.Fprintln(os.Stderr, "sync log failed", syncErr)
		}
	}
}
