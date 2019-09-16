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
	"github.com/pingcap/errors"
)

func main() {
	cfg := common.NewConfig()
	args := os.Args[1:]
	cmdArgs := collectArgs(args)
	lenArgs := len(args)
	lastCmdArgs := len(cmdArgs) - 1
	if lastCmdArgs > 0 {
		lastArgs := lenArgs - 1
		for lastCmdArgs > 0 && lastArgs > 0 {
			if cmdArgs[lastCmdArgs] != args[lastArgs] {
				break
			}
			lastCmdArgs--
			lastArgs--
		}
		lenArgs = lastArgs + 1
	}
	err := cfg.Parse(args[:lenArgs])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		fmt.Printf("parse cmd flags err: %s", err)
		os.Exit(2)
	}
	// now, we use checker in dmctl while it using some pkg which log some thing when running
	// to make dmctl output more clear, simply redirect log to file rather output to stdout
	err = log.InitLogger(&log.Config{
		File:  "dmctl.log",
		Level: "info",
	})
	if err != nil {
		fmt.Printf("init logger error %v", errors.ErrorStack(err))
		os.Exit(2)
	}

	err = ctl.Init(cfg)
	if err != nil {
		fmt.Printf("init control error %v", errors.ErrorStack(err))
		os.Exit(2)
	}

	if len(cmdArgs) > 0 {
		commandMode(cmdArgs)
	} else {
		interactionMode()
	}
}

func collectArgs(args []string) []string {
	collectedArgs := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		switch strings.ToLower(args[i]) {
		case "start-task", "query-status", "stop-task", "pause-task", "resume-task", "show-ddl-locks":
			{
				collectedArgs = append(collectedArgs, args[i])
				i++
				collectedArgs = append(collectedArgs, args[i])
			}
		case "unlock-ddl-lock":
			{

			}
		case "break-ddl-lock":
			{

			}
		default:
			continue
		}
	}
	return collectedArgs
}

func commandMode(args []string) {
	ctl.Start(args)
	syncErr := log.L().Sync()
	if syncErr != nil {
		fmt.Fprintln(os.Stderr, "sync log failed", syncErr)
	}
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
