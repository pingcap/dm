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
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/pingcap/errors"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/master"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/utils"
)

func main() {
	cfg := master.NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		fmt.Printf("parse cmd flags err %s", err)
		os.Exit(2)
	}

	err = log.InitLogger(&log.Config{
		File:  cfg.LogFile,
		Level: strings.ToLower(cfg.LogLevel),
	})
	if err != nil {
		fmt.Printf("init logger error %v", errors.ErrorStack(err))
		os.Exit(2)
	}

	utils.PrintInfo("dm-master", func() {
		log.L().Info("", zap.Stringer("dm-master config", cfg))
	})

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	server := master.NewServer(cfg)

	go func() {
		sig := <-sc
		log.L().Info("got signal to exit", zap.Stringer("signal", sig))
		server.Close()
	}()

	err = server.Start()
	if err != nil {
		log.L().Error("fail to start dm-master", zap.Error(err))
	}
	server.Close()

	log.L().Info("dm-master exit")

	syncErr := log.L().Sync()
	if syncErr != nil {
		fmt.Fprintln(os.Stderr, "sync log failed", syncErr)
		log.L().Error("sync log failed", zap.Error(syncErr))
	}

	if err != nil || syncErr != nil {

		os.Exit(1)
	}
}
