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

	"github.com/pingcap/dm/dm/tracer"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/pingcap/errors"
	"go.uber.org/zap"
)

func main() {
	cfg := tracer.NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		fmt.Printf("parse cmd flags err %s", errors.ErrorStack(err))
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

	utils.PrintInfo("dm-tracer", func() {
		log.L().Info("", zap.Stringer("dm-tracer config", cfg))
	})

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	s := tracer.NewServer(cfg)

	go func() {
		sig := <-sc
		log.L().Info("got signal to exit", zap.Stringer("signal", sig))
		s.Close()
	}()

	err = s.Start()
	if err != nil {
		log.L().Error("fail to start dm-tracer", zap.Error(err))
	}
	s.Close() // wait until closed
	log.L().Info("dm-tracer exit")

	syncErr := log.L().Sync()
	if syncErr != nil {
		fmt.Fprintln(os.Stderr, "sync log failed", syncErr)
	}

	if err != nil || syncErr != nil {
		os.Exit(1)
	}
}
