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

package main

import (
	"flag"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-enterprise-tools/checker"
	"github.com/pingcap/tidb-enterprise-tools/dm/config"
	"github.com/pingcap/tidb-enterprise-tools/dm/pb"
	"github.com/pingcap/tidb-enterprise-tools/pkg/utils"
	"github.com/pingcap/tidb-enterprise-tools/syncer"
	"golang.org/x/net/context"
)

func main() {
	cfg := config.NewSubTaskConfig()
	cfg.SetupFlags(config.CmdSyncer)
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.Errorf("parse cmd flags err %s", err)
		os.Exit(2)
	}

	log.SetLevelByString(strings.ToLower(cfg.LogLevel))

	if len(cfg.LogFile) > 0 {
		log.SetOutputByName(cfg.LogFile)
		log.SetHighlighting(false)

		if cfg.LogRotate == "day" {
			log.SetRotateByDay()
		} else {
			log.SetRotateByHour()
		}
	}

	utils.PrintInfo("syncer", func() {
		log.Infof("config: %s", cfg)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = checker.CheckSyncConfig(ctx, []*config.SubTaskConfig{cfg})
	if err != nil {
		log.Fatal(err)
	}

	syncUnit := syncer.NewSyncer(cfg)
	err = syncUnit.Init()
	if err != nil {
		log.Error(errors.ErrorStack(err))
		return
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		log.Infof("got signal [%v], exit", sig)
		cancel()
		syncUnit.Close()
	}()

	if cfg.StatusAddr != "" {
		syncer.InitStatusAndMetrics(cfg.StatusAddr)
	}

	pr := make(chan pb.ProcessResult, 1)
	syncUnit.Process(ctx, pr)
	syncUnit.Close()

	var errOccurred bool
	for len(pr) > 0 {
		r := <-pr
		for _, err := range r.Errors {
			errOccurred = true
			log.Errorf("process error with type %v:\n %v", err.Type, err.Msg)
		}
	}
	if errOccurred {
		log.Fatal("syncer exits with some errors")
	}

	log.Info("syncer stopped and exits")
}
