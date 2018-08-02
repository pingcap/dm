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
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-enterprise-tools/dm/master"
	"github.com/pingcap/tidb-enterprise-tools/pkg/utils"
)

func main() {
	cfg := master.NewConfig()
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

	utils.PrintInfo("dm-master", func() {
		log.Infof("config: %s", cfg)
	})

	if cfg.StatusAddr != "" {
		master.InitStatus(cfg.StatusAddr)
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	server := master.NewServer(cfg)

	go func() {
		sig := <-sc
		log.Infof("got signal [%v], exit", sig)
		server.Close()
	}()

	err = server.Start()
	if err != nil {
		log.Errorf("dm-master start with error %v", errors.ErrorStack(err))
	}
	server.Close()

	log.Info("dm-master exit")
}
