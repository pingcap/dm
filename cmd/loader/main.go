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
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	loaderPkg "github.com/pingcap/dm/loader"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/context"
)

func main() {
	cfg := config.NewSubTaskConfig()
	cfg.SetupFlags(config.CmdLoader)
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.Errorf("parse cmd flags err %s\n", err)
		os.Exit(2)
	}

	log.SetLevelByString(strings.ToLower(cfg.LogLevel))
	if len(cfg.LogFile) > 0 {
		log.SetOutputByName(cfg.LogFile)
		log.SetHighlighting(false)
	}

	utils.PrintInfo("loader", func() {
		log.Infof("config: %s", cfg)
	})

	loader := loaderPkg.NewLoader(cfg)
	err = loader.Init()
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sig := <-sc
		log.Infof("Got signal [%v] to exit.", sig)
		cancel()
		loader.Close()
	}()

	go func() {
		registry := prometheus.NewRegistry()
		registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
		registry.MustRegister(prometheus.NewGoCollector())
		loaderPkg.RegisterMetrics(registry)
		prometheus.DefaultGatherer = registry

		http.Handle("/metrics", prometheus.Handler())
		err1 := http.ListenAndServe(cfg.PprofAddr, nil)
		if err1 != nil {
			log.Fatal(err1)
		}
	}()

	pr := make(chan pb.ProcessResult, 1)
	loader.Process(ctx, pr)
	loader.Close()

	var errOccurred bool
	for len(pr) > 0 {
		r := <-pr
		for _, err := range r.Errors {
			errOccurred = true
			log.Errorf("process error with type %v:\n %v", err.Type, err.Msg)
		}
	}
	if errOccurred {
		log.Fatal("loader exits with some errors")
	}

	log.Info("loader stopped and exits")
}
