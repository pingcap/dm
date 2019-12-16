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
    "context"
    "flag"
    "fmt"
    "github.com/pingcap/dm/dm/pb"
    "github.com/pingcap/dm/pkg/utils"
    "github.com/pingcap/dm/syncer"
    "github.com/pingcap/errors"
    "github.com/pingcap/dm/pkg/log"
    "go.uber.org/zap"
    "os"
    "os/signal"
    "strings"
    "syscall"
)

func main() {
    commonConfig := NewConfig()
    config, err := commonConfig.Parse(os.Args[1:])
    switch errors.Cause(err) {
    case nil:
    case flag.ErrHelp:
        os.Exit(0)
    default:
        log.L().Error("parse cmd flags err " + err.Error())
        os.Exit(2)
    }

    config.RelayDir = "./relay-dir"

    // 2. init logger
    err = log.InitLogger(&log.Config{
        File:  config.LogFile,
        Level: strings.ToLower(config.LogLevel),
    })
    if err != nil {
        fmt.Printf("init logger error %v", errors.ErrorStack(err))
        os.Exit(2)
    }

    // 3. print process version information
    utils.PrintInfo("dm-syncer", func() {
        log.L().Info("", zap.Stringer("dm-syncer config", config))
    })

    sync := syncer.NewSyncer(config)
    sc := make(chan os.Signal, 1)
    signal.Notify(sc, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

    ctx, cancel := context.WithCancel(context.Background())

    go func() {
        sig := <-sc
        log.L().Info("got signal to exit", zap.Stringer("signal", sig))
        cancel()
    }()

    // 4. start the server
    err = sync.Init(ctx)
    if err != nil {
        fmt.Printf("init syncer error %v", errors.ErrorStack(err))
        os.Exit(2)
    }
    pr := make(chan pb.ProcessResult, 1)
    sync.Process(ctx, pr)

    // 5. close the server
    sync.Close()
    log.L().Info("dm-syncer exit")

    // 6. flush log
    syncErr := log.L().Sync()
    if syncErr != nil {
        fmt.Fprintln(os.Stderr, "sync log failed", syncErr)
        os.Exit(1)
    }
}