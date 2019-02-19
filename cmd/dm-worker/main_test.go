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

// Reference: https://dzone.com/articles/measuring-integration-test-coverage-rate-in-pouchc

import (
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"testing"
)

func TestRunMain(t *testing.T) {
	var (
		args   []string
		wg     sync.WaitGroup
		waitCh = make(chan int, 1)
		exitCh = make(chan struct{})
	)

	for _, arg := range os.Args {
		switch {
		case arg == "DEVEL":
		case strings.HasPrefix(arg, "-test."):
		default:
			args = append(args, arg)
		}
	}

	os.Args = args
	wg.Add(1)
	go func() {
		defer wg.Done()
		mainWrapper(exitCh)
		close(waitCh)
	}()

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGHUP)
	select {
	case <-waitCh:
	case <-signalCh:
		close(exitCh)
	}
	wg.Wait()
}
