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
	"strings"
	"testing"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/utils"
)

func TestRunMain(t *testing.T) {
	var (
		args []string
		exit chan int = make(chan int)
	)
	for _, arg := range os.Args {
		switch {
		case arg == "DEVEL":
		case strings.HasPrefix(arg, "-test."):
		default:
			args = append(args, arg)
		}
	}

	go func() {
		select {
		case <-exit:
			os.Exit(0)
		}
	}()

	oldOsExit := utils.OsExit
	defer func() { utils.OsExit = oldOsExit }()
	utils.OsExit = func(code int) {
		log.Infof("[test] os.Exit with code %d", code)
		exit <- code
	}

	os.Args = args
	main()
}
