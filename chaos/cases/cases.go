// Copyright 2020 PingCAP, Inc.
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
	"path/filepath"

	config2 "github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
)

const (
	singleTaskFile = "task-single.yaml"
	singleDB       = "db_single" // specified in `task-single.yaml`.
)

// runSingleCase runs a test case with single source task.
func runSingleCase(ctx context.Context, cli pb.MasterClient, confDir string,
	targetCfg config2.DBConfig, sourcesCfg ...config2.DBConfig) error {
	taskFile := filepath.Join(confDir, singleTaskFile)
	t, err := newTask(ctx, cli, taskFile, singleDB, targetCfg, sourcesCfg...)
	if err != nil {
		return err
	}
	return t.run()
}
