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
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/pingcap/dm/dm/pb"
)

const (
	singleDB = "db_single" // specified in `source1.yaml`.
)

// runSingleTaskCase runs a case with only single task.
func runSingleTaskCase(ctx context.Context, cli pb.MasterClient, confDir string) error {
	err := createSingleTask(ctx, cli, confDir)
	if err != nil {
		return err
	}

	return nil
}

// createSingleTask creates a single source task.
func createSingleTask(ctx context.Context, cli pb.MasterClient, confDir string) error {
	cfgPath := filepath.Join(confDir, "task-single.yaml")
	content, err := ioutil.ReadFile(cfgPath)
	if err != nil {
		return err
	}

	resp, err := cli.StartTask(ctx, &pb.StartTaskRequest{
		Task: string(content),
	})
	if err != nil {
		return err
	} else if !resp.Result && !strings.Contains(resp.Msg, "already exist") { // imprecise match
		return fmt.Errorf("fail to start task: %s", resp.Msg)
	}
	return nil
}
