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

	"golang.org/x/sync/errgroup"

	config2 "github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/utils"
)

var (
	// NOTE: items in `doSchemas` should be specified in the corresponding task files (`filenames`).
	filenames = []string{"task-single.yaml", "task-pessimistic.yaml", "task-optimistic.yaml"}
	doSchemas = []string{"db_single", "db_pessimistic", "db_optimistic"}
)

// runCases runs test cases.
func runCases(ctx context.Context, cli pb.MasterClient, confDir string,
	targetCfg config2.DBConfig, sourcesCfg ...config2.DBConfig) error {
	var eg errgroup.Group
	for i := range filenames {
		taskFile := filepath.Join(confDir, filenames[i])
		schema := doSchemas[i]
		eg.Go(func() error {
			t, err := newTask(ctx, cli, taskFile, schema, targetCfg, sourcesCfg...)
			if err != nil {
				return err
			}
			err = t.run()
			if utils.IsContextCanceledError(err) {
				err = nil // clear err
			}
			return err
		})
	}
	return eg.Wait()
}
