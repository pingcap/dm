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

package master

import (
	"context"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-enterprise-tools/checker"
	"github.com/pingcap/tidb-enterprise-tools/dm/config"
)

func checkTask(ctx context.Context, task string) error {
	// precheck task
	cfg := config.NewTaskConfig()
	err := cfg.Decode(task)
	if err != nil {
		return errors.Annotatef(err, "decode config %s", task)
	}

	stCfgs := cfg.SubTaskConfigs()
	// poor man's precheck
	// TODO: improve process and display
	err = checker.CheckSyncConfig(ctx, stCfgs)
	return errors.Trace(err)
}
