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

package master

import (
	"context"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/master/scheduler"
	"github.com/pingcap/dm/dm/master/workerrpc"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/upgrade"
	"github.com/pingcap/dm/pkg/v1workermeta"
)

// bootstrap bootstraps the cluster, now including:
// - upgrade the cluster from v1.0.x if needed.
// - upgrade the cluster from a previous v2.0.x version to the current version.
func (s *Server) bootstrap(ctx context.Context) error {
	if s.cfg.V1SourcesPath != "" {
		err := s.importFromV10x(ctx)
		if err != nil {
			return terror.ErrMasterFailToImportFromV10x.Delegate(err)
		}
	}

	err := upgrade.TryUpgrade(s.etcdClient)
	if err != nil {
		return err
	}
	return nil
}

// importFromV10x tries to import/upgrade the cluster from v1.0.x.
func (s *Server) importFromV10x(ctx context.Context) error {
	// 1. check whether need to upgrade based on the cluster version.
	preVer, _, err := upgrade.GetVersion(s.etcdClient)
	if err != nil {
		return err
	} else if !preVer.NotSet() {
		return nil // v2.0.x cluster, no need to import.
	}

	// 2. collect source config files.
	sourceCfgs, err := s.collectSourceConfigFilesV1Import()
	if err != nil {
		return err
	}

	// 3. wait for all DM-worker instances ready.
	err = s.waitWorkersReadyV1Import(ctx, sourceCfgs)
	if err != nil {
		return err
	}

	// 4. get subtasks config and stage from DM-worker instances.
	_, _, err = s.getSubtaskCfgsStagesV1Import(ctx)
	if err != nil {
		return err
	}

	// 5. create sources.

	// 6. upgrade v1.0.x downstream metadata table.

	// 7. create subtasks with the specified stage.

	// 8. mark the upgrade operation as done.

	// 9. clear v1.0.x data (source config files, DM-worker metadata), failed is not a problem.

	return nil
}

// collectSourceConfigFilesV1Import tries to collect source config files for v1.0.x importing.
func (s *Server) collectSourceConfigFilesV1Import() (map[string]config.SourceConfig, error) {
	files, err := ioutil.ReadDir(s.cfg.V1SourcesPath)
	if err != nil {
		return nil, err
	}

	cfgs := make(map[string]config.SourceConfig)
	for _, f := range files {
		if f.IsDir() {
			continue // ignore sub directories.
		}

		fp := filepath.Join(s.cfg.V1SourcesPath, f.Name())
		content, err := ioutil.ReadFile(fp)
		if err != nil {
			return nil, err
		}

		var cfg config.SourceConfig
		err = cfg.ParseYaml(string(content))
		if err != nil {
			// abort importing if any invalid source config files exist.
			return nil, err
		}

		cfgs[cfg.SourceID] = cfg
	}

	return cfgs, nil
}

// waitWorkersReadyV1Import waits for DM-worker instances ready for v1.0.x importing.
// NOTE: in v1.0.x, `count of DM-worker instances` equals `count of source config files`.
func (s *Server) waitWorkersReadyV1Import(ctx context.Context, sourceCfgs map[string]config.SourceConfig) error {
	var (
		// now, we simply check count repeatedly, and hardcode timeout now (refine if needed later).
		count    = len(sourceCfgs)
		interval = time.Second
		timeout  = 5 * time.Minute
	)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
			workers, err := s.scheduler.GetAllWorkers()
			if err != nil {
				return err
			}
			if len(workers) >= count {
				return nil
			}
		case <-time.After(timeout):
			return errors.Errorf("wait for DM-worker instances timeout in %v", timeout)
		}
	}
}

// getSubtaskCfgsStagesV1Import tries to get all subtask config and stage from DM-worker instances.
// returned:
//   - configs: task-name -> source-ID -> subtask config.
//   - stages: task-name -> source-ID -> subtask stage.
func (s *Server) getSubtaskCfgsStagesV1Import(ctx context.Context) (
	map[string]map[string]config.SubTaskConfig, map[string]map[string]pb.Stage, error) {
	workers, err := s.scheduler.GetAllWorkers()
	if err != nil {
		return nil, nil, err
	}

	req := workerrpc.Request{
		Type:          workerrpc.CmdOperateV1Meta,
		OperateV1Meta: &pb.OperateV1MetaRequest{Op: pb.V1MetaOp_GetV1Meta},
	}
	respCh := make(chan *pb.OperateV1MetaResponse, len(workers))
	var wg sync.WaitGroup
	for _, worker := range workers {
		wg.Add(1)
		go func(worker *scheduler.Worker) {
			defer wg.Done()
			resp, err := worker.SendRequest(ctx, &req, s.cfg.RPCTimeout)
			if err != nil {
				respCh <- &pb.OperateV1MetaResponse{
					Result: false,
					Msg:    fmt.Sprintf("worker %s, %s", worker.BaseInfo().String(), err.Error()),
				}
			} else {
				respCh <- resp.OperateV1Meta
			}
		}(worker)
	}
	wg.Wait()

	subtaskCfgs := make(map[string]map[string]config.SubTaskConfig)
	subtaskStages := make(map[string]map[string]pb.Stage)
	errorMsgs := make([]string, 0)
	for resp := range respCh {
		if !resp.Result {
			errorMsgs = append(errorMsgs, resp.Msg)
			continue
		}

		for taskName, meta := range resp.Meta {
			cfg, err := v1workermeta.SubTaskConfigFromV1TOML(meta.Task)
			if err != nil {
				log.L().Error("fail to get subtask config from v1 TOML", zap.ByteString("config", meta.Task))
				errorMsgs = append(errorMsgs, fmt.Sprintf("task %s, %s", meta.Name, err.Error()))
				continue
			}

			if _, ok := subtaskCfgs[taskName]; !ok {
				subtaskCfgs[taskName] = make(map[string]config.SubTaskConfig)
				subtaskStages[taskName] = make(map[string]pb.Stage)
			}
			subtaskCfgs[taskName][cfg.SourceID] = cfg
			subtaskStages[taskName][cfg.SourceID] = meta.Stage
		}
	}

	if len(errorMsgs) > 0 {
		// if failed for any DM-worker instances, we abort the importing process now.
		return nil, nil, errors.Errorf("fail to get subtask config and stage: %s", strings.Join(errorMsgs, ","))
	}

	return subtaskCfgs, subtaskStages, nil
}
