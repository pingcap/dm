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
	"github.com/pingcap/dm/pkg/conn"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/upgrade"
	"github.com/pingcap/dm/pkg/v1dbschema"
	"github.com/pingcap/dm/pkg/v1workermeta"
)

var (
	// interval when waiting for the specified count of DM-worker instances become registered when importing from v1.0.x.
	waitWorkerV1Interval = time.Second
	// timeout when waiting for the specified count of DM-worker instances become registered when importing from v1.0.x.
	waitWorkerV1Timeout = 5 * time.Minute
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

	logger := log.L().WithFields(zap.String("op", "import from v1.0.x"))
	tctx := tcontext.NewContext(ctx, logger)

	// 2. collect source config files.
	logger.Info("collecting source config files", zap.String("path", s.cfg.V1SourcesPath))
	sourceCfgs, err := s.collectSourceConfigFilesV1Import()
	if err != nil {
		return err
	}

	// 3. wait for all DM-worker instances ready.
	logger.Info("waiting for all DM-worker instances ready", zap.Int("count", len(sourceCfgs)))
	err = s.waitWorkersReadyV1Import(tctx, sourceCfgs)
	if err != nil {
		return err
	}

	// 4. get subtasks config and stage from DM-worker instances.
	logger.Info("getting subtask config and status from DM-worker")
	subtaskCfgs, _, err := s.getSubtaskCfgsStagesV1Import(tctx)
	if err != nil {
		return err
	}

	// 5. create sources.
	logger.Info("add source config into cluster")
	err = s.addSourcesV1Import(tctx, sourceCfgs)
	if err != nil {
		return err
	}

	// 6. upgrade v1.0.x downstream metadata table.
	logger.Info("upgrading downstream metadata tables")
	err = s.upgradeDBSchemaV1Import(tctx, subtaskCfgs)
	if err != nil {
		return err
	}

	// 7. create subtasks with the specified stage.
	logger.Info("creating subtasks")

	// 8. mark the upgrade operation as done.
	logger.Info("marking upgrade from v1.0.x as done")

	// 9. clear v1.0.x data (source config files, DM-worker metadata), failed is not a problem.
	logger.Info("clearing v1.0.x data")

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
func (s *Server) waitWorkersReadyV1Import(tctx *tcontext.Context, sourceCfgs map[string]config.SourceConfig) error {
	// now, we simply check count repeatedly.
	count := len(sourceCfgs)
	ctx2, cancel2 := context.WithTimeout(context.Background(), waitWorkerV1Timeout)
	defer cancel2()
	for {
		select {
		case <-tctx.Ctx.Done():
			return tctx.Ctx.Err()
		case <-ctx2.Done():
			return errors.Errorf("wait for DM-worker instances timeout in %v", waitWorkerV1Timeout)
		case <-time.After(waitWorkerV1Interval):
			workers, err := s.scheduler.GetAllWorkers()
			if err != nil {
				return err
			}
			if len(workers) >= count {
				return nil
			}
		}
	}
}

// getSubtaskCfgsStagesV1Import tries to get all subtask config and stage from DM-worker instances.
// returned:
//   - configs: task-name -> source-ID -> subtask config.
//   - stages: task-name -> source-ID -> subtask stage.
func (s *Server) getSubtaskCfgsStagesV1Import(tctx *tcontext.Context) (
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
			resp, err := worker.SendRequest(tctx.Ctx, &req, s.cfg.RPCTimeout)
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
	for len(respCh) > 0 {
		resp := <-respCh
		if !resp.Result {
			errorMsgs = append(errorMsgs, resp.Msg)
			continue
		}

		for taskName, meta := range resp.Meta {
			cfg, err := v1workermeta.SubTaskConfigFromV1TOML(meta.Task)
			if err != nil {
				tctx.Logger.Error("fail to get subtask config from v1 TOML", zap.ByteString("config", meta.Task))
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

// addSourcesV1Import tries to add source config into the cluster for v1.0.x importing.
func (s *Server) addSourcesV1Import(tctx *tcontext.Context, cfgs map[string]config.SourceConfig) error {
	var (
		added []string
		err   error
	)
	for _, cfg := range cfgs {
		err = s.scheduler.AddSourceCfg(cfg)
		if err != nil {
			break
		}
		added = append(added, cfg.SourceID)
	}

	if err != nil {
		// try to remove source configs if any error occurred, but it's not a big problem if failed.
		for _, sid := range added {
			err2 := s.scheduler.RemoveSourceCfg(sid)
			if err2 != nil {
				tctx.Logger.Error("fail to remove source config", zap.String("source", sid), zap.Error(err2))
			}
		}
	}
	return err
}

// upgradeDBSchemaV1Import tries to upgrade the metadata DB schema (and data) for v1.0.x importing.
func (s *Server) upgradeDBSchemaV1Import(tctx *tcontext.Context, cfgs map[string]map[string]config.SubTaskConfig) error {
	for _, taskCfgs := range cfgs {
		// all subtasks in one task must have the same downstream, so we only create one BaseDB instance.
		// but different tasks may have different downstream.
		var targetDB *conn.BaseDB
		for _, cfg := range taskCfgs {
			cfg2, err := cfg.DecryptPassword() // `cfg` should already be `Adjust`.
			if err != nil {
				return err
			}
			if targetDB == nil {
				targetDB, err = conn.DefaultDBProvider.Apply(cfg2.To)
				if err != nil {
					return err
				}
			}
			err = v1dbschema.UpdateSchema(tctx, targetDB, cfg2)
			if err != nil {
				targetDB.Close()
				return err
			}
		}
		targetDB.Close() // close BaseDB for this task.
	}
	return nil
}

// createSubtaskV1Import tries to create subtasks with the specified stage.
func (s *Server) createSubtaskV1Import(tctx tcontext.Context,
	cfgs map[string]map[string]config.SubTaskConfig,
	stages map[string]map[string]pb.Stage) error {
	return nil
}
