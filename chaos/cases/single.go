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
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/chaos-mesh/go-sqlsmith"
	"github.com/pingcap/errors"
	"go.uber.org/zap"

	config2 "github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/conn"
	"github.com/pingcap/dm/pkg/log"
)

const (
	singleDB              = "db_single"      // specified in `source1.yaml`.
	singleTableCount      = 10               // tables count for the table.
	singleFullInsertCount = 100              // `INSERT INTO` count (not rows count) for tables in full stage.
	singleDiffCount       = 10               // diff data check count
	singleDiffInterval    = 10 * time.Second // diff data check interval
	singleIncrRoundTime   = 20 * time.Second // time to generate incremental data in one round
)

// singleTask is a test case with only one upstream source.
type singleTask struct {
	logger log.Logger

	ctx        context.Context
	cli        pb.MasterClient
	confDir    string
	ss         *sqlsmith.SQLSmith
	sourceDB   *conn.BaseDB
	targetDB   *conn.BaseDB
	sourceConn *dbConn
	targetConn *dbConn
	tables     []string
	taskCfg    config2.TaskConfig
	result     *singleResult
}

// singleResult holds the result of the single source task case.
type singleResult struct {
	Insert int `json:"insert"`
	Update int `json:"update"`
	Delete int `json:"delete"`
}

func (sr *singleResult) String() string {
	data, err := json.Marshal(sr)
	if err != nil {
		return err.Error()
	}
	return string(data)
}

// newSingleTask creates a new singleTask instance.
func newSingleTask(ctx context.Context, cli pb.MasterClient, confDir string,
	targetCfg config2.DBConfig, sourcesCfg ...config2.DBConfig) (*singleTask, error) {
	sourceDB, err := conn.DefaultDBProvider.Apply(sourcesCfg[0])
	if err != nil {
		return nil, err
	}
	sourceConn, err := createDBConn(ctx, sourceDB, singleDB)
	if err != nil {
		return nil, err
	}

	targetDB, err := conn.DefaultDBProvider.Apply(targetCfg)
	if err != nil {
		return nil, err
	}
	targetConn, err := createDBConn(ctx, targetDB, singleDB)
	if err != nil {
		return nil, err
	}

	var taskCfg config2.TaskConfig
	err = taskCfg.DecodeFile(filepath.Join(confDir, "task-single.yaml"))
	if err != nil {
		return nil, err
	}
	taskCfg.TargetDB = &targetCfg

	st := &singleTask{
		logger:     log.L().WithFields(zap.String("case", "single-task")),
		ctx:        ctx,
		cli:        cli,
		confDir:    confDir,
		ss:         sqlsmith.New(),
		sourceDB:   sourceDB,
		targetDB:   targetDB,
		sourceConn: sourceConn,
		targetConn: targetConn,
		taskCfg:    taskCfg,
		result:     &singleResult{},
	}
	st.ss.SetDB(singleDB)
	return st, nil
}

// run runs the case.
func (st *singleTask) run() error {
	defer func() {
		st.sourceDB.Close()
		st.targetDB.Close()

		st.logger.Info("single task run result", zap.Stringer("result", st.result))
	}()

	if err := st.stopPreviousTask(); err != nil {
		return err
	}
	if err := st.clearPreviousData(); err != nil {
		return err
	}

	if err := st.genFullData(); err != nil {
		return err
	}

	if err := st.createTask(); err != nil {
		return err
	}

	st.logger.Info("check data for full stage")
	if err := diffDataLoop(st.ctx, singleDiffCount, singleDiffInterval, singleDB, st.tables, st.targetDB.DB, st.sourceDB.DB); err != nil {
		return err
	}

	return st.incrLoop()
}

// stopPreviousTask stops the previous task with the same name if exists.
func (st *singleTask) stopPreviousTask() error {
	st.logger.Info("stopping previous task")
	resp, err := st.cli.OperateTask(st.ctx, &pb.OperateTaskRequest{
		Op:   pb.TaskOp_Stop,
		Name: st.taskCfg.Name,
	})
	if err != nil {
		return err
	} else if !resp.Result && !strings.Contains(resp.Msg, "not exist") {
		return fmt.Errorf("fail to stop task: %s", resp.Msg)
	}
	return nil
}

// clearPreviousData clears previous data in upstream source and downstream target.
func (st *singleTask) clearPreviousData() error {
	st.logger.Info("clearing previous source and target data")
	if err := dropDatabase(st.ctx, st.sourceConn, singleDB); err != nil {
		return err
	}
	return dropDatabase(st.ctx, st.targetConn, singleDB)
}

// genFullData generates data for the full stage.
func (st *singleTask) genFullData() error {
	st.logger.Info("generating data for full stage")
	if err := createDatabase(st.ctx, st.sourceConn, singleDB); err != nil {
		return err
	}

	// NOTE: we set CURRENT database here.
	if err := st.sourceConn.execSQLs(st.ctx, fmt.Sprintf("USE %s", singleDB)); err != nil {
		return err
	}

	var (
		columns = make([][5]string, 0)
		indexes = make(map[string][]string)
	)

	// generate `CREATE TABLE` statements.
	for i := 0; i < singleTableCount; i++ {
		query, name, err := st.ss.CreateTableStmt()
		if err != nil {
			return err
		}
		err = st.sourceConn.execSQLs(st.ctx, query)
		if err != nil {
			return err
		}
		st.tables = append(st.tables, name)

		col2, idx2, err := createTableToSmithSchema(singleDB, query)
		if err != nil {
			return err
		}
		columns = append(columns, col2...)
		indexes[name] = idx2
	}

	// go-sqlsmith needs to load schema before generating DML and `ALTER TABLE` statements.
	st.ss.LoadSchema(columns, indexes)

	for i := 0; i < singleFullInsertCount; i++ {
		query, _, err := st.ss.InsertStmt(false)
		if err != nil {
			return err
		}
		err = st.sourceConn.execSQLs(st.ctx, query)
		if err != nil {
			return err
		}
	}

	return nil
}

// createSingleTask creates a single source task.
func (st *singleTask) createTask() error {
	st.logger.Info("starting task")
	resp, err := st.cli.StartTask(st.ctx, &pb.StartTaskRequest{
		Task: st.taskCfg.String(),
	})
	if err != nil {
		return err
	} else if !resp.Result && !strings.Contains(resp.Msg, "already exist") { // imprecise match
		return fmt.Errorf("fail to start task: %s", resp.Msg)
	}
	return nil
}

// incrLoop enters the loop of generating incremental data and diff them.
func (st *singleTask) incrLoop() error {
	for {
		select {
		case <-st.ctx.Done():
			return nil
		default:
			ctx2, cancel2 := context.WithTimeout(st.ctx, singleIncrRoundTime)
			// generate data
			err := st.genIncrData(ctx2)
			if err != nil {
				cancel2()
				return err
			}

			// diff data
			err = st.diffIncrData(ctx2)
			if err != nil {
				cancel2()
				return err
			}
			cancel2()
		}
	}
}

// genIncrData generates data for the incremental stage in one round.
// NOTE: it return nil for context done.
func (st *singleTask) genIncrData(ctx context.Context) (err error) {
	st.logger.Info("generating data for incremental stage")

	defer func() {
		if errors.Cause(err) == context.Canceled || errors.Cause(err) == context.DeadlineExceeded {
			err = nil // clear error for context done.
		} else if err != nil {
			select {
			case <-ctx.Done():
				st.logger.Warn("ignore error when generating data for incremental stage", zap.Error(err))
				err = nil // some other errors like `connection is already closed` may also be reported for context done.
			default:
				if forceIgnoreExecSQLError(err) {
					st.logger.Warn("ignore error when generating data for incremental stage", zap.Error(err))
					err2 := st.sourceConn.resetConn(ctx) // reset connection for the next round.
					if err2 != nil {
						st.logger.Warn("fail to reset connection", zap.Error(err2))
					}
					err = nil
				}
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		query, t, err := randDML(st.ss)
		if err != nil {
			return err
		}
		err = st.sourceConn.execSQLs(ctx, query)
		if err != nil {
			return err
		}

		switch t {
		case insertDML:
			st.result.Insert++
		case updateDML:
			st.result.Update++
		case deleteDML:
			st.result.Delete++
		default:
		}
	}
}

// diffIncrData checks data equal for the incremental stage in one round.
// NOTE: it return nil for context done.
func (st *singleTask) diffIncrData(ctx context.Context) (err error) {
	st.logger.Info("check data for incremental stage")

	defer func() {
		if errors.Cause(err) == context.Canceled || errors.Cause(err) == context.DeadlineExceeded {
			err = nil // clear error for context done.
		} else if err != nil {
			select {
			case <-ctx.Done():
				st.logger.Warn("ignore error when check data for incremental stage", zap.Error(err))
				err = nil // some other errors like `connection is already closed` may also be reported for context done.
			default:
			}
		}
	}()

	return diffDataLoop(ctx, singleDiffCount, singleDiffInterval, singleDB, st.tables, st.targetDB.DB, st.sourceDB.DB)
}
