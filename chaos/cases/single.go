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
	"path/filepath"
	"strings"

	"github.com/chaos-mesh/go-sqlsmith"

	config2 "github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/conn"
)

const (
	singleDB         = "db_single" // specified in `source1.yaml`.
	singleTableCount = 10          // tables count for the table.
)

// singleTask is a test case with only one upstream source.
type singleTask struct {
	ctx        context.Context
	cli        pb.MasterClient
	confDir    string
	ss         *sqlsmith.SQLSmith
	sourceDB   *conn.BaseDB
	targetDB   *conn.BaseDB
	sourceConn *conn.BaseConn
	targetConn *conn.BaseConn
	tables     []string
	taskCfg    config2.TaskConfig
}

// newSingleTask creates a new singleTask instance.
func newSingleTask(ctx context.Context, cli pb.MasterClient, confDir string,
	targetCfg config2.DBConfig, sourcesCfg ...config2.DBConfig) (*singleTask, error) {
	sourceDB, err := conn.DefaultDBProvider.Apply(sourcesCfg[0])
	if err != nil {
		return nil, err
	}
	sourceConn, err := sourceDB.GetBaseConn(ctx)
	if err != nil {
		return nil, err
	}

	targetDB, err := conn.DefaultDBProvider.Apply(targetCfg)
	if err != nil {
		return nil, err
	}
	targetConn, err := targetDB.GetBaseConn(ctx)
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
		ctx:        ctx,
		cli:        cli,
		confDir:    confDir,
		ss:         sqlsmith.New(),
		sourceDB:   sourceDB,
		targetDB:   targetDB,
		sourceConn: sourceConn,
		targetConn: targetConn,
		taskCfg:    taskCfg,
	}
	st.ss.SetDB(singleDB)
	return st, nil
}

// run runs the case.
func (st *singleTask) run() error {
	defer func() {
		st.sourceDB.Close()
		st.targetDB.Close()
	}()

	if err := st.clearPreviousData(); err != nil {
		return err
	}
	if err := st.genFullData(); err != nil {
		return err
	}

	if err := st.createTask(); err != nil {
		return err
	}

	return nil
}

// clearPreviousData clears previous data in upstream source and downstream target.
func (st *singleTask) clearPreviousData() error {
	if err := dropDatabase(st.ctx, st.sourceConn, singleDB); err != nil {
		return err
	}
	if err := dropDatabase(st.ctx, st.targetConn, singleDB); err != nil {
		return err
	}
	return nil
}

// genSingleTaskFullData generates data for the full stage.
func (st *singleTask) genFullData() error {
	if err := createDatabase(st.ctx, st.sourceConn, singleDB); err != nil {
		return err
	}

	// NOTE: we set CURRENT database here.
	if err := execSQLs(st.ctx, st.sourceConn, fmt.Sprintf("USE %s", singleDB)); err != nil {
		return err
	}

	for i := 0; i < singleTableCount; i++ {
		query, name, err := st.ss.CreateTableStmt()
		if err != nil {
			return err
		}
		err = execSQLs(st.ctx, st.sourceConn, query)
		st.tables = append(st.tables, name)
	}

	return nil
}

// createSingleTask creates a single source task.
func (st *singleTask) createTask() error {
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
