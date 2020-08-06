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
	"io/ioutil"
	"path/filepath"
	"strings"
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/pingcap/check"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/pbmock"
	"github.com/pingcap/dm/pkg/terror"
)

const (
	// do not forget to update this path if the file removed/renamed.
	subTaskSampleFile = "../worker/subtask.toml"
)

func (t *testMaster) TestCollectSourceConfigFilesV1Import(c *C) {
	s := testDefaultMasterServer(c)
	defer s.Close()
	s.cfg.V1SourcesPath = c.MkDir()

	// no source file exist.
	cfgs, err := s.collectSourceConfigFilesV1Import()
	c.Assert(err, IsNil)
	c.Assert(cfgs, HasLen, 0)

	// load a valid source file.
	cfg1 := config.NewSourceConfig()
	cfg1.From.Session = map[string]string{} // fix empty map after marshal/unmarshal becomes nil
	c.Assert(cfg1.LoadFromFile("./source.yaml"), IsNil)
	cfg2 := cfg1.Clone()
	cfg2.SourceID = "mysql-replica-02"

	// write into source files.
	data1, err := cfg1.Yaml()
	c.Assert(err, IsNil)
	c.Assert(ioutil.WriteFile(filepath.Join(s.cfg.V1SourcesPath, "source1.yaml"), []byte(data1), 0644), IsNil)
	data2, err := cfg2.Yaml()
	c.Assert(err, IsNil)
	c.Assert(ioutil.WriteFile(filepath.Join(s.cfg.V1SourcesPath, "source2.yaml"), []byte(data2), 0644), IsNil)

	// collect again, two configs exist.
	cfgs, err = s.collectSourceConfigFilesV1Import()
	c.Assert(err, IsNil)
	c.Assert(cfgs, HasLen, 2)
	c.Assert(cfgs[cfg1.SourceID], DeepEquals, *cfg1)
	c.Assert(cfgs[cfg2.SourceID], DeepEquals, *cfg2)

	// put a invalid source file.
	c.Assert(ioutil.WriteFile(filepath.Join(s.cfg.V1SourcesPath, "invalid.yaml"), []byte("invalid-source-data"), 0644), IsNil)
	cfgs, err = s.collectSourceConfigFilesV1Import()
	c.Assert(terror.ErrConfigYamlTransform.Equal(err), IsTrue)
	c.Assert(cfgs, HasLen, 0)
}

func (t *testMaster) TestWaitWorkersReadyV1Import(c *C) {
	oldWaitWorkerV1Timeout := waitWorkerV1Timeout
	defer func() {
		waitWorkerV1Timeout = oldWaitWorkerV1Timeout
	}()
	waitWorkerV1Timeout = 5 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := testDefaultMasterServer(c)
	defer s.Close()
	s.cfg.V1SourcesPath = c.MkDir()
	c.Assert(s.scheduler.Start(ctx, etcdTestCli), IsNil)

	cfg1 := config.NewSourceConfig()
	c.Assert(cfg1.LoadFromFile("./source.yaml"), IsNil)
	cfg2 := cfg1.Clone()
	cfg2.SourceID = "mysql-replica-02"
	cfgs := map[string]config.SourceConfig{
		cfg1.SourceID: *cfg1,
		cfg2.SourceID: *cfg2,
	}

	// no worker registered, timeout.
	err := s.waitWorkersReadyV1Import(context.Background(), cfgs)
	c.Assert(err, ErrorMatches, ".*wait for DM-worker instances timeout.*")

	// register one worker.
	req1 := &pb.RegisterWorkerRequest{
		Name:    "worker-1",
		Address: "127.0.0.1:8262",
	}
	resp1, err := s.RegisterWorker(ctx, req1)
	c.Assert(err, IsNil)
	c.Assert(resp1.Result, IsTrue)

	// still timeout because no enough workers.
	err = s.waitWorkersReadyV1Import(context.Background(), cfgs)
	c.Assert(err, ErrorMatches, ".*wait for DM-worker instances timeout.*")

	// register another worker.
	go func() {
		time.Sleep(1500 * time.Millisecond)
		req2 := &pb.RegisterWorkerRequest{
			Name:    "worker-2",
			Address: "127.0.0.1:8263",
		}
		resp2, err := s.RegisterWorker(ctx, req2)
		c.Assert(err, IsNil)
		c.Assert(resp2.Result, IsTrue)
	}()

	err = s.waitWorkersReadyV1Import(context.Background(), cfgs)
	c.Assert(err, IsNil)
}

func (t *testMaster) TestSubtaskCfgsStagesV1Import(c *C) {
	var (
		worker1Name = "worker-1"
		worker1Addr = "127.0.0.1:8262"
		worker2Name = "worker-2"
		worker2Addr = "127.0.0.1:8263"
		taskName1   = "task-1"
		taskName2   = "task-2"
		sourceID1   = "mysql-replica-01"
		sourceID2   = "mysql-replica-02"
	)

	var cfg11 config.SubTaskConfig
	c.Assert(cfg11.DecodeFile(subTaskSampleFile, true), IsNil)
	cfg11.Name = taskName1
	cfg11.SourceID = sourceID1
	data11, err := cfg11.Toml()
	c.Assert(err, IsNil)
	data11 = strings.ReplaceAll(data11, `chunk-filesize = ""`, `chunk-filesize = 0`)

	cfg12, err := cfg11.Clone()
	c.Assert(err, IsNil)
	cfg12.SourceID = sourceID2
	data12, err := cfg12.Toml()
	c.Assert(err, IsNil)
	data12 = strings.ReplaceAll(data12, `chunk-filesize = ""`, `chunk-filesize = 0`)

	cfg21, err := cfg11.Clone()
	c.Assert(err, IsNil)
	cfg21.Name = taskName2
	data21, err := cfg21.Toml()
	c.Assert(err, IsNil)
	data21 = strings.ReplaceAll(data21, `chunk-filesize = ""`, `chunk-filesize = 0`)

	cfg22, err := cfg21.Clone()
	c.Assert(err, IsNil)
	cfg22.SourceID = sourceID2
	data22, err := cfg22.Toml()
	c.Assert(err, IsNil)
	data22 = strings.ReplaceAll(data22, `chunk-filesize = ""`, `chunk-filesize = 0`)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := testDefaultMasterServer(c)
	defer s.Close()
	s.cfg.V1SourcesPath = c.MkDir()
	c.Assert(s.scheduler.Start(ctx, etcdTestCli), IsNil)

	ctrl := gomock.NewController(c)
	defer ctrl.Finish()
	mockWCli1 := pbmock.NewMockWorkerClient(ctrl)
	mockWCli2 := pbmock.NewMockWorkerClient(ctrl)
	c.Assert(s.scheduler.AddWorker(worker1Name, worker1Addr), IsNil)
	c.Assert(s.scheduler.AddWorker(worker2Name, worker2Addr), IsNil)
	s.scheduler.SetWorkerClientForTest(worker1Name, newMockRPCClient(mockWCli1))
	s.scheduler.SetWorkerClientForTest(worker2Name, newMockRPCClient(mockWCli2))

	mockWCli1.EXPECT().OperateV1Meta(
		gomock.Any(),
		&pb.OperateV1MetaRequest{
			Op: pb.V1MetaOp_GetV1Meta,
		},
	).Return(&pb.OperateV1MetaResponse{
		Result: true,
		Meta: map[string]*pb.V1SubTaskMeta{
			taskName1: {
				Op:    pb.TaskOp_Start,
				Stage: pb.Stage_Running,
				Name:  taskName1,
				Task:  []byte(data11),
			},
			taskName2: {
				Op:    pb.TaskOp_Pause,
				Stage: pb.Stage_Paused,
				Name:  taskName2,
				Task:  []byte(data21),
			},
		},
	}, nil)

	mockWCli2.EXPECT().OperateV1Meta(
		gomock.Any(),
		&pb.OperateV1MetaRequest{
			Op: pb.V1MetaOp_GetV1Meta,
		},
	).Return(&pb.OperateV1MetaResponse{
		Result: true,
		Meta: map[string]*pb.V1SubTaskMeta{
			taskName1: {
				Op:    pb.TaskOp_Resume,
				Stage: pb.Stage_Running,
				Name:  taskName1,
				Task:  []byte(data12),
			},
			taskName2: {
				Op:    pb.TaskOp_Start,
				Stage: pb.Stage_Running,
				Name:  taskName2,
				Task:  []byte(data22),
			},
		},
	}, nil)

	// all workers return valid config and stage.
	cfgs, stages, err := s.getSubtaskCfgsStagesV1Import(ctx)
	c.Assert(err, IsNil)
	c.Assert(cfgs, HasLen, 2)
	c.Assert(stages, HasLen, 2)
}
