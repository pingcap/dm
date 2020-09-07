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

package worker

import (
	"context"
	"fmt"
	"io/ioutil"
	"strings"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/tikv/pd/pkg/tempurl"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/unit"
	"github.com/pingcap/dm/pkg/ha"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/utils"
)

var emptyWorkerStatusInfoJSONLength = 25

func (t *testServer) testWorker(c *C) {
	cfg := loadSourceConfigWithoutPassword(c)

	dir := c.MkDir()
	cfg.EnableRelay = true
	cfg.RelayDir = dir
	cfg.MetaDir = dir

	NewRelayHolder = NewDummyRelayHolderWithInitError
	defer func() {
		NewRelayHolder = NewRealRelayHolder
	}()

	_, err := NewWorker(&cfg, nil, "")
	c.Assert(err, ErrorMatches, "init error")

	NewRelayHolder = NewDummyRelayHolder
	w, err := NewWorker(&cfg, nil, "")
	c.Assert(err, IsNil)
	c.Assert(w.StatusJSON(""), HasLen, emptyWorkerStatusInfoJSONLength)
	//c.Assert(w.closed.Get(), Equals, closedFalse)
	//go func() {
	//	w.Start()
	//}()

	// close twice
	w.Close()
	c.Assert(w.closed.Get(), Equals, closedTrue)
	c.Assert(w.subTaskHolder.getAllSubTasks(), HasLen, 0)
	w.Close()
	c.Assert(w.closed.Get(), Equals, closedTrue)
	c.Assert(w.subTaskHolder.getAllSubTasks(), HasLen, 0)
	c.Assert(w.closed.Get(), Equals, closedTrue)

	w.StartSubTask(&config.SubTaskConfig{
		Name: "testStartTask",
	})
	task := w.subTaskHolder.findSubTask("testStartTask")
	c.Assert(task, NotNil)
	c.Assert(task.Result().String(), Matches, ".*worker already closed.*")

	err = w.UpdateSubTask(&config.SubTaskConfig{
		Name: "testStartTask",
	})
	c.Assert(err, ErrorMatches, ".*worker already closed.*")

	err = w.OperateSubTask("testSubTask", pb.TaskOp_Stop)
	c.Assert(err, ErrorMatches, ".*worker already closed.*")
}

type testServer2 struct{}

var _ = Suite(&testServer2{})

func (t *testServer2) SetUpSuite(c *C) {
	err := log.InitLogger(&log.Config{})
	c.Assert(err, IsNil)

	getMinPosForSubTaskFunc = getFakePosForSubTask
}

func (t *testServer2) TearDownSuite(c *C) {
	getMinPosForSubTaskFunc = getMinPosForSubTask
}

func (t *testServer2) TestTaskAutoResume(c *C) {
	var (
		taskName = "sub-task-name"
		port     = 8263
	)
	hostName := "127.0.0.1:8261"
	etcdDir := c.MkDir()
	ETCD, err := createMockETCD(etcdDir, "host://"+hostName)
	c.Assert(err, IsNil)
	defer ETCD.Close()

	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), IsNil)
	sourceConfig := loadSourceConfigWithoutPassword(c)
	sourceConfig.Checker.CheckEnable = true
	sourceConfig.Checker.CheckInterval = config.Duration{Duration: 40 * time.Millisecond}
	sourceConfig.Checker.BackoffMin = config.Duration{Duration: 20 * time.Millisecond}
	sourceConfig.Checker.BackoffMax = config.Duration{Duration: 1 * time.Second}

	cfg.WorkerAddr = fmt.Sprintf(":%d", port)

	dir := c.MkDir()
	sourceConfig.RelayDir = dir
	sourceConfig.MetaDir = dir
	sourceConfig.EnableRelay = true

	NewRelayHolder = NewDummyRelayHolder
	defer func() {
		NewRelayHolder = NewRealRelayHolder
	}()

	c.Assert(failpoint.Enable("github.com/pingcap/dm/dumpling/dumpUnitProcessForever", `return(true)`), IsNil)
	defer failpoint.Disable("github.com/pingcap/dm/dumpling/dumpUnitProcessForever")
	c.Assert(failpoint.Enable("github.com/pingcap/dm/dm/worker/mockCreateUnitsDumpOnly", `return(true)`), IsNil)
	defer failpoint.Disable("github.com/pingcap/dm/dm/worker/mockCreateUnitsDumpOnly")
	c.Assert(failpoint.Enable("github.com/pingcap/dm/loader/ignoreLoadCheckpointErr", `return()`), IsNil)
	defer failpoint.Disable("github.com/pingcap/dm/loader/ignoreLoadCheckpointErr")
	c.Assert(failpoint.Enable("github.com/pingcap/dm/dumpling/dumpUnitProcessWithError", `return("test auto resume inject error")`), IsNil)
	defer failpoint.Disable("github.com/pingcap/dm/dumpling/dumpUnitProcessWithError")

	s := NewServer(cfg)

	defer s.Close()
	go func() {
		c.Assert(s.Start(), IsNil)
	}()
	c.Assert(utils.WaitSomething(10, 100*time.Millisecond, func() bool {
		if s.closed.Get() {
			return false
		}
		c.Assert(s.startWorker(&sourceConfig), IsNil)
		return true
	}), IsTrue)
	// start task
	cli := t.createClient(c, fmt.Sprintf("127.0.0.1:%d", port))
	subtaskCfgBytes, err := ioutil.ReadFile("./subtask.toml")
	// strings.Replace is used here to uncomment extra-args to avoid mydumper connecting to DB and generating arg --tables-list which will cause failure
	_, err = cli.StartSubTask(context.Background(), &pb.StartSubTaskRequest{Task: strings.Replace(string(subtaskCfgBytes), "#extra-args", "extra-args", 1)})
	c.Assert(err, IsNil)

	// check task in paused state
	c.Assert(utils.WaitSomething(100, 100*time.Millisecond, func() bool {
		for _, st := range s.getWorker(true).QueryStatus(taskName) {
			if st.Name == taskName && st.Stage == pb.Stage_Paused {
				return true
			}
		}
		return false
	}), IsTrue)
	failpoint.Disable("github.com/pingcap/dm/dumpling/dumpUnitProcessWithError")

	rtsc, ok := s.getWorker(true).taskStatusChecker.(*realTaskStatusChecker)
	c.Assert(ok, IsTrue)
	defer func() {
		// close multiple time
		rtsc.Close()
		rtsc.Close()
	}()

	// check task will be auto resumed
	c.Assert(utils.WaitSomething(10, 100*time.Millisecond, func() bool {
		sts := s.getWorker(true).QueryStatus(taskName)
		for _, st := range sts {
			if st.Name == taskName && st.Stage == pb.Stage_Running {
				return true
			}
		}
		c.Log(sts)
		return false
	}), IsTrue)
}

func (t *testServer2) createClient(c *C, addr string) pb.WorkerClient {
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBackoffMaxDelay(3*time.Second))
	c.Assert(err, IsNil)
	return pb.NewWorkerClient(conn)
}

type testWorkerEtcdCompact struct{}

var _ = Suite(&testWorkerEtcdCompact{})

func (t *testWorkerEtcdCompact) SetUpSuite(c *C) {
	NewRelayHolder = NewDummyRelayHolder
	NewSubTask = func(cfg *config.SubTaskConfig, etcdClient *clientv3.Client) *SubTask {
		cfg.UseRelay = false
		return NewRealSubTask(cfg, etcdClient)
	}
	createUnits = func(cfg *config.SubTaskConfig, etcdClient *clientv3.Client) []unit.Unit {
		mockDumper := NewMockUnit(pb.UnitType_Dump)
		mockLoader := NewMockUnit(pb.UnitType_Load)
		mockSync := NewMockUnit(pb.UnitType_Sync)
		return []unit.Unit{mockDumper, mockLoader, mockSync}
	}
}

func (t *testWorkerEtcdCompact) TearDownSuite(c *C) {
	NewRelayHolder = NewRealRelayHolder
	NewSubTask = NewRealSubTask
	createUnits = createRealUnits
}

func (t *testWorkerEtcdCompact) TestWatchSubtaskStageEtcdCompact(c *C) {
	var (
		masterAddr   = tempurl.Alloc()[len("http://"):]
		keepAliveTTL = int64(1)
		startRev     = int64(1)
	)
	etcdDir := c.MkDir()
	ETCD, err := createMockETCD(etcdDir, "host://"+masterAddr)
	c.Assert(err, IsNil)
	defer ETCD.Close()
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), IsNil)
	cfg.Join = masterAddr
	cfg.KeepAliveTTL = keepAliveTTL

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:            GetJoinURLs(cfg.Join),
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    keepaliveTime,
		DialKeepAliveTimeout: keepaliveTimeout,
	})
	c.Assert(err, IsNil)
	sourceCfg := loadSourceConfigWithoutPassword(c)
	sourceCfg.EnableRelay = false

	// step 1: start worker
	w, err := NewWorker(&sourceCfg, etcdCli, "")
	c.Assert(err, IsNil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer w.Close()
	go func() {
		w.Start(false)
	}()
	c.Assert(utils.WaitSomething(50, 100*time.Millisecond, func() bool {
		return w.closed.Get() == closedFalse
	}), IsTrue)
	// step 2: Put a subtask config and subtask stage to this source, then delete it
	subtaskCfg := config.SubTaskConfig{}
	err = subtaskCfg.DecodeFile(subtaskSampleFile, true)
	c.Assert(err, IsNil)
	subtaskCfg.MydumperPath = mydumperPath

	_, err = ha.PutSubTaskCfgStage(etcdCli, []config.SubTaskConfig{subtaskCfg},
		[]ha.Stage{ha.NewSubTaskStage(pb.Stage_Running, sourceCfg.SourceID, subtaskCfg.Name)})
	c.Assert(err, IsNil)
	rev, err := ha.DeleteSubTaskCfgStage(etcdCli, []config.SubTaskConfig{subtaskCfg},
		[]ha.Stage{ha.NewSubTaskStage(pb.Stage_Stopped, sourceCfg.SourceID, subtaskCfg.Name)})
	c.Assert(err, IsNil)
	// step 2.1: start a subtask manually
	w.StartSubTask(&subtaskCfg)
	// step 3: trigger etcd compaction and check whether we can receive it through watcher
	_, err = etcdCli.Compact(ctx, rev)
	c.Assert(err, IsNil)
	subTaskStageCh := make(chan ha.Stage, 10)
	subTaskErrCh := make(chan error, 10)
	ha.WatchSubTaskStage(ctx, etcdCli, sourceCfg.SourceID, startRev, subTaskStageCh, subTaskErrCh)
	select {
	case err = <-subTaskErrCh:
		c.Assert(err, Equals, etcdErrCompacted)
	case <-time.After(300 * time.Millisecond):
		c.Fatal("fail to get etcd error compacted")
	}
	// step 4: watch subtask stage from startRev
	c.Assert(w.subTaskHolder.findSubTask(subtaskCfg.Name), NotNil)
	var wg sync.WaitGroup
	ctx1, cancel1 := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Assert(w.observeSubtaskStage(ctx1, etcdCli, startRev), IsNil)
	}()
	time.Sleep(time.Second)
	// step 4.1: after observe, invalid subtask should be removed
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return w.subTaskHolder.findSubTask(subtaskCfg.Name) == nil
	}), IsTrue)
	// step 4.2: add a new subtask stage, worker should receive and start it
	_, err = ha.PutSubTaskCfgStage(etcdCli, []config.SubTaskConfig{subtaskCfg},
		[]ha.Stage{ha.NewSubTaskStage(pb.Stage_Running, sourceCfg.SourceID, subtaskCfg.Name)})
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return w.subTaskHolder.findSubTask(subtaskCfg.Name) != nil
	}), IsTrue)
	status := w.QueryStatus(subtaskCfg.Name)
	c.Assert(status, HasLen, 1)
	c.Assert(status[0].Name, Equals, subtaskCfg.Name)
	c.Assert(status[0].Stage, Equals, pb.Stage_Running)
	cancel1()
	wg.Wait()
	w.subTaskHolder.closeAllSubTasks()
	// step 5: restart observe and start from startRev, this subtask should be added
	ctx2, cancel2 := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Assert(w.observeSubtaskStage(ctx2, etcdCli, startRev), IsNil)
	}()
	time.Sleep(time.Second)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return w.subTaskHolder.findSubTask(subtaskCfg.Name) != nil
	}), IsTrue)
	status = w.QueryStatus(subtaskCfg.Name)
	c.Assert(status, HasLen, 1)
	c.Assert(status[0].Name, Equals, subtaskCfg.Name)
	c.Assert(status[0].Stage, Equals, pb.Stage_Running)
	w.Close()
	cancel2()
	wg.Wait()
}

func (t *testWorkerEtcdCompact) TestWatchRelayStageEtcdCompact(c *C) {
	var (
		masterAddr   = tempurl.Alloc()[len("http://"):]
		keepAliveTTL = int64(1)
		startRev     = int64(1)
	)
	etcdDir := c.MkDir()
	ETCD, err := createMockETCD(etcdDir, "host://"+masterAddr)
	c.Assert(err, IsNil)
	defer ETCD.Close()
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), IsNil)
	cfg.Join = masterAddr
	cfg.KeepAliveTTL = keepAliveTTL

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:            GetJoinURLs(cfg.Join),
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    keepaliveTime,
		DialKeepAliveTimeout: keepaliveTimeout,
	})
	c.Assert(err, IsNil)
	sourceCfg := loadSourceConfigWithoutPassword(c)
	sourceCfg.EnableRelay = true
	sourceCfg.RelayDir = c.MkDir()
	sourceCfg.MetaDir = c.MkDir()

	// step 1: start worker
	w, err := NewWorker(&sourceCfg, etcdCli, "")
	c.Assert(err, IsNil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer w.Close()
	go func() {
		w.Start(true)
	}()
	c.Assert(utils.WaitSomething(50, 100*time.Millisecond, func() bool {
		return w.closed.Get() == closedFalse
	}), IsTrue)
	// step 2: Put a relay stage to this source, then delete it
	// put mysql config into relative etcd key adapter to trigger operation event
	c.Assert(w.relayHolder, NotNil)
	_, err = ha.PutSourceCfg(etcdCli, sourceCfg)
	c.Assert(err, IsNil)
	_, err = ha.PutRelayStageSourceBound(etcdCli, ha.NewRelayStage(pb.Stage_Running, sourceCfg.SourceID),
		ha.NewSourceBound(sourceCfg.SourceID, cfg.Name))
	c.Assert(err, IsNil)
	rev, err := ha.DeleteSourceCfgRelayStageSourceBound(etcdCli, sourceCfg.SourceID, cfg.Name)
	c.Assert(err, IsNil)
	// check relay stage, should be running
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return w.relayHolder.Stage() == pb.Stage_Running
	}), IsTrue)
	// step 3: trigger etcd compaction and check whether we can receive it through watcher
	_, err = etcdCli.Compact(ctx, rev)
	c.Assert(err, IsNil)
	relayStageCh := make(chan ha.Stage, 10)
	relayErrCh := make(chan error, 10)
	ha.WatchRelayStage(ctx, etcdCli, cfg.Name, startRev, relayStageCh, relayErrCh)
	select {
	case err := <-relayErrCh:
		c.Assert(err, Equals, etcdErrCompacted)
	case <-time.After(300 * time.Millisecond):
		c.Fatal("fail to get etcd error compacted")
	}
	// step 4: watch relay stage from startRev
	var wg sync.WaitGroup
	ctx1, cancel1 := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Assert(w.observeRelayStage(ctx1, etcdCli, startRev), IsNil)
	}()
	// step 5: should stop the running relay
	time.Sleep(time.Second)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return w.relayHolder.Stage() == pb.Stage_Stopped
	}), IsTrue)
	cancel1()
	wg.Wait()
}
