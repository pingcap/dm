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
	"time"

	"github.com/pingcap/check"

	"github.com/pingcap/dm/dm/pb"
)

var _ = check.Suite(&testTaskCheckerSuite{})

type testTaskCheckerSuite struct{}

func (s *testTaskCheckerSuite) TestResumeStrategy(c *check.C) {
	c.Assert(ResumeSkip.String(), check.Equals, resumeStrategy2Str[ResumeSkip])
	c.Assert(ResumeStrategy(10000).String(), check.Equals, "unsupported resume strategy: 10000")

	taskName := "test-task"
	now := func(addition time.Duration) time.Time { return time.Now().Add(addition) }
	testCases := []struct {
		status         *pb.SubTaskStatus
		latestResumeFn func(addition time.Duration) time.Time
		addition       time.Duration
		duration       time.Duration
		expected       ResumeStrategy
	}{
		{nil, now, time.Duration(0), 1 * time.Millisecond, ResumeIgnore},
		{&pb.SubTaskStatus{Name: taskName, Stage: pb.Stage_Running}, now, time.Duration(0), 1 * time.Millisecond, ResumeIgnore},
		{&pb.SubTaskStatus{Name: taskName, Stage: pb.Stage_Paused}, now, time.Duration(0), 1 * time.Millisecond, ResumeIgnore},
		{&pb.SubTaskStatus{Name: taskName, Stage: pb.Stage_Paused, Result: &pb.ProcessResult{IsCanceled: true}}, now, time.Duration(0), 1 * time.Millisecond, ResumeIgnore},
		{&pb.SubTaskStatus{Name: taskName, Stage: pb.Stage_Paused, Result: &pb.ProcessResult{IsCanceled: false, Errors: []*pb.ProcessError{{pb.ErrorType_ExecSQL, "ERROR 1105 (HY000): unsupported modify column length 20 is less than origin 40"}}}}, now, time.Duration(0), 1 * time.Millisecond, ResumeNoSense},
		{&pb.SubTaskStatus{Name: taskName, Stage: pb.Stage_Paused, Result: &pb.ProcessResult{IsCanceled: false}}, now, time.Duration(0), 1 * time.Second, ResumeSkip},
		{&pb.SubTaskStatus{Name: taskName, Stage: pb.Stage_Paused, Result: &pb.ProcessResult{IsCanceled: false}}, now, -2 * time.Millisecond, 1 * time.Millisecond, ResumeDispatch},
	}

	tsc := NewRealTaskStatusChecker(CheckerConfig{
		CheckEnable:     true,
		CheckInterval:   DefaultCheckInterval,
		BackoffRollback: DefaultBackoffRollback,
		BackoffMin:      DefaultBackoffMin,
		BackoffMax:      DefaultBackoffMax,
		BackoffFactor:   DefaultBackoffFactor,
	}, nil)
	for _, tc := range testCases {
		rtsc, ok := tsc.(*realTaskStatusChecker)
		c.Assert(ok, check.IsTrue)
		rtsc.latestResumeTime[taskName] = tc.latestResumeFn(tc.addition)
		strategy := rtsc.getResumeStrategy(tc.status, tc.duration)
		c.Assert(strategy, check.Equals, tc.expected)
	}
}

func (s *testTaskCheckerSuite) TestCheck(c *check.C) {
	var (
		latestResumeTime time.Time
		latestPausedTime time.Time
		latestBlockTime  time.Time
		taskName         = "test-check-task"
	)

	NewRelayHolder = NewDummyRelayHolder
	dir := c.MkDir()
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), check.IsNil)
	cfg.RelayDir = dir
	cfg.MetaDir = dir
	w, err := NewWorker(cfg)
	c.Assert(err, check.IsNil)

	tsc := NewRealTaskStatusChecker(CheckerConfig{
		CheckEnable:     true,
		CheckInterval:   DefaultCheckInterval,
		BackoffRollback: 200 * time.Millisecond,
		BackoffMin:      1 * time.Millisecond,
		BackoffMax:      1 * time.Second,
		BackoffFactor:   DefaultBackoffFactor,
	}, nil)
	c.Assert(tsc.Init(), check.IsNil)
	rtsc, ok := tsc.(*realTaskStatusChecker)
	c.Assert(ok, check.IsTrue)
	rtsc.w = w

	rtsc.w.subTasks = map[string]*SubTask{
		taskName: {
			stage: pb.Stage_Running,
		},
	}
	rtsc.check()
	bf, ok := rtsc.backoffs[taskName]
	c.Assert(ok, check.IsTrue)

	// test resume with paused task
	rtsc.w.subTasks[taskName].stage = pb.Stage_Paused
	rtsc.w.subTasks[taskName].result = &pb.ProcessResult{
		IsCanceled: false,
		Errors:     []*pb.ProcessError{{pb.ErrorType_UnknownError, "error message"}},
	}
	time.Sleep(1 * time.Millisecond)
	rtsc.check()
	time.Sleep(2 * time.Millisecond)
	rtsc.check()
	time.Sleep(4 * time.Millisecond)
	rtsc.check()
	c.Assert(bf.Current(), check.Equals, 8*time.Millisecond)
	c.Assert(w.meta.logs, check.HasLen, 3)
	for _, tl := range w.meta.logs {
		c.Assert(tl.Task, check.NotNil)
		c.Assert(tl.Task.Op, check.Equals, pb.TaskOp_AutoResume)
	}

	// test backoff rollback at least once, as well as resume ignore strategy
	rtsc.w.subTasks[taskName].result = &pb.ProcessResult{IsCanceled: true}
	w.meta.logs = []*pb.TaskLog{}
	time.Sleep(200 * time.Millisecond)
	rtsc.check()
	c.Assert(bf.Current() <= 4*time.Millisecond, check.IsTrue)
	c.Assert(w.meta.logs, check.HasLen, 0)
	current := bf.Current()

	// test no sense strategy
	rtsc.w.subTasks[taskName].result = &pb.ProcessResult{
		IsCanceled: false,
		Errors:     []*pb.ProcessError{{pb.ErrorType_ExecSQL, "ERROR 1105 (HY000): unsupported modify column length 20 is less than origin 40"}},
	}
	latestPausedTime = rtsc.latestPausedTime[taskName]
	rtsc.check()
	c.Assert(latestPausedTime.Before(rtsc.latestPausedTime[taskName]), check.IsTrue)
	latestPausedTime = rtsc.latestPausedTime[taskName]
	latestBlockTime = rtsc.latestBlockTime[taskName]
	time.Sleep(200 * time.Millisecond)
	rtsc.check()
	c.Assert(rtsc.latestBlockTime[taskName], check.Equals, latestBlockTime)
	c.Assert(bf.Current(), check.Equals, current)
	c.Assert(w.meta.logs, check.HasLen, 0)

	// test resume skip strategy
	tsc = NewRealTaskStatusChecker(CheckerConfig{
		CheckEnable:     true,
		CheckInterval:   DefaultCheckInterval,
		BackoffRollback: 200 * time.Millisecond,
		BackoffMin:      10 * time.Second,
		BackoffMax:      100 * time.Second,
		BackoffFactor:   DefaultBackoffFactor,
	}, w)
	c.Assert(tsc.Init(), check.IsNil)
	rtsc, ok = tsc.(*realTaskStatusChecker)
	c.Assert(ok, check.IsTrue)

	rtsc.w.subTasks = map[string]*SubTask{
		taskName: {
			stage: pb.Stage_Running,
		},
	}
	rtsc.check()
	bf, ok = rtsc.backoffs[taskName]
	c.Assert(ok, check.IsTrue)

	rtsc.w.subTasks[taskName].stage = pb.Stage_Paused
	rtsc.w.subTasks[taskName].result = &pb.ProcessResult{
		IsCanceled: false,
		Errors:     []*pb.ProcessError{{pb.ErrorType_UnknownError, "error message"}},
	}
	rtsc.check()
	latestResumeTime = rtsc.latestResumeTime[taskName]
	latestPausedTime = rtsc.latestPausedTime[taskName]
	c.Assert(bf.Current(), check.Equals, 10*time.Second)
	c.Assert(w.meta.logs, check.HasLen, 0)
	for i := 0; i < 10; i++ {
		rtsc.check()
		c.Assert(latestResumeTime, check.Equals, rtsc.latestResumeTime[taskName])
		c.Assert(latestPausedTime.Before(rtsc.latestPausedTime[taskName]), check.IsTrue)
		latestPausedTime = rtsc.latestPausedTime[taskName]
	}
	c.Assert(w.meta.logs, check.HasLen, 0)
}

func (s *testTaskCheckerSuite) TestCheckTaskIndependent(c *check.C) {
	var (
		task1                 = "task1"
		task2                 = "tesk2"
		task1LatestResumeTime time.Time
		task2LatestResumeTime time.Time
		backoffMin            = 5 * time.Millisecond
	)

	NewRelayHolder = NewDummyRelayHolder
	dir := c.MkDir()
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), check.IsNil)
	cfg.RelayDir = dir
	cfg.MetaDir = dir
	w, err := NewWorker(cfg)
	c.Assert(err, check.IsNil)

	tsc := NewRealTaskStatusChecker(CheckerConfig{
		CheckEnable:     true,
		CheckInterval:   DefaultCheckInterval,
		BackoffRollback: 200 * time.Millisecond,
		BackoffMin:      backoffMin,
		BackoffMax:      10 * time.Second,
		BackoffFactor:   1.0,
	}, nil)
	c.Assert(tsc.Init(), check.IsNil)
	rtsc, ok := tsc.(*realTaskStatusChecker)
	c.Assert(ok, check.IsTrue)
	rtsc.w = w

	rtsc.w.subTasks = map[string]*SubTask{
		task1: {
			stage: pb.Stage_Running,
		},
		task2: {
			stage: pb.Stage_Running,
		},
	}
	rtsc.check()
	c.Assert(len(rtsc.backoffs), check.Equals, 2)
	c.Assert(len(rtsc.latestPausedTime), check.Equals, 2)
	c.Assert(len(rtsc.latestResumeTime), check.Equals, 2)
	c.Assert(len(rtsc.latestBlockTime), check.Equals, 0)

	// test backoff strategies of different tasks do not affect each other
	rtsc.w.subTasks[task1].stage = pb.Stage_Paused
	rtsc.w.subTasks[task1].result = &pb.ProcessResult{
		IsCanceled: false,
		Errors:     []*pb.ProcessError{{pb.ErrorType_ExecSQL, "ERROR 1105 (HY000): unsupported modify column length 20 is less than origin 40"}},
	}
	rtsc.w.subTasks[task2].stage = pb.Stage_Paused
	rtsc.w.subTasks[task2].result = &pb.ProcessResult{
		IsCanceled: false,
		Errors:     []*pb.ProcessError{{pb.ErrorType_UnknownError, "error message"}},
	}
	task1LatestResumeTime = rtsc.latestResumeTime[task1]
	task2LatestResumeTime = rtsc.latestResumeTime[task2]
	for i := 0; i < 10; i++ {
		time.Sleep(backoffMin)
		rtsc.check()
		c.Assert(task1LatestResumeTime, check.Equals, rtsc.latestResumeTime[task1])
		c.Assert(task2LatestResumeTime.Before(rtsc.latestResumeTime[task2]), check.IsTrue)
		c.Assert(len(rtsc.latestBlockTime), check.Equals, 1)
		task2LatestResumeTime = rtsc.latestResumeTime[task2]
		c.Assert(w.meta.logs, check.HasLen, i+1)
	}

	// test task information cleanup in task status checker
	delete(rtsc.w.subTasks, task1)
	time.Sleep(backoffMin)
	rtsc.check()
	c.Assert(task2LatestResumeTime.Before(rtsc.latestResumeTime[task2]), check.IsTrue)
	c.Assert(w.meta.logs, check.HasLen, 11)
	c.Assert(len(rtsc.backoffs), check.Equals, 1)
	c.Assert(len(rtsc.latestPausedTime), check.Equals, 1)
	c.Assert(len(rtsc.latestResumeTime), check.Equals, 1)
	c.Assert(len(rtsc.latestBlockTime), check.Equals, 0)
}
