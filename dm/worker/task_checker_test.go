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
	c.Assert(ResumeStrategy(10000).String(), check.Equals, "unknown resume strategy: 10000")

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
		rtsc.latestResume = tc.latestResumeFn(tc.addition)
		strategy := rtsc.getResumeStrategy(tc.status, tc.duration)
		c.Assert(strategy, check.Equals, tc.expected)
	}
}

func (s *testTaskCheckerSuite) TestCheck(c *check.C) {
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

	// test resume with paused task
	rtsc.w.subTasks = map[string]*SubTask{
		"task1": {
			stage: pb.Stage_Paused,
			result: &pb.ProcessResult{
				IsCanceled: false,
				Errors:     []*pb.ProcessError{{pb.ErrorType_UnknownError, "error message"}},
			},
		},
	}
	time.Sleep(1 * time.Millisecond)
	rtsc.check()
	time.Sleep(2 * time.Millisecond)
	rtsc.check()
	time.Sleep(4 * time.Millisecond)
	rtsc.check()
	c.Assert(rtsc.bf.Current(), check.Equals, 8*time.Millisecond)
	c.Assert(w.meta.logs, check.HasLen, 3)
	for _, tl := range w.meta.logs {
		c.Assert(tl.Task, check.NotNil)
		c.Assert(tl.Task.Op, check.Equals, pb.TaskOp_AutoResume)
	}

	// test backoff rollback at least once, as well as resume ignore strategy
	rtsc.w.subTasks = map[string]*SubTask{
		"task2": {
			stage: pb.Stage_Paused,
			result: &pb.ProcessResult{
				IsCanceled: true,
			},
		},
	}
	w.meta.logs = []*pb.TaskLog{}
	time.Sleep(200 * time.Millisecond)
	rtsc.check()
	c.Assert(rtsc.bf.Current() <= 4*time.Millisecond, check.IsTrue)
	c.Assert(w.meta.logs, check.HasLen, 0)
	current := rtsc.bf.Current()

	// test no sense strategy
	rtsc.w.subTasks = map[string]*SubTask{
		"task3": {
			stage: pb.Stage_Paused,
			result: &pb.ProcessResult{
				IsCanceled: false,
				Errors:     []*pb.ProcessError{{pb.ErrorType_ExecSQL, "ERROR 1105 (HY000): unsupported modify column length 20 is less than origin 40"}},
			},
		},
	}
	latestNormalTime := rtsc.latestNormalTime
	rtsc.check()
	// check latestNormalTime is updated when we first meet no-auto-resume paused task
	c.Assert(latestNormalTime.Before(rtsc.latestNormalTime), check.IsTrue)
	latestNormalTime = rtsc.latestNormalTime
	rtsc.check()
	// check latestNormalTime is not updated when we meet historical no-auto-resume paused task
	c.Assert(rtsc.latestNormalTime, check.Equals, latestNormalTime)
	time.Sleep(200 * time.Millisecond)
	rtsc.check()
	c.Assert(rtsc.bf.Current(), check.Equals, current/2)
	c.Assert(w.meta.logs, check.HasLen, 0)

	// test resume skip strategy
	tsc = NewRealTaskStatusChecker(CheckerConfig{
		CheckEnable:     true,
		CheckInterval:   DefaultCheckInterval,
		BackoffRollback: 200 * time.Millisecond,
		BackoffMin:      10 * time.Second,
		BackoffMax:      100 * time.Second,
		BackoffFactor:   DefaultBackoffFactor,
	}, nil)
	c.Assert(tsc.Init(), check.IsNil)
	rtsc, ok = tsc.(*realTaskStatusChecker)
	c.Assert(ok, check.IsTrue)
	rtsc.w = w
	rtsc.w.subTasks = map[string]*SubTask{
		"task1": {
			stage: pb.Stage_Paused,
			result: &pb.ProcessResult{
				IsCanceled: false,
				Errors:     []*pb.ProcessError{{pb.ErrorType_UnknownError, "error message"}},
			},
		},
	}
	rtsc.check()
	c.Assert(w.meta.logs, check.HasLen, 1)
	c.Assert(rtsc.bf.Current(), check.Equals, 20*time.Second)
	for i := 0; i < 10; i++ {
		rtsc.check()
	}
	c.Assert(w.meta.logs, check.HasLen, 1)
}
