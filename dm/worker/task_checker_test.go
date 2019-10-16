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

	"github.com/pingcap/dm/dm/config"
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
		{&pb.SubTaskStatus{Name: taskName, Stage: pb.Stage_Paused, Result: &pb.ProcessResult{IsCanceled: false, Errors: []*pb.ProcessError{{Type: pb.ErrorType_ExecSQL, Msg: "ERROR 1105 (HY000): unsupported modify column length 20 is less than origin 40"}}}}, now, time.Duration(0), 1 * time.Millisecond, ResumeNoSense},
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
		rtsc.bc.latestResumeTime[taskName] = tc.latestResumeFn(tc.addition)
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

	st := &SubTask{
		cfg:   &config.SubTaskConfig{Name: taskName},
		stage: pb.Stage_Running,
	}
	rtsc.w.subTaskHolder.recordSubTask(st)
	rtsc.check()
	bf, ok := rtsc.bc.backoffs[taskName]
	c.Assert(ok, check.IsTrue)

	// test resume with paused task
	st.stage = pb.Stage_Paused
	st.result = &pb.ProcessResult{
		IsCanceled: false,
		Errors:     []*pb.ProcessError{{Type: pb.ErrorType_UnknownError, Msg: "error message"}},
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
	st.result = &pb.ProcessResult{IsCanceled: true}
	w.meta.logs = []*pb.TaskLog{}
	time.Sleep(200 * time.Millisecond)
	rtsc.check()
	c.Assert(bf.Current() <= 4*time.Millisecond, check.IsTrue)
	c.Assert(w.meta.logs, check.HasLen, 0)
	current := bf.Current()

	// test no sense strategy
	st.result = &pb.ProcessResult{
		IsCanceled: false,
		Errors:     []*pb.ProcessError{{Type: pb.ErrorType_ExecSQL, Msg: "ERROR 1105 (HY000): unsupported modify column length 20 is less than origin 40"}},
	}
	latestPausedTime = rtsc.bc.latestPausedTime[taskName]
	rtsc.check()
	c.Assert(latestPausedTime.Before(rtsc.bc.latestPausedTime[taskName]), check.IsTrue)
	latestPausedTime = rtsc.bc.latestPausedTime[taskName]
	latestBlockTime = rtsc.bc.latestBlockTime[taskName]
	time.Sleep(200 * time.Millisecond)
	rtsc.check()
	c.Assert(rtsc.bc.latestBlockTime[taskName], check.Equals, latestBlockTime)
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

	st = &SubTask{
		cfg:   &config.SubTaskConfig{Name: taskName},
		stage: pb.Stage_Running,
	}
	rtsc.w.subTaskHolder.recordSubTask(st)
	rtsc.check()
	bf, ok = rtsc.bc.backoffs[taskName]
	c.Assert(ok, check.IsTrue)

	st.stage = pb.Stage_Paused
	st.result = &pb.ProcessResult{
		IsCanceled: false,
		Errors:     []*pb.ProcessError{{Type: pb.ErrorType_UnknownError, Msg: "error message"}},
	}
	rtsc.check()
	latestResumeTime = rtsc.bc.latestResumeTime[taskName]
	latestPausedTime = rtsc.bc.latestPausedTime[taskName]
	c.Assert(bf.Current(), check.Equals, 10*time.Second)
	c.Assert(w.meta.logs, check.HasLen, 0)
	for i := 0; i < 10; i++ {
		rtsc.check()
		c.Assert(latestResumeTime, check.Equals, rtsc.bc.latestResumeTime[taskName])
		c.Assert(latestPausedTime.Before(rtsc.bc.latestPausedTime[taskName]), check.IsTrue)
		latestPausedTime = rtsc.bc.latestPausedTime[taskName]
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

	st1 := &SubTask{
		cfg:   &config.SubTaskConfig{Name: task1},
		stage: pb.Stage_Running,
	}
	rtsc.w.subTaskHolder.recordSubTask(st1)
	st2 := &SubTask{
		cfg:   &config.SubTaskConfig{Name: task2},
		stage: pb.Stage_Running,
	}
	rtsc.w.subTaskHolder.recordSubTask(st2)
	rtsc.check()
	c.Assert(len(rtsc.bc.backoffs), check.Equals, 2)
	c.Assert(len(rtsc.bc.latestPausedTime), check.Equals, 2)
	c.Assert(len(rtsc.bc.latestResumeTime), check.Equals, 2)
	c.Assert(len(rtsc.bc.latestBlockTime), check.Equals, 0)

	// test backoff strategies of different tasks do not affect each other
	st1 = &SubTask{
		cfg:   &config.SubTaskConfig{Name: task1},
		stage: pb.Stage_Paused,
		result: &pb.ProcessResult{
			IsCanceled: false,
			Errors:     []*pb.ProcessError{{Type: pb.ErrorType_ExecSQL, Msg: "ERROR 1105 (HY000): unsupported modify column length 20 is less than origin 40"}},
		},
	}
	rtsc.w.subTaskHolder.recordSubTask(st1)
	st2 = &SubTask{
		cfg:   &config.SubTaskConfig{Name: task2},
		stage: pb.Stage_Paused,
		result: &pb.ProcessResult{
			IsCanceled: false,
			Errors:     []*pb.ProcessError{{Type: pb.ErrorType_UnknownError, Msg: "error message"}},
		},
	}
	rtsc.w.subTaskHolder.recordSubTask(st2)

	task1LatestResumeTime = rtsc.bc.latestResumeTime[task1]
	task2LatestResumeTime = rtsc.bc.latestResumeTime[task2]
	for i := 0; i < 10; i++ {
		time.Sleep(backoffMin)
		rtsc.check()
		c.Assert(task1LatestResumeTime, check.Equals, rtsc.bc.latestResumeTime[task1])
		c.Assert(task2LatestResumeTime.Before(rtsc.bc.latestResumeTime[task2]), check.IsTrue)
		c.Assert(len(rtsc.bc.latestBlockTime), check.Equals, 1)
		task2LatestResumeTime = rtsc.bc.latestResumeTime[task2]
		c.Assert(w.meta.logs, check.HasLen, i+1)
	}

	// test task information cleanup in task status checker
	rtsc.w.subTaskHolder.removeSubTask(task1)
	time.Sleep(backoffMin)
	rtsc.check()
	c.Assert(task2LatestResumeTime.Before(rtsc.bc.latestResumeTime[task2]), check.IsTrue)
	c.Assert(w.meta.logs, check.HasLen, 11)
	c.Assert(len(rtsc.bc.backoffs), check.Equals, 1)
	c.Assert(len(rtsc.bc.latestPausedTime), check.Equals, 1)
	c.Assert(len(rtsc.bc.latestResumeTime), check.Equals, 1)
	c.Assert(len(rtsc.bc.latestBlockTime), check.Equals, 0)
}

func (s *testTaskCheckerSuite) TestIsResumableError(c *check.C) {
	testCases := []struct {
		err       *pb.ProcessError
		resumable bool
	}{
		{&pb.ProcessError{Type: pb.ErrorType_ExecSQL, Msg: "ERROR 1105 (HY000): unsupported modify column length 20 is less than origin 40"}, false},
		{&pb.ProcessError{Type: pb.ErrorType_ExecSQL, Msg: "ERROR 1105 (HY000): unsupported drop integer primary key"}, false},
		{&pb.ProcessError{Type: pb.ErrorType_ExecSQL, Msg: ""}, true},
		{&pb.ProcessError{Type: pb.ErrorType_ExecSQL, Msg: "[code=10006:class=database:scope=not-set:level=high] file test.t3.sql: execute statement failed: USE `test_abc`;: context canceled"}, true},
		{&pb.ProcessError{Type: pb.ErrorType_ExecSQL, Msg: "[code=10006:class=database:scope=not-set:level=high] execute statement failed: commit: Error 1062: Duplicate entry '5' for key 'PRIMARY'"}, false},
		{&pb.ProcessError{Type: pb.ErrorType_ExecSQL, Msg: "[code=10006:class=database:scope=not-set:level=high] execute statement failed: INSERT INTO `db`.`tbl` (`c1`,`c2`) VALUES (?,?);: Error 1406: Data too long for column 'c2' at row 1"}, false},
		{&pb.ProcessError{Type: pb.ErrorType_UnknownError, Msg: "[code=11038:class=functional:scope=internal:level=high] parse relay log file bin.000018 from offset 555 in dir /home/tidb/deploy/relay_log/d2e831df-b4ec-11e9-9237-0242ac110008.000004: parse relay log file bin.000018 from offset 0 in dir /home/tidb/deploy/relay_log/d2e831df-b4ec-11e9-9237-0242ac110008.000004: parse relay log file /home/tidb/deploy/relay_log/d2e831df-b4ec-11e9-9237-0242ac110008.000004/bin.000018: binlog checksum mismatch, data may be corrupted"}, false},
		{&pb.ProcessError{Type: pb.ErrorType_UnknownError, Msg: "[code=11038:class=functional:scope=internal:level=high] parse relay log file bin.000018 from offset 500 in dir /home/tidb/deploy/relay_log/d2e831df-b4ec-11e9-9237-0242ac110008.000004: parse relay log file bin.000018 from offset 0 in dir /home/tidb/deploy/relay_log/d2e831df-b4ec-11e9-9237-0242ac110008.000004: parse relay log file /home/tidb/deploy/relay_log/d2e831df-b4ec-11e9-9237-0242ac110008.000004/bin.000018: get event err EOF, need 1567488104 but got 316323"}, false},
		{&pb.ProcessError{Type: pb.ErrorType_UnknownError, Msg: ""}, true},
		{&pb.ProcessError{Type: pb.ErrorType_UnknownError, Msg: "unknown error"}, true},
	}

	for _, tc := range testCases {
		c.Assert(isResumableError(tc.err), check.Equals, tc.resumable)
	}
}
