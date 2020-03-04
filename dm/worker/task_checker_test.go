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
	"fmt"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	tmysql "github.com/pingcap/parser/mysql"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/unit"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
)

var _ = check.Suite(&testTaskCheckerSuite{})

type testTaskCheckerSuite struct{}

var (
	unsupporteModifyColumnError = unit.NewProcessError(terror.ErrDBExecuteFailed.Delegate(&tmysql.SQLError{1105, "unsupported modify column length 20 is less than origin 40", tmysql.DefaultMySQLState}))
	unknownProcessError         = unit.NewProcessError(errors.New("error mesage"))
)

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
		{&pb.SubTaskStatus{Name: taskName, Stage: pb.Stage_Paused, Result: &pb.ProcessResult{IsCanceled: false, Errors: []*pb.ProcessError{unsupporteModifyColumnError}}}, now, time.Duration(0), 1 * time.Millisecond, ResumeNoSense},
		{&pb.SubTaskStatus{Name: taskName, Stage: pb.Stage_Paused, Result: &pb.ProcessResult{IsCanceled: false}}, now, time.Duration(0), 1 * time.Second, ResumeSkip},
		{&pb.SubTaskStatus{Name: taskName, Stage: pb.Stage_Paused, Result: &pb.ProcessResult{IsCanceled: false}}, now, -2 * time.Millisecond, 1 * time.Millisecond, ResumeDispatch},
	}

	tsc := NewRealTaskStatusChecker(config.CheckerConfig{
		CheckEnable:     true,
		CheckInterval:   config.Duration{Duration: config.DefaultCheckInterval},
		BackoffRollback: config.Duration{Duration: config.DefaultBackoffRollback},
		BackoffMin:      config.Duration{Duration: config.DefaultBackoffMin},
		BackoffMax:      config.Duration{Duration: config.DefaultBackoffMax},
		BackoffFactor:   config.DefaultBackoffFactor,
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
	cfg := loadSourceConfigWithoutPassword(c)
	cfg.RelayDir = dir
	cfg.MetaDir = dir
	w, err := NewWorker(&cfg, nil)
	c.Assert(err, check.IsNil)
	w.closed.Set(closedFalse)

	tsc := NewRealTaskStatusChecker(config.CheckerConfig{
		CheckEnable:     true,
		CheckInterval:   config.Duration{Duration: config.DefaultCheckInterval},
		BackoffRollback: config.Duration{Duration: 200 * time.Millisecond},
		BackoffMin:      config.Duration{Duration: 1 * time.Millisecond},
		BackoffMax:      config.Duration{Duration: 1 * time.Second},
		BackoffFactor:   config.DefaultBackoffFactor,
	}, nil)
	c.Assert(tsc.Init(), check.IsNil)
	rtsc, ok := tsc.(*realTaskStatusChecker)
	c.Assert(ok, check.IsTrue)
	rtsc.w = w

	st := &SubTask{
		cfg:   &config.SubTaskConfig{Name: taskName},
		stage: pb.Stage_Running,
		l:     log.With(zap.String("subtask", taskName)),
	}
	rtsc.w.subTaskHolder.recordSubTask(st)
	rtsc.check()
	bf, ok := rtsc.bc.backoffs[taskName]
	c.Assert(ok, check.IsTrue)

	// test resume with paused task
	st.stage = pb.Stage_Paused
	st.result = &pb.ProcessResult{
		IsCanceled: false,
		Errors:     []*pb.ProcessError{unknownProcessError},
	}
	time.Sleep(1 * time.Millisecond)
	rtsc.check()
	time.Sleep(2 * time.Millisecond)
	rtsc.check()
	time.Sleep(4 * time.Millisecond)
	rtsc.check()
	c.Assert(bf.Current(), check.Equals, 8*time.Millisecond)

	// test backoff rollback at least once, as well as resume ignore strategy
	st.result = &pb.ProcessResult{IsCanceled: true}
	time.Sleep(200 * time.Millisecond)
	rtsc.check()
	c.Assert(bf.Current() <= 4*time.Millisecond, check.IsTrue)
	current := bf.Current()

	// test no sense strategy
	st.result = &pb.ProcessResult{
		IsCanceled: false,
		Errors:     []*pb.ProcessError{unsupporteModifyColumnError},
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

	// test resume skip strategy
	tsc = NewRealTaskStatusChecker(config.CheckerConfig{
		CheckEnable:     true,
		CheckInterval:   config.Duration{Duration: config.DefaultCheckInterval},
		BackoffRollback: config.Duration{Duration: 200 * time.Millisecond},
		BackoffMin:      config.Duration{Duration: 10 * time.Second},
		BackoffMax:      config.Duration{Duration: 100 * time.Second},
		BackoffFactor:   config.DefaultBackoffFactor,
	}, w)
	c.Assert(tsc.Init(), check.IsNil)
	rtsc, ok = tsc.(*realTaskStatusChecker)
	c.Assert(ok, check.IsTrue)

	st = &SubTask{
		cfg:   &config.SubTaskConfig{Name: taskName},
		stage: pb.Stage_Running,
		l:     log.With(zap.String("subtask", taskName)),
	}
	rtsc.w.subTaskHolder.recordSubTask(st)
	rtsc.check()
	bf, ok = rtsc.bc.backoffs[taskName]
	c.Assert(ok, check.IsTrue)

	st.stage = pb.Stage_Paused
	st.result = &pb.ProcessResult{
		IsCanceled: false,
		Errors:     []*pb.ProcessError{unknownProcessError},
	}
	rtsc.check()
	latestResumeTime = rtsc.bc.latestResumeTime[taskName]
	latestPausedTime = rtsc.bc.latestPausedTime[taskName]
	c.Assert(bf.Current(), check.Equals, 10*time.Second)
	for i := 0; i < 10; i++ {
		rtsc.check()
		c.Assert(latestResumeTime, check.Equals, rtsc.bc.latestResumeTime[taskName])
		c.Assert(latestPausedTime.Before(rtsc.bc.latestPausedTime[taskName]), check.IsTrue)
		latestPausedTime = rtsc.bc.latestPausedTime[taskName]
	}
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
	cfg := loadSourceConfigWithoutPassword(c)
	cfg.RelayDir = dir
	cfg.MetaDir = dir
	w, err := NewWorker(&cfg, nil)
	c.Assert(err, check.IsNil)
	w.closed.Set(closedFalse)

	tsc := NewRealTaskStatusChecker(config.CheckerConfig{
		CheckEnable:     true,
		CheckInterval:   config.Duration{Duration: config.DefaultCheckInterval},
		BackoffRollback: config.Duration{Duration: 200 * time.Millisecond},
		BackoffMin:      config.Duration{Duration: backoffMin},
		BackoffMax:      config.Duration{Duration: 10 * time.Second},
		BackoffFactor:   1.0,
	}, nil)
	c.Assert(tsc.Init(), check.IsNil)
	rtsc, ok := tsc.(*realTaskStatusChecker)
	c.Assert(ok, check.IsTrue)
	rtsc.w = w

	st1 := &SubTask{
		cfg:   &config.SubTaskConfig{Name: task1},
		stage: pb.Stage_Running,
		l:     log.With(zap.String("subtask", task1)),
	}
	rtsc.w.subTaskHolder.recordSubTask(st1)
	st2 := &SubTask{
		cfg:   &config.SubTaskConfig{Name: task2},
		stage: pb.Stage_Running,
		l:     log.With(zap.String("subtask", task2)),
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
			Errors:     []*pb.ProcessError{unsupporteModifyColumnError},
		},
		l: log.With(zap.String("subtask", task1)),
	}
	rtsc.w.subTaskHolder.recordSubTask(st1)
	st2 = &SubTask{
		cfg:   &config.SubTaskConfig{Name: task2},
		stage: pb.Stage_Paused,
		result: &pb.ProcessResult{
			IsCanceled: false,
			Errors:     []*pb.ProcessError{unknownProcessError},
		},
		l: log.With(zap.String("subtask", task2)),
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
	}

	// test task information cleanup in task status checker
	rtsc.w.subTaskHolder.removeSubTask(task1)
	time.Sleep(backoffMin)
	rtsc.check()
	c.Assert(task2LatestResumeTime.Before(rtsc.bc.latestResumeTime[task2]), check.IsTrue)
	c.Assert(len(rtsc.bc.backoffs), check.Equals, 1)
	c.Assert(len(rtsc.bc.latestPausedTime), check.Equals, 1)
	c.Assert(len(rtsc.bc.latestResumeTime), check.Equals, 1)
	c.Assert(len(rtsc.bc.latestBlockTime), check.Equals, 0)
}

func (s *testTaskCheckerSuite) TestIsResumableError(c *check.C) {
	testCases := []struct {
		err       error
		resumable bool
	}{
		// only DM new error is checked
		{&tmysql.SQLError{1105, "unsupported modify column length 20 is less than origin 40", tmysql.DefaultMySQLState}, true},
		{&tmysql.SQLError{1105, "unsupported drop integer primary key", tmysql.DefaultMySQLState}, true},
		{nil, true},
		{terror.ErrDBExecuteFailed.Generate("file test.t3.sql: execute statement failed: USE `test_abc`;: context canceled"), true},
		{terror.ErrDBExecuteFailed.Delegate(&tmysql.SQLError{1105, "unsupported modify column length 20 is less than origin 40", tmysql.DefaultMySQLState}, "alter table t modify col varchar(20)"), false},
		{terror.ErrDBExecuteFailed.Delegate(&tmysql.SQLError{1105, "unsupported drop integer primary key", tmysql.DefaultMySQLState}, "alter table t drop column id"), false},
		{terror.ErrDBExecuteFailed.Delegate(errors.New("Error 1062: Duplicate entry '5' for key 'PRIMARY'")), false},
		{terror.ErrDBExecuteFailed.Delegate(errors.New("INSERT INTO `db`.`tbl` (`c1`,`c2`) VALUES (?,?);: Error 1406: Data too long for column 'c2' at row 1")), false},
		// real error is generated by `Delegate` and multiple `Annotatef`, we use `New` to simplify it
		{terror.ErrParserParseRelayLog.New("parse relay log file bin.000018 from offset 555 in dir /home/tidb/deploy/relay_log/d2e831df-b4ec-11e9-9237-0242ac110008.000004: parse relay log file bin.000018 from offset 0 in dir /home/tidb/deploy/relay_log/d2e831df-b4ec-11e9-9237-0242ac110008.000004: parse relay log file /home/tidb/deploy/relay_log/d2e831df-b4ec-11e9-9237-0242ac110008.000004/bin.000018: binlog checksum mismatch, data may be corrupted"), false},
		{terror.ErrParserParseRelayLog.New("parse relay log file bin.000018 from offset 500 in dir /home/tidb/deploy/relay_log/d2e831df-b4ec-11e9-9237-0242ac110008.000004: parse relay log file bin.000018 from offset 0 in dir /home/tidb/deploy/relay_log/d2e831df-b4ec-11e9-9237-0242ac110008.000004: parse relay log file /home/tidb/deploy/relay_log/d2e831df-b4ec-11e9-9237-0242ac110008.000004/bin.000018: get event err EOF, need 1567488104 but got 316323"), false},
		{nil, true},
		{errors.New("unknown error"), true},
	}

	for _, tc := range testCases {
		err := unit.NewProcessError(tc.err)
		fmt.Printf("error: %v\n", err)
		c.Assert(isResumableError(err), check.Equals, tc.resumable)
	}
}
