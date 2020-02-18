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
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/siddontang/go/sync2"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/backoff"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/retry"
	"github.com/pingcap/dm/pkg/terror"
)

//// Backoff related constants
//var (
//	DefaultCheckInterval           = 5 * time.Second
//	DefaultBackoffRollback         = 5 * time.Minute
//	DefaultBackoffMin              = 1 * time.Second
//	DefaultBackoffMax              = 5 * time.Minute
//	DefaultBackoffJitter           = true
//	DefaultBackoffFactor   float64 = 2
//)

// ResumeStrategy represents what we can do when we meet a paused task in task status checker
type ResumeStrategy int

// resume strategies, in each round of `check`, the checker will apply one of the following strategies
// to a given task based on its `state`, `result` from `SubTaskStatus` and backoff information recored
// in task status checker.
// operation of different strategies:
// ResumeIgnore:
//	1. check duration since latestPausedTime, if larger than backoff rollback, rollback backoff once
// ResumeNoSense:
//	1. update latestPausedTime
//	2. update latestBlockTime
// ResumeSkip:
//	1. update latestPausedTime
// ResumeDispatch:
//	1. update latestPausedTime
//	2. dispatch auto resume task
//	3. if step2 successes, update latestResumeTime, forward backoff
const (
	// When a task is not in paused state, or paused by manually, or we can't get enough information from worker
	// to determine whether this task is paused because of some error, we will apply ResumeIgnore strategy, and
	// do nothing with this task in this check round.
	ResumeIgnore ResumeStrategy = iota + 1
	// When checker detects a paused task can recover synchronization by resume, but its last auto resume
	// duration is less than backoff waiting time, we will apply ResumeSkip strategy, and skip auto resume
	// for this task in this check round.
	ResumeSkip
	// When checker detects a task is paused because of some un-resumable error, such as paused because of
	// executing incompatible DDL to downstream, we will apply ResumeNoSense strategy
	ResumeNoSense
	// ResumeDispatch means we will dispatch an auto resume operation in this check round for the paused task
	ResumeDispatch
)

var resumeStrategy2Str = map[ResumeStrategy]string{
	ResumeIgnore:   "ignore task",
	ResumeSkip:     "skip task resume",
	ResumeNoSense:  "resume task makes no sense",
	ResumeDispatch: "dispatch auto resume",
}

// String implements fmt.Stringer interface
func (bs ResumeStrategy) String() string {
	if s, ok := resumeStrategy2Str[bs]; ok {
		return s
	}
	return fmt.Sprintf("unsupported resume strategy: %d", bs)
}

// duration is used to hold a time.Duration field
type duration struct {
	time.Duration
}

// MarshalText hacks to satisfy the encoding.TextMarshaler interface
func (d duration) MarshalText() ([]byte, error) {
	return []byte(d.Duration.String()), nil
}

// UnmarshalText hacks to satisfy the encoding.TextUnmarshaler interface
func (d duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

// MarshalJSON hacks to satisfy the json.Marshaler interface
func (d *duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Duration string `json:"duration"`
	}{
		d.Duration.String(),
	})
}

// TaskStatusChecker is an interface that defines how we manage task status
type TaskStatusChecker interface {
	// Init initializes the checker
	Init() error
	// Start starts the checker
	Start()
	// Close closes the checker
	Close()
}

// NewTaskStatusChecker is a TaskStatusChecker initializer
var NewTaskStatusChecker = NewRealTaskStatusChecker

type backoffController struct {
	// task name -> backoff counter
	backoffs map[string]*backoff.Backoff

	// task name -> task latest paused time that checker observes
	latestPausedTime map[string]time.Time

	// task name -> task latest block time, block means task paused with un-resumable error
	latestBlockTime map[string]time.Time

	// task name -> the latest auto resume time
	latestResumeTime map[string]time.Time
}

// newBackoffController returns a new backoffController instance
func newBackoffController() *backoffController {
	return &backoffController{
		backoffs:         make(map[string]*backoff.Backoff),
		latestPausedTime: make(map[string]time.Time),
		latestBlockTime:  make(map[string]time.Time),
		latestResumeTime: make(map[string]time.Time),
	}
}

// realTaskStatusChecker is not thread-safe.
// It runs asynchronously against DM-worker, and task information may be updated
// later than DM-worker, but it is acceptable.
type realTaskStatusChecker struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	closed sync2.AtomicInt32

	cfg config.CheckerConfig
	l   log.Logger
	w   *Worker
	bc  *backoffController
}

// NewRealTaskStatusChecker creates a new realTaskStatusChecker instance
func NewRealTaskStatusChecker(cfg config.CheckerConfig, w *Worker) TaskStatusChecker {
	tsc := &realTaskStatusChecker{
		cfg: cfg,
		l:   log.With(zap.String("component", "task checker")),
		w:   w,
		bc:  newBackoffController(),
	}
	tsc.closed.Set(closedTrue)
	return tsc
}

// Init implements TaskStatusChecker.Init
func (tsc *realTaskStatusChecker) Init() error {
	// just check configuration of backoff here, lazy creates backoff counter,
	// as we can't get task information before dm-worker starts
	_, err := backoff.NewBackoff(tsc.cfg.BackoffFactor, tsc.cfg.BackoffJitter, tsc.cfg.BackoffMin.Duration, tsc.cfg.BackoffMax.Duration)
	return terror.WithClass(err, terror.ClassDMWorker)
}

// Start implements TaskStatusChecker.Start
func (tsc *realTaskStatusChecker) Start() {
	tsc.wg.Add(1)
	go func() {
		defer tsc.wg.Done()
		tsc.run()
	}()
}

// Close implements TaskStatusChecker.Close
func (tsc *realTaskStatusChecker) Close() {
	if !tsc.closed.CompareAndSwap(closedFalse, closedTrue) {
		return
	}

	if tsc.cancel != nil {
		tsc.cancel()
	}
	tsc.wg.Wait()
}

func (tsc *realTaskStatusChecker) run() {
	tsc.ctx, tsc.cancel = context.WithCancel(context.Background())
	tsc.closed.Set(closedFalse)

	failpoint.Inject("TaskCheckInterval", func(val failpoint.Value) {
		interval, err := time.ParseDuration(val.(string))
		if err != nil {
			tsc.l.Warn("inject failpoint TaskCheckInterval failed", zap.Reflect("value", val), zap.Error(err))
		} else {
			tsc.cfg.CheckInterval = config.Duration{Duration: interval}
			tsc.l.Info("set TaskCheckInterval", zap.String("failpoint", "TaskCheckInterval"), zap.Duration("value", interval))
		}
	})

	ticker := time.NewTicker(tsc.cfg.CheckInterval.Duration)
	defer ticker.Stop()
	for {
		select {
		case <-tsc.ctx.Done():
			tsc.l.Info("worker task checker exits")
			return
		case <-ticker.C:
			tsc.check()
		}
	}
}

// isResumableError checks the error message and returns whether we need to
// resume the task and retry
func isResumableError(err *pb.ProcessError) bool {
	switch err.Type {
	case pb.ErrorType_ExecSQL:
		// not elegant code, because TiDB doesn't expose some error
		for _, msg := range retry.UnsupportedDDLMsgs {
			if err.Error != nil && strings.Contains(err.Error.RawCause, msg) {
				return false
			}
		}
		for _, msg := range retry.UnsupportedDMLMsgs {
			if err.Error != nil && strings.Contains(err.Error.RawCause, msg) {
				return false
			}
		}
	case pb.ErrorType_UnknownError:
		if err.Error != nil && err.Error.ErrCode == int32(terror.ErrParserParseRelayLog.Code()) {
			for _, msg := range retry.ParseRelayLogErrMsgs {
				if strings.Contains(err.Error.Message, msg) {
					return false
				}
			}
		}
	}

	return true
}

func (tsc *realTaskStatusChecker) getResumeStrategy(stStatus *pb.SubTaskStatus, duration time.Duration) ResumeStrategy {
	// task that is not paused or paused manually, just ignore it
	if stStatus == nil || stStatus.Stage != pb.Stage_Paused || stStatus.Result == nil || stStatus.Result.IsCanceled {
		return ResumeIgnore
	}

	// TODO: use different strategies based on the error detail
	for _, processErr := range stStatus.Result.Errors {
		if !isResumableError(processErr) {
			return ResumeNoSense
		}
	}

	// auto resume interval does not exceed backoff duration, skip this paused task
	if time.Since(tsc.bc.latestResumeTime[stStatus.Name]) < duration {
		return ResumeSkip
	}

	return ResumeDispatch
}

func (tsc *realTaskStatusChecker) check() {
	allSubTaskStatus := tsc.w.getAllSubTaskStatus()

	defer func() {
		// cleanup outdated tasks
		for taskName := range tsc.bc.backoffs {
			_, ok := allSubTaskStatus[taskName]
			if !ok {
				tsc.l.Debug("remove task from checker", zap.String("task", taskName))
				delete(tsc.bc.backoffs, taskName)
				delete(tsc.bc.latestPausedTime, taskName)
				delete(tsc.bc.latestBlockTime, taskName)
				delete(tsc.bc.latestResumeTime, taskName)
			}
		}
	}()

	for taskName, stStatus := range allSubTaskStatus {
		bf, ok := tsc.bc.backoffs[taskName]
		if !ok {
			bf, _ = backoff.NewBackoff(tsc.cfg.BackoffFactor, tsc.cfg.BackoffJitter, tsc.cfg.BackoffMin.Duration, tsc.cfg.BackoffMax.Duration)
			tsc.bc.backoffs[taskName] = bf
			tsc.bc.latestPausedTime[taskName] = time.Now()
			tsc.bc.latestResumeTime[taskName] = time.Now()
		}
		duration := bf.Current()
		strategy := tsc.getResumeStrategy(stStatus, duration)
		switch strategy {
		case ResumeIgnore:
			if time.Since(tsc.bc.latestPausedTime[taskName]) > tsc.cfg.BackoffRollback.Duration {
				bf.Rollback()
				// after each rollback, reset this timer
				tsc.bc.latestPausedTime[taskName] = time.Now()
			}
		case ResumeNoSense:
			// this strategy doesn't forward or rollback backoff
			tsc.bc.latestPausedTime[taskName] = time.Now()
			blockTime, ok := tsc.bc.latestBlockTime[taskName]
			if ok {
				tsc.l.Warn("task can't auto resume", zap.String("task", taskName), zap.Duration("paused duration", time.Since(blockTime)))
			} else {
				tsc.bc.latestBlockTime[taskName] = time.Now()
				tsc.l.Warn("task can't auto resume", zap.String("task", taskName))
			}
		case ResumeSkip:
			tsc.l.Warn("backoff skip auto resume task", zap.String("task", taskName), zap.Time("latestResumeTime", tsc.bc.latestResumeTime[taskName]), zap.Duration("duration", duration))
			tsc.bc.latestPausedTime[taskName] = time.Now()
		case ResumeDispatch:
			tsc.bc.latestPausedTime[taskName] = time.Now()
			err := tsc.w.OperateSubTask(taskName, pb.TaskOp_AutoResume)
			if err != nil {
				tsc.l.Error("dispatch auto resume task failed", zap.String("task", taskName), zap.Error(err))
			} else {
				tsc.l.Info("dispatch auto resume task", zap.String("task", taskName))
				tsc.bc.latestResumeTime[taskName] = time.Now()
				bf.BoundaryForward()
			}
		}
	}
}
