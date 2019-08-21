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
	"sync"
	"time"

	"github.com/siddontang/go/sync2"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/backoff"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
)

// Backoff related constants
var (
	DefaultCheckInterval           = 5 * time.Second
	DefaultBackoffRollback         = 5 * time.Minute
	DefaultBackoffMin              = 1 * time.Second
	DefaultBackoffMax              = 5 * time.Minute
	DefaultBackoffJitter           = true
	DefaultBackoffFactor   float64 = 2
)

// ResumeStrategy represents what we can do when we meet a paused task in task status checker
type ResumeStrategy int

// resume strategies
const (
	ResumeIgnore ResumeStrategy = iota + 1
	ResumeSkip
	ResumeNoSense
	ResumeDispatch
)

var backoffStrategy2Str = map[ResumeStrategy]string{
	ResumeIgnore:   "ignore task",
	ResumeSkip:     "skip task resume",
	ResumeNoSense:  "resume task makes no sense",
	ResumeDispatch: "dispatch auto resume",
}

// String implements fmt.Stringer interface
func (bs ResumeStrategy) String() string {
	if s, ok := backoffStrategy2Str[bs]; ok {
		return s
	}
	return fmt.Sprintf("unknown backoff strategy: %d", bs)
}

// CheckerConfig is configuration used for TaskStatusChecker
type CheckerConfig struct {
	CheckInterval   time.Duration `toml:"check-interval" json:"check-interval"`
	BackoffRollback time.Duration `toml:"backoff-rollback" json:"backoff-rollback"`
	BackoffMin      time.Duration `toml:"backoff-min" json:"backoff-min"`
	BackoffMax      time.Duration `toml:"backoff-max" json:"backoff-max"`
	BackoffJitter   bool          `toml:"backoff-jitter" json:"backoff-jitter"`
	BackoffFactor   float64       `toml:"backoff-factor" json:"backoff-factor"`
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

type realTaskStatusChecker struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	closed sync2.AtomicInt32

	cfg CheckerConfig
	l   log.Logger
	w   *Worker
	bf  *backoff.Backoff

	// normalTime is used to record the time interval that no abnormal paused task
	normalTime time.Time
	// lastestResume is used to record the lastest auto resume time
	lastestResume time.Time
}

// NewRealTaskStatusChecker creates a new realTaskStatusChecker instance
func NewRealTaskStatusChecker(cfg CheckerConfig, w *Worker) TaskStatusChecker {
	tsc := &realTaskStatusChecker{
		cfg: cfg,
		l:   log.With(zap.String("component", "task checker")),
		w:   w,
	}
	tsc.closed.Set(closedTrue)
	return tsc
}

// Init implements TaskStatusChecker.Init
func (tsc *realTaskStatusChecker) Init() error {
	var err error
	tsc.bf, err = backoff.NewBackoff(tsc.cfg.BackoffFactor, tsc.cfg.BackoffJitter, tsc.cfg.BackoffMin, tsc.cfg.BackoffMax)
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
	tsc.normalTime = time.Now()
	tsc.lastestResume = time.Now().Add(-tsc.cfg.BackoffMin)
	tsc.closed.Set(closedFalse)
	ticker := time.NewTicker(tsc.cfg.CheckInterval)
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

func (tsc *realTaskStatusChecker) getResumeStrategy(taskStatus *pb.TaskStatus, duration time.Duration) ResumeStrategy {
	// task that is not paused or paused manually, just ignore it
	if taskStatus == nil || taskStatus.Stage != pb.Stage_Paused || taskStatus.Result == nil || taskStatus.Result.IsCanceled {
		return ResumeIgnore
	}
	// TODO: use different strategies based on the error detail
	// for _, processErr := range taskStatus.Result.Errors {
	// }
	// auto resume interval does not exceed backoff duration, skip this paused task
	if time.Since(tsc.lastestResume) < duration {
		return ResumeSkip
	}
	return ResumeDispatch
}

func (tsc *realTaskStatusChecker) check() {
	tasks := tsc.w.getPausedSubTasks()
	abnormal := 0
	resumed := 0
	duration := tsc.bf.Current()
	for _, task := range tasks {
		strategy := tsc.getResumeStrategy(task, duration)
		if strategy == ResumeIgnore {
			continue
		}
		abnormal++
		if strategy == ResumeNoSense {
			tsc.l.Warn("no help to resume task", zap.String("task", task.Name))
			continue
		}
		if strategy == ResumeSkip {
			tsc.l.Warn("skip auto resume task", zap.String("task", task.Name), zap.Time("lastestResume", tsc.lastestResume), zap.Reflect("duration", duration))
			continue
		}
		opLogID, err := tsc.w.operateSubTask(&pb.TaskMeta{
			Name: task.Name,
			Op:   pb.TaskOp_AutoResume,
		})
		if err != nil {
			tsc.l.Error("dispatch auto resume task", zap.String("task", task.Name), zap.Error(err))
		} else {
			resumed++
			tsc.lastestResume = time.Now()
			tsc.l.Info("dispatch auto resume task", zap.String("task", task.Name), zap.Int64("opLogID", opLogID))
		}
	}
	if abnormal > 0 {
		tsc.normalTime = time.Now()
		if resumed > 0 {
			tsc.bf.Forward()
		}
	} else {
		if time.Since(tsc.normalTime) > tsc.cfg.BackoffRollback {
			tsc.bf.Rollback()
			tsc.normalTime = time.Now()
		}
	}
}
