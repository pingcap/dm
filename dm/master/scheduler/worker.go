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

package scheduler

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/dm/dm/master/workerrpc"
	"github.com/pingcap/dm/pkg/ha"
	"github.com/pingcap/dm/pkg/terror"
)

// WorkerStage represents the stage of a DM-worker instance.
type WorkerStage string

// the stage of DM-worker instances.
// valid transformation:
//   - Offline -> Free, receive keep-alive.
//   - Free -> Offline, lost keep-alive.
//   - Free -> Bound, schedule source.
//   - Bound -> Offline, lost keep-live, when receive keep-alive again, it should become Free.
//   - Bound -> Free, revoke source scheduler.
// invalid transformation:
//   - Offline -> WorkerBound, must become Free first.
const (
	WorkerOffline WorkerStage = "offline" // the worker is not online yet.
	WorkerFree    WorkerStage = "free"    // the worker is online, but no upstream source assigned to it yet.
	WorkerBound   WorkerStage = "bound"   // the worker is online, and one upstream source already assigned to it.
)

var (
	nullBound ha.SourceBound
)

// Worker is an agent for a DM-worker instance.
type Worker struct {
	mu sync.RWMutex

	cli workerrpc.Client // the gRPC client proxy.

	baseInfo ha.WorkerInfo  // the base information of the DM-worker instance.
	bound    ha.SourceBound // the source bound relationship, null value if not bounded.
	stage    WorkerStage    // the current stage.
}

// NewWorker creates a new Worker instance with Offline stage.
func NewWorker(baseInfo ha.WorkerInfo) (*Worker, error) {
	cli, err := workerrpc.NewGRPCClient(baseInfo.Addr)
	if err != nil {
		return nil, err
	}

	w := &Worker{
		cli:      cli,
		baseInfo: baseInfo,
		stage:    WorkerOffline,
	}
	return w, nil
}

// Close closes the worker and release resources.
func (w *Worker) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.cli.Close()
}

// ToOffline transforms to Offline.
// both Free and Bound can transform to Offline.
func (w *Worker) ToOffline() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stage = WorkerOffline
	w.bound = nullBound
}

// ToFree transforms to Free.
// both Offline and Bound can transform to Free.
func (w *Worker) ToFree() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stage = WorkerFree
	w.bound = nullBound
}

// ToBound transforms to Bound.
// Free can transform to Bound, but Offline can't.
func (w *Worker) ToBound(bound ha.SourceBound) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stage == WorkerOffline {
		return terror.ErrSchedulerWorkerInvalidTrans.Generate(w.BaseInfo(), WorkerOffline, WorkerBound)
	}
	w.stage = WorkerBound
	w.bound = bound
	return nil
}

// BaseInfo returns the base info of the worker.
// No lock needed because baseInfo should not be modified after the instance created.
func (w *Worker) BaseInfo() ha.WorkerInfo {
	return w.baseInfo
}

// Stage returns the current stage.
func (w *Worker) Stage() WorkerStage {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.stage
}

// Bound returns the current source ID bounded to,
// returns null value if not bounded.
func (w *Worker) Bound() ha.SourceBound {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.bound
}

// SendRequest sends request to the DM-worker instance.
func (w *Worker) SendRequest(ctx context.Context, req *workerrpc.Request, d time.Duration) (*workerrpc.Response, error) {
	return w.cli.SendRequest(ctx, req, d)
}
