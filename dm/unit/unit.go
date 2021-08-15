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

package unit

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/terror"
)

const (
	// DefaultInitTimeout represents the default timeout value when initializing a process unit.
	DefaultInitTimeout = time.Minute
)

// Unit defines interface for subtask process units, like syncer, loader, relay, etc.
type Unit interface {
	// Init initializes the dm process unit.
	// Init should be the first method to be called of this interface, and all unit is called Init before start running
	// the subtask.
	// It's recommended to let this method detect early stage errors, while only put lightweight initialization here to
	// reduce blocking time.
	// if initialing successfully, the outer caller must call Close to release the resources.
	// if initialing fail, Init itself should release resources which acquired before failure happens.
	Init(ctx context.Context) error
	// Start does some initialization work and then runs the unit. The result is sent to given channel.
	Start(pr chan pb.ProcessResult)
	// Close shuts down the process and closes the unit.
	Close()

	// Pause pauses the process, some status can be kept to support Resume later.
	Pause()
	// Resume runs the unit assuming the status of unit is initialized. The result is sent to given channel.
	Resume(pr chan pb.ProcessResult)

	// Update updates the configuration
	Update(cfg *config.SubTaskConfig) error

	// Status returns the unit's current status
	Status() interface{}
	// Type returns the unit's type
	Type() pb.UnitType
	// IsFreshTask return whether is a fresh task (not processed before)
	// it will be used to decide where the task should become restoring
	IsFreshTask(ctx context.Context) (bool, error)
}

// Base shares some common member of each implementation of Unit interface, such as context management, status, etc.
type Base struct {
	// unit should only rely on UnitCancel (instead of closing channel, etcd) to cancel its processing
	// UnitCtx and UnitCancel is assigned when enter Processing status, and UnitCancel is called when leave Processing.
	UnitCtx    context.Context
	UnitCancel context.CancelFunc
	Status     StatusType
	StatusLock sync.Mutex
	Processing sync.WaitGroup
}

// StatusType is used by Base.Status.
type StatusType int64

// Available status transition.
// NotStarted -> { NotStarted, Processing, Paused, Closed }
// Processing -> { Paused, Closed }
// Paused -> { Processing, Closed }
// Closed -> {}.
const (
	NotStarted StatusType = iota
	Processing
	Paused
	Closed
)

// String implements Stringer.
func (t StatusType) String() string {
	switch t {
	case NotStarted:
		return "not started"
	case Processing:
		return "processing"
	case Paused:
		return "paused"
	case Closed:
		return "closed"
	}
	return "invalid status"
}

// ToProcessing tries to convert current unit to Processing status and set a proper context and wait group.
// Return true when conversion happened and false otherwise. Return old status as well.
// b.Processing.Done must be called when processing ends for true ok.
func (b *Base) ToProcessing() (ok bool, oldStatus StatusType) {
	b.StatusLock.Lock()
	defer b.StatusLock.Unlock()

	oldStatus = b.Status
	if b.Status == NotStarted || b.Status == Paused {
		b.Status = Processing
		b.UnitCtx, b.UnitCancel = context.WithCancel(context.Background())
		b.Processing.Add(1)
		ok = true
	}
	return
}

// ToClosed tries to convert current unit to Closed status.
// Return true when conversion happened and false otherwise. The closing action should be called after return true.
func (b *Base) ToClosed() (ok bool) {
	b.StatusLock.Lock()
	defer b.StatusLock.Unlock()

	switch b.Status {
	case Closed:
		return false
	case Processing:
		b.UnitCancel()
		b.Processing.Wait()
	}
	b.Status = Closed
	return true
}

// ToPaused tries to convert current unit to Paused status.
// Return true when conversion happened and false otherwise. Return old status as well.
// The pausing action should be called after old status is Processing.
func (b *Base) ToPaused() (ok bool, oldStatus StatusType) {
	b.StatusLock.Lock()
	defer b.StatusLock.Unlock()

	oldStatus = b.Status
	switch b.Status {
	case NotStarted:
		b.Status = Paused
		ok = true
	case Processing:
		b.UnitCancel()
		b.Processing.Wait()
		b.Status = Paused
		ok = true
	case Paused:
	case Closed:
	}
	return
}

// NewProcessError creates a new ProcessError
// we can refine to add error scope field if needed.
func NewProcessError(err error) *pb.ProcessError {
	if e, ok := err.(*terror.Error); ok {
		return &pb.ProcessError{
			ErrCode:    int32(e.Code()),
			ErrClass:   e.Class().String(),
			ErrScope:   e.Scope().String(),
			ErrLevel:   e.Level().String(),
			Message:    terror.Message(e),
			RawCause:   terror.Message(e.Cause()),
			Workaround: e.Workaround(),
		}
	}

	return &pb.ProcessError{
		ErrCode:    int32(terror.ErrNotSet.Code()),
		ErrClass:   terror.ErrNotSet.Class().String(),
		ErrScope:   terror.ErrNotSet.Scope().String(),
		ErrLevel:   terror.ErrNotSet.Level().String(),
		Message:    terror.Message(err),
		RawCause:   terror.Message(terror.ErrNotSet.Cause()),
		Workaround: terror.ErrNotSet.Workaround(),
	}
}

// IsCtxCanceledProcessErr returns true if the err's context canceled.
func IsCtxCanceledProcessErr(err *pb.ProcessError) bool {
	return strings.Contains(err.Message, "context canceled")
}

// JoinProcessErrors return the string of pb.ProcessErrors joined by ", ".
func JoinProcessErrors(errors []*pb.ProcessError) string {
	serrs := make([]string, 0, len(errors))
	for _, serr := range errors {
		serrs = append(serrs, serr.String())
	}
	return strings.Join(serrs, ", ")
}
