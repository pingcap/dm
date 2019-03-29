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
	"sync"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/errors"
	"github.com/siddontang/go/sync2"

	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/streamer"
	"github.com/pingcap/dm/relay"
)

// RelayHolder used to hold the relay unit
type RelayHolder struct {
	sync.RWMutex
	wg sync.WaitGroup

	relay *relay.Relay

	ctx    context.Context
	cancel context.CancelFunc

	closed sync2.AtomicInt32
	stage  pb.Stage
	result *pb.ProcessResult // the process result, nil when is processing
}

// NewRelayHolder creates a new RelayHolder
func NewRelayHolder(cfg *Config) *RelayHolder {
	clone, _ := cfg.DecryptPassword()
	relayCfg := &relay.Config{
		EnableGTID:  clone.EnableGTID,
		AutoFixGTID: clone.AutoFixGTID,
		Flavor:      clone.Flavor,
		RelayDir:    clone.RelayDir,
		ServerID:    clone.ServerID,
		Charset:     clone.Charset,
		From: relay.DBConfig{
			Host:     clone.From.Host,
			Port:     clone.From.Port,
			User:     clone.From.User,
			Password: clone.From.Password,
		},
		BinLogName: clone.RelayBinLogName,
		BinlogGTID: clone.RelayBinlogGTID,
	}

	h := &RelayHolder{
		stage: pb.Stage_New,
		relay: relay.NewRelay(relayCfg),
	}
	h.closed.Set(closedTrue)
	return h
}

// Init initializes the holder
func (h *RelayHolder) Init() error {
	h.closed.Set(closedFalse)
	return errors.Trace(h.relay.Init())
}

// Start starts run the relay
func (h *RelayHolder) Start() {
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		h.run()
	}()
}

// Close closes the holder
func (h *RelayHolder) Close() {
	if !h.closed.CompareAndSwap(closedFalse, closedTrue) {
		return
	}

	if h.cancel != nil {
		h.cancel()
	}
	h.wg.Wait() // wait process return

	h.relay.Close()
}

func (h *RelayHolder) run() {
	h.setStage(pb.Stage_Running)
	h.setResult(nil) // clear previous result
	h.ctx, h.cancel = context.WithCancel(context.Background())
	pr := make(chan pb.ProcessResult, 1)

	h.relay.Process(h.ctx, pr)

	for len(pr) > 0 {
		r := <-pr
		h.setResult(&r)
		for _, err := range r.Errors {
			log.Errorf("process error with type %v:\n %v", err.Type, err.Msg)
		}
	}

	h.setStageIfNot(pb.Stage_Stopped, pb.Stage_Paused)
}

// Status returns relay unit's status
func (h *RelayHolder) Status() *pb.RelayStatus {
	if h.closed.Get() == closedTrue || h.relay.IsClosed() {
		return &pb.RelayStatus{
			Stage: pb.Stage_Stopped,
		}
	}

	s := h.relay.Status().(*pb.RelayStatus)
	s.Stage = h.Stage()
	s.Result = h.Result()

	return s
}

// Error returns relay unit's status
func (h *RelayHolder) Error() *pb.RelayError {
	if h.closed.Get() == closedTrue || h.relay.IsClosed() {
		return &pb.RelayError{
			Msg: "relay stopped",
		}
	}

	s := h.relay.Error().(*pb.RelayError)
	return s
}

// SwitchMaster requests relay unit to switch master server
func (h *RelayHolder) SwitchMaster(ctx context.Context, req *pb.SwitchRelayMasterRequest) error {
	h.RLock()
	defer h.RUnlock()
	if h.stage != pb.Stage_Paused {
		return errors.Errorf("current stage is %s, Paused required", h.stage.String())
	}
	return errors.Trace(h.relay.SwitchMaster(ctx, req))
}

// Operate operates relay unit
func (h *RelayHolder) Operate(ctx context.Context, req *pb.OperateRelayRequest) error {
	switch req.Op {
	case pb.RelayOp_PauseRelay:
		return h.pauseRelay(ctx, req)
	case pb.RelayOp_ResumeRelay:
		return h.resumeRelay(ctx, req)
	case pb.RelayOp_StopRelay:
		return h.stopRelay(ctx, req)
	}
	return errors.NotSupportedf("operation %s", req.Op.String())
}

func (h *RelayHolder) pauseRelay(ctx context.Context, req *pb.OperateRelayRequest) error {
	h.Lock()
	if h.stage != pb.Stage_Running {
		h.Unlock()
		return errors.Errorf("current stage is %s, Running required", h.stage.String())
	}
	h.stage = pb.Stage_Paused

	if h.cancel != nil {
		h.cancel()
	}
	h.Unlock()  // unlock to make `run` can return
	h.wg.Wait() // wait process return

	h.relay.Pause()

	return nil
}

func (h *RelayHolder) resumeRelay(ctx context.Context, req *pb.OperateRelayRequest) error {
	h.Lock()
	defer h.Unlock()
	if h.stage != pb.Stage_Paused {
		return errors.Errorf("current stage is %s, Paused required", h.stage.String())
	}
	h.stage = pb.Stage_Running

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		h.run()
	}()
	return nil
}

func (h *RelayHolder) stopRelay(ctx context.Context, req *pb.OperateRelayRequest) error {
	h.Lock()
	defer h.Unlock()
	if h.stage == pb.Stage_Stopped {
		return errors.NotValidf("current stage is already stopped")
	}
	h.stage = pb.Stage_Stopped

	// now, when try to stop relay unit, we close relay holder
	h.Close()
	return nil
}

// Stage returns the stage of the relay
func (h *RelayHolder) Stage() pb.Stage {
	h.RLock()
	defer h.RUnlock()
	return h.stage
}

func (h *RelayHolder) setStage(stage pb.Stage) {
	h.Lock()
	defer h.Unlock()
	h.stage = stage
}

// setStageIfNot sets stage to newStage if its current value is not oldStage, similar to CAS
func (h *RelayHolder) setStageIfNot(oldStage, newStage pb.Stage) bool {
	h.Lock()
	defer h.Unlock()
	if h.stage != oldStage {
		h.stage = newStage
		return true
	}
	return false
}

func (h *RelayHolder) setResult(result *pb.ProcessResult) {
	h.Lock()
	defer h.Unlock()
	if result == nil {
		h.result = nil
	} else {
		clone := *result
		h.result = &clone
	}
}

// Result returns the result of the relay
func (h *RelayHolder) Result() *pb.ProcessResult {
	h.RLock()
	defer h.RUnlock()
	if h.result == nil {
		return nil
	}
	clone := *h.result
	return &clone
}

// Update update relay config online
func (h *RelayHolder) Update(ctx context.Context, cfg *Config) error {
	relayCfg := &relay.Config{
		AutoFixGTID: cfg.AutoFixGTID,
		Charset:     cfg.Charset,
		From: relay.DBConfig{
			Host:     cfg.From.Host,
			Port:     cfg.From.Port,
			User:     cfg.From.User,
			Password: cfg.From.Password,
		},
	}

	stage := h.Stage()

	if stage == pb.Stage_Paused {
		err := h.relay.Reload(relayCfg)
		if err != nil {
			return errors.Trace(err)
		}
	} else if stage == pb.Stage_Running {
		err := h.Operate(ctx, &pb.OperateRelayRequest{Op: pb.RelayOp_PauseRelay})
		if err != nil {
			return errors.Trace(err)
		}

		err = h.relay.Reload(relayCfg)
		if err != nil {
			return errors.Trace(err)
		}

		err = h.Operate(ctx, &pb.OperateRelayRequest{Op: pb.RelayOp_ResumeRelay})
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// EarliestActiveRelayLog implements RelayOperator.EarliestActiveRelayLog
func (h *RelayHolder) EarliestActiveRelayLog() *streamer.RelayLogInfo {
	return h.relay.ActiveRelayLog()
}

// Migrate reset binlog name and binlog pos for relay unit
func (h *RelayHolder) Migrate(ctx context.Context, binlogName string, binlogPos uint32) error {
	h.Lock()
	defer h.Unlock()
	return h.relay.Migrate(ctx, binlogName, binlogPos)
}
