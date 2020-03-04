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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/unit"
	pkgstreamer "github.com/pingcap/dm/pkg/streamer"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/pingcap/dm/relay"
	"github.com/pingcap/dm/relay/purger"
)

type testRelay struct{}

var _ = Suite(&testRelay{})

/*********** dummy relay log process unit, used only for testing *************/

// DummyRelay is a dummy relay
type DummyRelay struct {
	initErr error

	processResult pb.ProcessResult
	errorInfo     *pb.RelayError
	reloadErr     error
}

// NewDummyRelay creates an instance of dummy Relay.
func NewDummyRelay(cfg *relay.Config) relay.Process {
	return &DummyRelay{}
}

// Init implements Process interface
func (d *DummyRelay) Init(ctx context.Context) error {
	return d.initErr
}

// InjectInitError injects init error
func (d *DummyRelay) InjectInitError(err error) {
	d.initErr = err
}

// Process implements Process interface
func (d *DummyRelay) Process(ctx context.Context, pr chan pb.ProcessResult) {
	<-ctx.Done()
	pr <- d.processResult
}

// InjectProcessResult injects process result
func (d *DummyRelay) InjectProcessResult(result pb.ProcessResult) {
	d.processResult = result
}

// SwitchMaster implements Process interface
func (d *DummyRelay) SwitchMaster(ctx context.Context, req *pb.SwitchRelayMasterRequest) error {
	return nil
}

// Migrate implements Process interface
func (d *DummyRelay) Migrate(ctx context.Context, binlogName string, binlogPos uint32) error {
	return nil
}

// ActiveRelayLog implements Process interface
func (d *DummyRelay) ActiveRelayLog() *pkgstreamer.RelayLogInfo {
	return nil
}

// Reload implements Process interface
func (d *DummyRelay) Reload(newCfg *relay.Config) error {
	return d.reloadErr
}

// InjectReloadError injects reload error
func (d *DummyRelay) InjectReloadError(err error) {
	d.reloadErr = err
}

// Update implements Process interface
func (d *DummyRelay) Update(cfg *config.SubTaskConfig) error {
	return nil
}

// Resume implements Process interface
func (d *DummyRelay) Resume(ctx context.Context, pr chan pb.ProcessResult) {}

// Pause implements Process interface
func (d *DummyRelay) Pause() {}

// Error implements Process interface
func (d *DummyRelay) Error() interface{} {
	return d.errorInfo
}

// Status implements Process interface
func (d *DummyRelay) Status() interface{} {
	return &pb.RelayStatus{
		Stage: pb.Stage_New,
	}
}

// Close implements Process interface
func (d *DummyRelay) Close() {}

// IsClosed implements Process interface
func (d *DummyRelay) IsClosed() bool { return false }

func (t *testRelay) TestRelay(c *C) {
	originNewRelay := relay.NewRelay
	relay.NewRelay = NewDummyRelay
	originNewPurger := purger.NewPurger
	purger.NewPurger = purger.NewDummyPurger
	defer func() {
		relay.NewRelay = originNewRelay
		purger.NewPurger = originNewPurger
	}()

	cfg := loadSourceConfigWithoutPassword(c)

	dir := c.MkDir()
	cfg.RelayDir = dir
	cfg.MetaDir = dir

	relayHolder := NewRealRelayHolder(&cfg)
	c.Assert(relayHolder, NotNil)

	holder, ok := relayHolder.(*realRelayHolder)
	c.Assert(ok, IsTrue)

	t.testInit(c, holder)
	t.testStart(c, holder)
	t.testPauseAndResume(c, holder)
	t.testClose(c, holder)
	t.testStop(c, holder)
}

func (t *testRelay) testInit(c *C, holder *realRelayHolder) {
	_, err := holder.Init(nil)
	c.Assert(err, IsNil)

	r, ok := holder.relay.(*DummyRelay)
	c.Assert(ok, IsTrue)

	initErr := errors.New("init error")
	r.InjectInitError(initErr)
	defer r.InjectInitError(nil)

	_, err = holder.Init(nil)
	c.Assert(err, ErrorMatches, ".*"+initErr.Error()+".*")
}

func (t *testRelay) testStart(c *C, holder *realRelayHolder) {
	c.Assert(holder.Stage(), Equals, pb.Stage_New)
	c.Assert(holder.closed.Get(), Equals, closedFalse)
	c.Assert(holder.Result(), IsNil)

	holder.Start()
	c.Assert(waitRelayStage(holder, pb.Stage_Running, 10), IsTrue)
	c.Assert(holder.Result(), IsNil)
	c.Assert(holder.closed.Get(), Equals, closedFalse)

	// test switch
	c.Assert(holder.SwitchMaster(context.Background(), nil), ErrorMatches, ".*current stage is Running.*")

	// test status
	status := holder.Status()
	c.Assert(status.Stage, Equals, pb.Stage_Running)
	c.Assert(status.Result, IsNil)

	c.Assert(holder.Error(), IsNil)

	// test update and pause -> resume
	t.testUpdate(c, holder)
	c.Assert(holder.Stage(), Equals, pb.Stage_Paused)
	c.Assert(holder.closed.Get(), Equals, closedFalse)

	err := holder.Operate(context.Background(), &pb.OperateRelayRequest{Op: pb.RelayOp_ResumeRelay})
	c.Assert(err, IsNil)
	c.Assert(waitRelayStage(holder, pb.Stage_Running, 10), IsTrue)
	c.Assert(holder.Result(), IsNil)
	c.Assert(holder.closed.Get(), Equals, closedFalse)
}

func (t *testRelay) testClose(c *C, holder *realRelayHolder) {
	r, ok := holder.relay.(*DummyRelay)
	c.Assert(ok, IsTrue)
	processResult := &pb.ProcessResult{
		IsCanceled: true,
		Errors: []*pb.ProcessError{
			unit.NewProcessError(errors.New("process error")),
		},
	}
	r.InjectProcessResult(*processResult)
	defer r.InjectProcessResult(pb.ProcessResult{})

	holder.Close()
	c.Assert(waitRelayStage(holder, pb.Stage_Paused, 10), IsTrue)
	c.Assert(holder.Result(), DeepEquals, processResult)
	c.Assert(holder.closed.Get(), Equals, closedTrue)

	holder.Close()
	c.Assert(holder.Stage(), Equals, pb.Stage_Paused)
	c.Assert(holder.Result(), DeepEquals, processResult)
	c.Assert(holder.closed.Get(), Equals, closedTrue)

	// todo: very strange, and can't resume
	status := holder.Status()
	c.Assert(status.Stage, Equals, pb.Stage_Stopped)
	c.Assert(status.Result, IsNil)

	errInfo := holder.Error()
	c.Assert(errInfo.Msg, Equals, "relay stopped")
}

func (t *testRelay) testPauseAndResume(c *C, holder *realRelayHolder) {
	err := holder.Operate(context.Background(), &pb.OperateRelayRequest{Op: pb.RelayOp_PauseRelay})
	c.Assert(err, IsNil)
	c.Assert(holder.Stage(), Equals, pb.Stage_Paused)
	c.Assert(holder.closed.Get(), Equals, closedFalse)

	err = holder.pauseRelay(context.Background(), &pb.OperateRelayRequest{Op: pb.RelayOp_PauseRelay})
	c.Assert(err, ErrorMatches, ".*current stage is Paused.*")

	// test status
	status := holder.Status()
	c.Assert(status.Stage, Equals, pb.Stage_Paused)

	// test switch
	c.Assert(holder.SwitchMaster(context.Background(), nil), IsNil)

	// test update
	t.testUpdate(c, holder)

	err = holder.Operate(context.Background(), &pb.OperateRelayRequest{Op: pb.RelayOp_ResumeRelay})
	c.Assert(err, IsNil)
	c.Assert(waitRelayStage(holder, pb.Stage_Running, 10), IsTrue)
	c.Assert(holder.Result(), IsNil)
	c.Assert(holder.closed.Get(), Equals, closedFalse)

	err = holder.Operate(context.Background(), &pb.OperateRelayRequest{Op: pb.RelayOp_ResumeRelay})
	c.Assert(err, ErrorMatches, ".*current stage is Running.*")

	// test status
	status = holder.Status()
	c.Assert(status.Stage, Equals, pb.Stage_Running)
	c.Assert(status.Result, IsNil)

	// invalid operation
	err = holder.Operate(context.Background(), &pb.OperateRelayRequest{Op: pb.RelayOp_InvalidRelayOp})
	c.Assert(err, ErrorMatches, ".*not supported.*")
}

func (t *testRelay) testUpdate(c *C, holder *realRelayHolder) {
	cfg := &config.SourceConfig{
		From: config.DBConfig{
			Host:     "127.0.0.1",
			Port:     3306,
			User:     "root",
			Password: "1234",
		},
	}

	originStage := holder.Stage()
	c.Assert(holder.Update(context.Background(), cfg), IsNil)
	c.Assert(waitRelayStage(holder, originStage, 10), IsTrue)
	c.Assert(holder.closed.Get(), Equals, closedFalse)

	r, ok := holder.relay.(*DummyRelay)
	c.Assert(ok, IsTrue)

	err := errors.New("reload error")
	r.InjectReloadError(err)
	defer r.InjectReloadError(nil)
	c.Assert(holder.Update(context.Background(), cfg), Equals, err)
}

func (t *testRelay) testStop(c *C, holder *realRelayHolder) {
	err := holder.Operate(context.Background(), &pb.OperateRelayRequest{Op: pb.RelayOp_StopRelay})
	c.Assert(err, IsNil)
	c.Assert(holder.Stage(), Equals, pb.Stage_Stopped)
	c.Assert(holder.closed.Get(), Equals, closedTrue)

	err = holder.Operate(context.Background(), &pb.OperateRelayRequest{Op: pb.RelayOp_StopRelay})
	c.Assert(err, ErrorMatches, ".*current stage is already stopped.*")
}

func waitRelayStage(holder *realRelayHolder, expect pb.Stage, backoff int) bool {
	return utils.WaitSomething(backoff, 10*time.Millisecond, func() bool {
		return holder.Stage() == expect
	})
}
