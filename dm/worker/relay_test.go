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
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/relay"
	"github.com/pingcap/dm/relay/purger"
	"github.com/pingcap/errors"
)

type testRelay struct{}

var _ = Suite(&testRelay{})

func (t *testRelay) TestRelay(c *C) {
	originNewRelay := relay.NewRelay
	relay.NewRelay = relay.NewDummyRelay
	originNewPurger := purger.NewPurger
	purger.NewPurger = purger.NewDummyPurger
	defer func() {
		relay.NewRelay = originNewRelay
		purger.NewPurger = originNewPurger
	}()

	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), IsNil)

	dir := c.MkDir()
	cfg.RelayDir = dir
	cfg.MetaDir = dir

	relayHolder := NewRealRelayHolder(cfg)
	c.Assert(relayHolder, NotNil)

	holder, ok := relayHolder.(*RealRelayHolder)
	c.Assert(ok, IsTrue)

	t.testInit(c, holder)
	t.testStart(c, holder)
	t.testPauseAndResume(c, holder)
	t.testClose(c, holder)
	t.testStop(c, holder)
}

func (t *testRelay) testInit(c *C, holder *RealRelayHolder) {
	_, err := holder.Init(nil)
	c.Assert(err, IsNil)

	r, ok := holder.relay.(*relay.DummyRelay)
	c.Assert(ok, IsTrue)

	initErr := errors.New("init error")
	r.InjectInitError(initErr)
	defer r.InjectInitError(nil)

	_, err = holder.Init(nil)
	c.Assert(err, Equals, initErr)
}

func (t *testRelay) testStart(c *C, holder *RealRelayHolder) {
	c.Assert(holder.Stage(), Equals, pb.Stage_New)
	c.Assert(holder.closed.Get(), Equals, closedFalse)
	c.Assert(holder.Result(), IsNil)

	holder.Start()
	c.Assert(waitRelayStage(holder, pb.Stage_Running, 10), IsTrue)
	c.Assert(holder.Result(), IsNil)
	c.Assert(holder.closed.Get(), Equals, closedFalse)

	// test switch
	c.Assert(holder.SwitchMaster(context.Background(), nil), ErrorMatches, "current stage is Running.*")

	// test status
	status := holder.Status()
	c.Assert(status.Stage, Equals, pb.Stage_Running)
	c.Assert(status.Result, IsNil)

	c.Assert(holder.Error(), IsNil)

	// test update and pause -> resume
	t.testUpdate(c, holder)
	c.Assert(holder.Stage(), DeepEquals, pb.Stage_Paused)
	c.Assert(holder.closed.Get(), Equals, closedFalse)

	err := holder.Operate(context.Background(), &pb.OperateRelayRequest{Op: pb.RelayOp_ResumeRelay})
	c.Assert(err, IsNil)
	c.Assert(waitRelayStage(holder, pb.Stage_Running, 10), IsTrue)
	c.Assert(holder.Result(), IsNil)
	c.Assert(holder.closed.Get(), Equals, closedFalse)
}

func (t *testRelay) testClose(c *C, holder *RealRelayHolder) {
	r, ok := holder.relay.(*relay.DummyRelay)
	c.Assert(ok, IsTrue)
	processResult := &pb.ProcessResult{
		IsCanceled: true,
		Errors: []*pb.ProcessError{
			&pb.ProcessError{
				Msg: "process error",
			},
		},
	}
	r.InjectProcessResult(*processResult)
	defer r.InjectProcessResult(pb.ProcessResult{})

	holder.Close()
	c.Assert(waitRelayStage(holder, pb.Stage_Paused, 10), IsTrue)
	c.Assert(holder.Result(), DeepEquals, processResult)
	c.Assert(holder.closed.Get(), Equals, closedTrue)

	holder.Close()
	c.Assert(holder.Stage(), DeepEquals, pb.Stage_Paused)
	c.Assert(holder.Result(), DeepEquals, processResult)
	c.Assert(holder.closed.Get(), Equals, closedTrue)

	// todo: very strange, and can't resume
	status := holder.Status()
	c.Assert(status.Stage, Equals, pb.Stage_Stopped)
	c.Assert(status.Result, IsNil)

	errInfo := holder.Error()
	c.Assert(errInfo.Msg, Equals, "relay stopped")
}

func (t *testRelay) testPauseAndResume(c *C, holder *RealRelayHolder) {
	err := holder.Operate(context.Background(), &pb.OperateRelayRequest{Op: pb.RelayOp_PauseRelay})
	c.Assert(err, IsNil)
	c.Assert(holder.Stage(), DeepEquals, pb.Stage_Paused)
	c.Assert(holder.closed.Get(), Equals, closedFalse)

	err = holder.pauseRelay(context.Background(), &pb.OperateRelayRequest{Op: pb.RelayOp_PauseRelay})
	c.Assert(err, ErrorMatches, "current stage is Paused.*")

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
	c.Assert(err, ErrorMatches, "current stage is Running.*")

	// test status
	status = holder.Status()
	c.Assert(status.Stage, Equals, pb.Stage_Running)
	c.Assert(status.Result, IsNil)

	// invalid operation
	err = holder.Operate(context.Background(), &pb.OperateRelayRequest{Op: pb.RelayOp_InvalidRelayOp})
	c.Assert(err, ErrorMatches, ".*not supported.*")
}

func (t *testRelay) testUpdate(c *C, holder *RealRelayHolder) {
	cfg := &Config{
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

	r, ok := holder.relay.(*relay.DummyRelay)
	c.Assert(ok, IsTrue)

	err := errors.New("reload error")
	r.InjectReloadError(err)
	defer r.InjectReloadError(nil)
	c.Assert(holder.Update(context.Background(), cfg), Equals, err)
}

func (t *testRelay) testStop(c *C, holder *RealRelayHolder) {
	err := holder.Operate(context.Background(), &pb.OperateRelayRequest{Op: pb.RelayOp_StopRelay})
	c.Assert(err, IsNil)
	c.Assert(holder.Stage(), Equals, pb.Stage_Stopped)
	c.Assert(holder.closed.Get(), Equals, closedTrue)

	err = holder.Operate(context.Background(), &pb.OperateRelayRequest{Op: pb.RelayOp_StopRelay})
	c.Assert(err, ErrorMatches, ".*current stage is already stopped.*")
}

func waitRelayStage(holder *RealRelayHolder, expect pb.Stage, backoff int) bool {
	for i := 0; i < backoff; i++ {
		if holder.Stage() == expect {
			return true
		}

		time.Sleep(10 * time.Millisecond)
	}

	return false
}
