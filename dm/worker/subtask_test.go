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
	"strings"
	"time"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/unit"
	"github.com/pingcap/dm/loader"
	"github.com/pingcap/dm/mydumper"
	"github.com/pingcap/dm/syncer"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"go.etcd.io/etcd/clientv3"
)

type testSubTask struct{}

var _ = Suite(&testSubTask{})

func (t *testSubTask) TestCreateUnits(c *C) {
	cfg := &config.SubTaskConfig{
		Mode: "xxx",
	}
	c.Assert(createUnits(cfg, nil), HasLen, 0)

	cfg.Mode = config.ModeFull
	unitsFull := createUnits(cfg, nil)
	c.Assert(unitsFull, HasLen, 2)
	_, ok := unitsFull[0].(*mydumper.Mydumper)
	c.Assert(ok, IsTrue)
	_, ok = unitsFull[1].(*loader.Loader)
	c.Assert(ok, IsTrue)

	cfg.Mode = config.ModeIncrement
	unitsIncr := createUnits(cfg, nil)
	c.Assert(unitsIncr, HasLen, 1)
	_, ok = unitsIncr[0].(*syncer.Syncer)
	c.Assert(ok, IsTrue)

	cfg.Mode = config.ModeAll
	unitsAll := createUnits(cfg, nil)
	c.Assert(unitsAll, HasLen, 3)
	_, ok = unitsAll[0].(*mydumper.Mydumper)
	c.Assert(ok, IsTrue)
	_, ok = unitsAll[1].(*loader.Loader)
	c.Assert(ok, IsTrue)
	_, ok = unitsAll[2].(*syncer.Syncer)
	c.Assert(ok, IsTrue)
}

type MockUnit struct {
	processErrorCh chan error
	errInit        error
	errUpdate      error

	isFresh  bool
	errFresh error

	typ pb.UnitType
}

func NewMockUnit(typ pb.UnitType) *MockUnit {
	return &MockUnit{
		typ:            typ,
		processErrorCh: make(chan error),
		isFresh:        true,
	}
}

func (m *MockUnit) Init(ctx context.Context) error {
	return m.errInit
}

func (m *MockUnit) Process(ctx context.Context, pr chan pb.ProcessResult) {
	select {
	case <-ctx.Done():
		pr <- pb.ProcessResult{
			IsCanceled: true,
			Errors:     nil,
		}
	case err := <-m.processErrorCh:
		if err == nil {
			pr <- pb.ProcessResult{}
		} else {
			pr <- pb.ProcessResult{
				Errors: append([]*pb.ProcessError{}, unit.NewProcessError(pb.ErrorType_UnknownError, err)),
			}
		}
	}
}

func (m *MockUnit) Close() {}

func (m MockUnit) Pause() {}

func (m *MockUnit) Resume(ctx context.Context, pr chan pb.ProcessResult) { m.Process(ctx, pr) }

func (m *MockUnit) Update(cfg *config.SubTaskConfig) error {
	return m.errUpdate
}

func (m *MockUnit) Status() interface{} {
	switch m.typ {
	case pb.UnitType_Check:
		return &pb.CheckStatus{}
	case pb.UnitType_Dump:
		return &pb.DumpStatus{}
	case pb.UnitType_Load:
		return &pb.LoadStatus{}
	case pb.UnitType_Sync:
		return &pb.SyncStatus{}
	default:
		return struct{}{}
	}
}

func (m *MockUnit) Error() interface{} { return nil }

func (m *MockUnit) Type() pb.UnitType { return m.typ }

func (m *MockUnit) IsFreshTask(ctx context.Context) (bool, error) { return m.isFresh, m.errFresh }

func (m *MockUnit) InjectProcessError(ctx context.Context, err error) error {
	newCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	select {
	case <-newCtx.Done():
		return newCtx.Err()
	case m.processErrorCh <- err:
	}

	return nil
}

func (m *MockUnit) InjectInitError(err error) { m.errInit = err }

func (m *MockUnit) InjectUpdateError(err error) { m.errUpdate = err }

func (m *MockUnit) InjectFreshError(isFresh bool, err error) { m.isFresh, m.errFresh = isFresh, err }

func (t *testSubTask) TestSubTaskNormalUsage(c *C) {
	cfg := &config.SubTaskConfig{
		Name: "testSubtaskScene",
		Mode: config.ModeFull,
	}

	st := NewSubTask(cfg, nil)
	c.Assert(st.Stage(), DeepEquals, pb.Stage_New)

	// test empty and fail
	defer func() {
		createUnits = createRealUnits
	}()
	createUnits = func(cfg *config.SubTaskConfig, etcdClient *clientv3.Client) []unit.Unit {
		return nil
	}
	st.Run()
	c.Assert(st.Stage(), Equals, pb.Stage_Paused)
	c.Assert(strings.Contains(st.Result().Errors[0].Msg, "has no dm units for mode"), IsTrue)

	mockDumper := NewMockUnit(pb.UnitType_Dump)
	mockLoader := NewMockUnit(pb.UnitType_Load)
	createUnits = func(cfg *config.SubTaskConfig, etcdClient *clientv3.Client) []unit.Unit {
		return []unit.Unit{mockDumper, mockLoader}
	}

	st.Run()
	c.Assert(st.Stage(), Equals, pb.Stage_Running)
	c.Assert(st.CurrUnit(), Equals, mockDumper)
	c.Assert(st.Error(), IsNil)
	c.Assert(st.Result(), IsNil)

	// finish dump
	c.Assert(mockDumper.InjectProcessError(context.Background(), nil), IsNil)
	for i := 0; i < 10; i++ {
		if st.CurrUnit().Type() == pb.UnitType_Load {
			break
		}
		time.Sleep(time.Millisecond)
	}
	c.Assert(st.CurrUnit(), Equals, mockLoader)
	c.Assert(st.Result(), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Running)

	// fail loader
	c.Assert(mockLoader.InjectProcessError(context.Background(), errors.New("loader process error")), IsNil)
	for i := 0; i < 10; i++ {
		res := st.Result()
		if res != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	c.Assert(st.CurrUnit(), Equals, mockLoader)
	c.Assert(st.Result(), NotNil)
	c.Assert(st.Result().Errors, HasLen, 1)
	c.Assert(strings.Contains(st.Result().Errors[0].Msg, "loader process error"), IsTrue)
	c.Assert(st.Stage(), Equals, pb.Stage_Paused)

	// restore from pausing
	c.Assert(st.Resume(), IsNil)
	c.Assert(st.CurrUnit(), Equals, mockLoader)
	c.Assert(st.Result(), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Running)

	// update in running
	c.Assert(st.Update(nil), NotNil)
	c.Assert(st.CurrUnit(), Equals, mockLoader)
	c.Assert(st.Result(), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Running)

	// Pause
	c.Assert(st.Pause(), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Paused)
	c.Assert(st.CurrUnit(), Equals, mockLoader)
	if st.Result() != nil && (!st.Result().IsCanceled || len(st.Result().Errors) > 0) {
		c.Fatalf("result %+v is not right after closing", st.Result())
	}

	// update again
	c.Assert(st.Update(&config.SubTaskConfig{Name: "updateSubtask"}), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Paused)
	c.Assert(st.CurrUnit(), Equals, mockLoader)
	if st.Result() != nil && (!st.Result().IsCanceled || len(st.Result().Errors) > 0) {
		c.Fatalf("result %+v is not right after closing", st.Result())
	}

	// run again
	c.Assert(st.Resume(), IsNil)
	c.Assert(st.CurrUnit(), Equals, mockLoader)
	c.Assert(st.Result(), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Running)

	// pause again
	c.Assert(st.Pause(), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Paused)
	c.Assert(st.CurrUnit(), Equals, mockLoader)
	if st.Result() != nil && (!st.Result().IsCanceled || len(st.Result().Errors) > 0) {
		c.Fatalf("result %+v is not right after closing", st.Result())
	}

	// run again
	c.Assert(st.Resume(), IsNil)
	c.Assert(st.CurrUnit(), Equals, mockLoader)
	c.Assert(st.Result(), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Running)

	// finish loader
	c.Assert(mockLoader.InjectProcessError(context.Background(), nil), IsNil)
	for i := 0; i < 1000; i++ {
		if st.Stage() == pb.Stage_Finished {
			break
		}
		time.Sleep(time.Millisecond)
	}
	c.Assert(st.CurrUnit(), Equals, mockLoader)
	c.Assert(st.Stage(), Equals, pb.Stage_Finished)
	c.Assert(st.Result().Errors, HasLen, 0)
}

func (t *testSubTask) TestPauseAndResumeSubtask(c *C) {
	cfg := &config.SubTaskConfig{
		Name: "testSubtaskScene",
		Mode: config.ModeFull,
	}

	st := NewSubTask(cfg, nil)
	c.Assert(st.Stage(), DeepEquals, pb.Stage_New)

	mockDumper := NewMockUnit(pb.UnitType_Dump)
	mockLoader := NewMockUnit(pb.UnitType_Load)
	defer func() {
		createUnits = createRealUnits
	}()
	createUnits = func(cfg *config.SubTaskConfig, etcdClient *clientv3.Client) []unit.Unit {
		return []unit.Unit{mockDumper, mockLoader}
	}

	st.Run()
	c.Assert(st.Stage(), Equals, pb.Stage_Running)
	c.Assert(st.CurrUnit(), Equals, mockDumper)
	c.Assert(st.Error(), IsNil)
	c.Assert(st.Result(), IsNil)
	c.Assert(st.CheckUnit(), IsFalse)

	cfg1 := &config.SubTaskConfig{
		Name: "xxx",
		Mode: config.ModeFull,
		From: config.DBConfig{
			Host: "127.0.0.1",
		},
	}
	c.Assert(st.UpdateFromConfig(cfg1), IsNil)
	c.Assert(st.cfg.From, DeepEquals, cfg1.From)
	c.Assert(st.cfg.Name, Equals, "testSubtaskScene")

	// pause twice
	c.Assert(st.Pause(), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Paused)
	c.Assert(st.CurrUnit(), Equals, mockDumper)
	if st.Result() != nil && (!st.Result().IsCanceled || len(st.Result().Errors) > 0) {
		c.Fatalf("result %+v is not right after closing", st.Result())
	}

	c.Assert(st.Pause(), NotNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Paused)
	c.Assert(st.CurrUnit(), Equals, mockDumper)
	if st.Result() != nil && (!st.Result().IsCanceled || len(st.Result().Errors) > 0) {
		c.Fatalf("result %+v is not right after closing", st.Result())
	}

	//  resume
	c.Assert(st.Resume(), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Running)
	c.Assert(st.CurrUnit(), Equals, mockDumper)
	c.Assert(st.Error(), IsNil)
	c.Assert(st.Result(), IsNil)

	c.Assert(st.Pause(), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Paused)
	c.Assert(st.CurrUnit(), Equals, mockDumper)
	if st.Result() != nil && (!st.Result().IsCanceled || len(st.Result().Errors) > 0) {
		c.Fatalf("result %+v is not right after closing", st.Result())
	}

	// resume
	c.Assert(st.Resume(), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Running)
	c.Assert(st.CurrUnit(), Equals, mockDumper)
	c.Assert(st.Error(), IsNil)
	c.Assert(st.Result(), IsNil)

	// fail dumper
	c.Assert(mockDumper.InjectProcessError(context.Background(), errors.New("dumper process error")), IsNil)
	for i := 0; i < 1000; i++ {
		res := st.Result()
		if res != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	c.Assert(st.CurrUnit(), Equals, mockDumper)
	c.Assert(st.Result(), NotNil)
	c.Assert(st.Result().Errors, HasLen, 1)
	c.Assert(strings.Contains(st.Result().Errors[0].Msg, "dumper process error"), IsTrue)
	c.Assert(st.Stage(), Equals, pb.Stage_Paused)

	// pause
	c.Assert(st.Pause(), NotNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Paused)
	c.Assert(st.CurrUnit(), Equals, mockDumper)
	c.Assert(st.Result(), NotNil)
	c.Assert(st.Result().Errors, HasLen, 1)
	c.Assert(strings.Contains(st.Result().Errors[0].Msg, "dumper process error"), IsTrue)

	// resume twice
	c.Assert(st.Resume(), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Running)
	c.Assert(st.CurrUnit(), Equals, mockDumper)
	c.Assert(st.Error(), IsNil)
	c.Assert(st.Result(), IsNil)

	c.Assert(st.Resume(), NotNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Running)
	c.Assert(st.CurrUnit(), Equals, mockDumper)
	c.Assert(st.Error(), IsNil)
	c.Assert(st.Result(), IsNil)
	// finish dump
	c.Assert(mockDumper.InjectProcessError(context.Background(), nil), IsNil)
	for i := 0; i < 1000; i++ {
		if st.CurrUnit().Type() == pb.UnitType_Load {
			break
		}
		time.Sleep(time.Millisecond)
	}
	c.Assert(st.CurrUnit(), Equals, mockLoader)
	c.Assert(st.Result(), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Running)

	// finish loader
	c.Assert(mockLoader.InjectProcessError(context.Background(), nil), IsNil)
	for i := 0; i < 1000; i++ {
		if st.Stage() == pb.Stage_Finished {
			break
		}
		time.Sleep(time.Millisecond)
	}
	c.Assert(st.CurrUnit(), Equals, mockLoader)
	c.Assert(st.Stage(), Equals, pb.Stage_Finished)
	c.Assert(st.Result().Errors, HasLen, 0)

	st.Close()
	c.Assert(st.CurrUnit(), Equals, mockLoader)
	c.Assert(st.Stage(), Equals, pb.Stage_Finished)
	c.Assert(st.Result().Errors, HasLen, 0)

	st.Run()
	c.Assert(st.CurrUnit(), Equals, mockLoader)
	c.Assert(st.Stage(), Equals, pb.Stage_Finished)
	c.Assert(st.Result().Errors, HasLen, 0)
}

func (t *testSubTask) TestSubtaskWithStage(c *C) {
	cfg := &config.SubTaskConfig{
		Name: "testSubtaskScene",
		Mode: config.ModeFull,
	}

	st := NewSubTaskWithStage(cfg, pb.Stage_Paused, nil)
	c.Assert(st.Stage(), DeepEquals, pb.Stage_Paused)

	mockDumper := NewMockUnit(pb.UnitType_Dump)
	mockLoader := NewMockUnit(pb.UnitType_Load)
	defer func() {
		createUnits = createRealUnits
	}()
	createUnits = func(cfg *config.SubTaskConfig, etcdClient *clientv3.Client) []unit.Unit {
		return []unit.Unit{mockDumper, mockLoader}
	}

	// pause
	c.Assert(st.Pause(), NotNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Paused)
	c.Assert(st.CurrUnit(), Equals, nil)
	c.Assert(st.Result(), IsNil)

	c.Assert(st.Resume(), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Running)
	c.Assert(st.CurrUnit(), Equals, mockDumper)
	c.Assert(st.Error(), IsNil)
	c.Assert(st.Result(), IsNil)

	// pause again
	c.Assert(st.Pause(), IsNil)
	c.Assert(st.Stage(), Equals, pb.Stage_Paused)
	c.Assert(st.CurrUnit(), Equals, mockDumper)
	if st.Result() != nil && (!st.Result().IsCanceled || len(st.Result().Errors) > 0) {
		c.Fatalf("result %+v is not right after closing", st.Result())
	}

	st = NewSubTaskWithStage(cfg, pb.Stage_Finished, nil)
	c.Assert(st.Stage(), DeepEquals, pb.Stage_Finished)
	createUnits = func(cfg *config.SubTaskConfig, etcdClient *clientv3.Client) []unit.Unit {
		return []unit.Unit{mockDumper, mockLoader}
	}

	// close again
	st.Close()
	c.Assert(st.Stage(), Equals, pb.Stage_Finished)
	c.Assert(st.CurrUnit(), Equals, nil)
	c.Assert(st.Result(), IsNil)

	st.Run()
	c.Assert(st.Stage(), Equals, pb.Stage_Finished)
	c.Assert(st.CurrUnit(), Equals, nil)
	c.Assert(st.Result(), IsNil)

	// close again
	st.Close()
	c.Assert(st.Stage(), Equals, pb.Stage_Finished)
	c.Assert(st.CurrUnit(), Equals, nil)
	c.Assert(st.Result(), IsNil)
}
