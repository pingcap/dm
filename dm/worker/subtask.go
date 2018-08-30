// Copyright 2018 PingCAP, Inc.
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
	"sync"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-enterprise-tools/checker"
	"github.com/pingcap/tidb-enterprise-tools/dm/config"
	"github.com/pingcap/tidb-enterprise-tools/dm/pb"
	"github.com/pingcap/tidb-enterprise-tools/dm/unit"
	"github.com/pingcap/tidb-enterprise-tools/loader"
	"github.com/pingcap/tidb-enterprise-tools/mydumper"
	"github.com/pingcap/tidb-enterprise-tools/syncer"

	// hack for glide update, remove it later
	_ "github.com/pingcap/tidb-tools/pkg/check"
	_ "github.com/pingcap/tidb-tools/pkg/dbutil"
	_ "github.com/pingcap/tidb-tools/pkg/utils"
	"golang.org/x/net/context"
)

// createUnits creates process units base on task mode
func createUnits(cfg *config.SubTaskConfig) []unit.Unit {
	us := make([]unit.Unit, 0, 5)
	switch cfg.Mode {
	case config.ModeAll:
		us = append(us, checker.NewChecker(cfg))
		us = append(us, mydumper.NewMydumper(cfg))
		us = append(us, loader.NewLoader(cfg))
		us = append(us, syncer.NewSyncer(cfg))
	case config.ModeFull:
		us = append(us, mydumper.NewMydumper(cfg))
		us = append(us, loader.NewLoader(cfg))
	case config.ModeIncrement:
		us = append(us, checker.NewChecker(cfg))
		us = append(us, syncer.NewSyncer(cfg))
	default:
		log.Errorf("[subtask] unsupported task mode %s", cfg.Mode)
	}
	return us
}

// SubTask represents a sub task of data migration
type SubTask struct {
	cfg *config.SubTaskConfig

	sync.RWMutex
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	units    []unit.Unit // units do job one by one
	currUnit unit.Unit

	stage  pb.Stage          // stage of current sub task
	result *pb.ProcessResult // the process result, nil when is processing
}

// NewSubTask creates a new SubTask
func NewSubTask(cfg *config.SubTaskConfig) *SubTask {
	st := SubTask{
		cfg:   cfg,
		units: createUnits(cfg),
		stage: pb.Stage_New,
	}
	return &st
}

// Init initializes the sub task processing units
func (st *SubTask) Init() error {
	if len(st.units) < 1 {
		return errors.Errorf("sub task %s has no dm units for mode %s", st.cfg.Name, st.cfg.Mode)
	}
	// every unit does base initialization in `Init`, and this must pass before start running the sub task
	// other setups can be done in `Process`, like Loader's prepare which depends on Mydumper's output
	// but setups in `Process` should be treated carefully, let it's compatible with Pause / Resume
	for _, u := range st.units {
		err := u.Init()
		if err != nil {
			return errors.Errorf("sub task %s init dm-unit error %v", st.cfg.Name, errors.ErrorStack(err))
		}
	}
	st.setCurrUnit(st.units[0])
	return nil
}

// Run runs the sub task
func (st *SubTask) Run() {
	st.setStage(pb.Stage_Running)
	st.setResult(nil) // clear previous result
	cu := st.CurrUnit()
	log.Infof("[subtask] %s start running %s dm-unit", st.cfg.Name, cu.Type())
	st.ctx, st.cancel = context.WithCancel(context.Background())
	pr := make(chan pb.ProcessResult, 1)
	st.wg.Add(1)
	go st.fetchResult(st.ctx, st.cancel, pr)
	go cu.Process(st.ctx, pr)
}

// fetchResult fetches units process result
// when dm-unit report an error, we need to re-Process the sub task
func (st *SubTask) fetchResult(ctx context.Context, cancel context.CancelFunc, pr chan pb.ProcessResult) {
	defer st.wg.Done()

	select {
	case <-ctx.Done():
		return
	case result := <-pr:
		st.setResult(&result) // save result
		cancel()              // dm-unit finished, canceled or error occurred, always cancel processing

		if len(result.Errors) == 0 && st.Stage() == pb.Stage_Paused {
			return // paused by external request
		}

		var stage pb.Stage
		if len(result.Errors) == 0 {
			if result.IsCanceled {
				stage = pb.Stage_Stopped // canceled by user
			} else {
				stage = pb.Stage_Finished // process finished with no error
			}
		} else {
			stage = pb.Stage_Paused // error occurred, paused
		}
		st.setStage(stage)

		cu := st.CurrUnit()

		log.Infof("[subtask] %s dm-unit %s process returned with stage %s, status %s", st.cfg.Name, cu.Type(), stage.String(), st.StatusJSON())

		switch stage {
		case pb.Stage_Finished:
			cu.Close()
			nu := st.getNextUnit()
			if nu == nil {
				// Now, when finished, it only stops the process
				// if needed, we can refine to Close it
				log.Infof("[subtask] %s all process units finished", st.cfg.Name)
			} else {
				log.Infof("[subtask] %s switching to next dm-unit %s", st.cfg.Name, nu.Type())
				st.setCurrUnit(nu)
				// NOTE: maybe need a Lock mechanism for sharding scenario
				st.Run() // re-run for next process unit
			}
		case pb.Stage_Stopped:
		case pb.Stage_Paused:
			for _, err := range result.Errors {
				log.Errorf("[subtask] %s dm-unit %s process error with type %v:\n %v",
					st.cfg.Name, cu.Type(), err.Type, err.Msg)
			}
		}
	}
}

// setCurrUnit set current dm unit to ut and returns previous unit
func (st *SubTask) setCurrUnit(ut unit.Unit) unit.Unit {
	st.Lock()
	defer st.Unlock()
	pu := st.currUnit
	st.currUnit = ut
	return pu
}

// CurrUnit returns current dm unit
func (st *SubTask) CurrUnit() unit.Unit {
	st.RLock()
	defer st.RUnlock()
	return st.currUnit
}

// getNextUnit gets the next process unit from st.units
// if no next unit, return nil
func (st *SubTask) getNextUnit() unit.Unit {
	var (
		nu  unit.Unit
		cui = len(st.units)
		cu  = st.CurrUnit()
	)
	for i, u := range st.units {
		if u == cu {
			cui = i
		}
		if i == cui+1 {
			nu = u
			break
		}
	}
	return nu
}

func (st *SubTask) setStage(stage pb.Stage) {
	st.Lock()
	defer st.Unlock()
	st.stage = stage
}

// Stage returns the stage of the sub task
func (st *SubTask) Stage() pb.Stage {
	st.RLock()
	defer st.RUnlock()
	return st.stage
}

func (st *SubTask) setResult(result *pb.ProcessResult) {
	st.Lock()
	defer st.Unlock()
	st.result = result
}

// Result returns the result of the sub task
func (st *SubTask) Result() *pb.ProcessResult {
	st.RLock()
	defer st.RUnlock()
	return st.result
}

// Close stops the sub task
func (st *SubTask) Close() {
	log.Infof("[subtask] %s is closing", st.cfg.Name)
	if st.cancel == nil {
		log.Infof("[subtask] not run yet, no need to close")
		return
	}
	st.cancel()
	st.CurrUnit().Close()
	if st.Stage() != pb.Stage_Finished {
		st.setStage(pb.Stage_Paused)
	}
	st.wg.Wait()
}

// Pause pauses the running sub task
func (st *SubTask) Pause() error {
	if st.Stage() != pb.Stage_Running {
		return errors.NotValidf("current stage is not running")
	}
	st.setStage(pb.Stage_Paused)

	st.cancel()
	st.wg.Wait() // wait fetchResult return

	cu := st.CurrUnit()
	cu.Pause()

	log.Infof("[subtask] %s paused with %s dm-unit", st.cfg.Name, cu.Type())
	return nil
}

// Resume resumes the paused sub task
// similar to Run
func (st *SubTask) Resume() error {
	if st.Stage() != pb.Stage_Paused {
		return errors.NotValidf("current stage is not paused")
	}
	st.setStage(pb.Stage_Running)

	st.setResult(nil) // clear previous result
	cu := st.CurrUnit()
	log.Infof("[subtask] %s resuming with %s dm-unit", st.cfg.Name, cu.Type())

	st.ctx, st.cancel = context.WithCancel(context.Background())
	pr := make(chan pb.ProcessResult, 1)
	st.wg.Add(1)
	go st.fetchResult(st.ctx, st.cancel, pr)
	go cu.Resume(st.ctx, pr)
	return nil
}
