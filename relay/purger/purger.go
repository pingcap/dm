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

package purger

import (
	"context"
	"path/filepath"
	"sync"
	"time"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/errors"
	"github.com/siddontang/go/sync2"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/streamer"
	"github.com/pingcap/dm/pkg/utils"
)

// errors created by Purger
var (
	// logger writes log start with `[component=purger]`
	logger log.Logger

	ErrSelfPurging  = errors.New("this strategy is purging")
	MsgOtherPurging = "%s is purging"
)

func init() {
	logger = log.With(zap.String("component", "purger"))
}

// RelayOperator represents an operator for relay log files, like writer, reader
type RelayOperator interface {
	// EarliestActiveRelayLog returns the earliest active relay log info in this operator
	EarliestActiveRelayLog() *streamer.RelayLogInfo
}

// PurgeInterceptor represents an interceptor may forbid the purge process
type PurgeInterceptor interface {
	// ForbidPurge returns whether forbidding purge currently and an optional message
	ForbidPurge() (bool, string)
}

const (
	stageNew int32 = iota
	stageRunning
	stageClosed
)

// Purger purges relay log according to some strategies
type Purger interface {
	// Start starts strategies by config
	Start()
	// Close stops the started strategies
	Close()
	// Purging returns whether the purger is purging
	Purging() bool
	// Do does the purge process one time
	Do(ctx context.Context, req *pb.PurgeRelayRequest) error
}

// NewPurger creates a new purger
var NewPurger = NewRelayPurger

// RelayPurger purges relay log according to some strategies
type RelayPurger struct {
	lock            sync.RWMutex
	wg              sync.WaitGroup
	cancel          context.CancelFunc
	running         sync2.AtomicInt32
	purgingStrategy sync2.AtomicUint32

	cfg          Config
	baseRelayDir string
	indexPath    string // server-uuid.index file path
	operators    []RelayOperator
	interceptors []PurgeInterceptor
	strategies   map[strategyType]PurgeStrategy
}

// NewRelayPurger creates a new purger
func NewRelayPurger(cfg Config, baseRelayDir string, operators []RelayOperator, interceptors []PurgeInterceptor) Purger {
	p := &RelayPurger{
		cfg:          cfg,
		baseRelayDir: baseRelayDir,
		indexPath:    filepath.Join(baseRelayDir, utils.UUIDIndexFilename),
		operators:    operators,
		interceptors: interceptors,
		strategies:   make(map[strategyType]PurgeStrategy),
	}

	// add strategies
	p.strategies[strategyInactive] = newInactiveStrategy()
	p.strategies[strategyFilename] = newFilenameStrategy()
	p.strategies[strategyTime] = newTimeStrategy()
	p.strategies[strategySpace] = newSpaceStrategy()

	return p
}

// Start starts strategies by config
func (p *RelayPurger) Start() {
	if !p.running.CompareAndSwap(stageNew, stageRunning) {
		return
	}

	if p.cfg.Interval <= 0 || (p.cfg.Expires <= 0 && p.cfg.RemainSpace <= 0) {
		return // no need do purge in the background
	}

	logger.Info("starting relay log purger", zap.Reflect("config", p.cfg))

	// Close will wait process to return
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.run()
	}()
}

// run starts running the process
// NOTE: ensure run is called at most once of a Purger
func (p *RelayPurger) run() {
	ticker := time.NewTicker(time.Duration(p.cfg.Interval) * time.Second)
	defer ticker.Stop()

	var ctx context.Context
	p.lock.Lock()
	ctx, p.cancel = context.WithCancel(context.Background())
	p.lock.Unlock()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.tryPurge()
		}
	}
}

// Close stops the started strategies
func (p *RelayPurger) Close() {
	if !p.running.CompareAndSwap(stageRunning, stageClosed) {
		return
	}

	logger.Info("[purger] closing relay log purger")

	p.lock.RLock()
	if p.cancel != nil {
		p.cancel()
	}
	p.lock.RUnlock()
	p.wg.Wait()
}

// Purging returns whether the purger is purging
func (p *RelayPurger) Purging() bool {
	return p.purgingStrategy.Get() != uint32(strategyNone)
}

// Do does the purge process one time
func (p *RelayPurger) Do(ctx context.Context, req *pb.PurgeRelayRequest) error {
	uuids, err := utils.ParseUUIDIndex(p.indexPath)
	if err != nil {
		return errors.Annotatef(err, "parse UUID index file %s", p.indexPath)
	}

	if req.Inactive {
		ps := p.strategies[strategyInactive]
		args := &inactiveArgs{
			relayBaseDir: p.baseRelayDir,
			uuids:        uuids,
		}
		return errors.Trace(p.doPurge(ps, args))
	} else if req.Time > 0 {
		ps := p.strategies[strategyTime]
		args := &timeArgs{
			relayBaseDir: p.baseRelayDir,
			safeTime:     time.Unix(req.Time, 0),
			uuids:        uuids,
		}
		return errors.Trace(p.doPurge(ps, args))
	} else if len(req.Filename) > 0 {
		ps := p.strategies[strategyFilename]
		args := &filenameArgs{
			relayBaseDir: p.baseRelayDir,
			filename:     req.Filename,
			subDir:       req.SubDir,
			uuids:        uuids,
		}
		return errors.Trace(p.doPurge(ps, args))
	}
	return errors.NotValidf("request %+v", req)
}

// tryPurge tries to do purge by check condition first
func (p *RelayPurger) tryPurge() {
	strategy, args, err := p.check()
	if err != nil {
		logger.Error("[purger] check whether need to purge relay log files in background", zap.Error(err))
		return
	}
	if strategy == nil {
		return
	}
	err = p.doPurge(strategy, args)
	if err != nil {
		logger.Error("do purge", zap.String("strategy", strategy.Type()), zap.Error(err))
	}
}

// doPurge does the purging operation
func (p *RelayPurger) doPurge(ps PurgeStrategy, args StrategyArgs) error {
	if !p.purgingStrategy.CompareAndSwap(uint32(strategyNone), uint32(ps.Type())) {
		return errors.Errorf(MsgOtherPurging, ps.Type())
	}
	defer p.purgingStrategy.Set(uint32(strategyNone))

	for _, inter := range p.interceptors {
		forbidden, msg := inter.ForbidPurge()
		if forbidden {
			return errors.Errorf("relay log purge is forbidden temporarily, because %s, please try again later", msg)
		}
	}

	// set ActiveRelayLog lazily to make it can be protected by purgingStrategy
	earliest := p.earliestActiveRelayLog()
	if earliest == nil {
		return errors.NotValidf("no active relay log file found")
	}
	args.SetActiveRelayLog(earliest)

	logger.Info("[purger] start purging relay log files", zap.String("type", ps.Type()), zap.Reflect("args", args))
	return errors.Trace(ps.Do(args))
}

func (p *RelayPurger) check() (PurgeStrategy, StrategyArgs, error) {
	logger.Info("[purger] checking whether needing to purge relay log files")

	uuids, err := utils.ParseUUIDIndex(p.indexPath)
	if err != nil {
		return nil, nil, errors.Annotatef(err, "parse UUID index file %s", p.indexPath)
	}

	// NOTE: no priority supported yet
	// 1. strategyInactive only used by dmctl manually
	// 2. strategyFilename only used by dmctl manually

	// 3. strategySpace should be started if set RemainSpace
	if p.cfg.RemainSpace > 0 {
		args := &spaceArgs{
			relayBaseDir: p.baseRelayDir,
			remainSpace:  p.cfg.RemainSpace,
			uuids:        uuids,
		}
		ps := p.strategies[strategySpace]
		need, err := ps.Check(args)
		if err != nil {
			return nil, nil, errors.Annotatef(err, "check with %s with args %+v", ps.Type(), args)
		}
		if need {
			return ps, args, nil
		}
	}

	// 4. strategyTime should be started if set Expires
	if p.cfg.Expires > 0 {
		safeTime := time.Now().Add(time.Duration(-p.cfg.Expires) * time.Hour)
		args := &timeArgs{
			relayBaseDir: p.baseRelayDir,
			safeTime:     safeTime,
			uuids:        uuids,
		}
		ps := p.strategies[strategyTime]
		need, err := ps.Check(args)
		if err != nil {
			return nil, nil, errors.Annotatef(err, "check with %s with args %+v", ps.Type(), args)
		}
		if need {
			return ps, args, nil
		}
	}

	return nil, nil, nil
}

// earliestActiveRelayLog returns the current earliest active relay log info
func (p *RelayPurger) earliestActiveRelayLog() *streamer.RelayLogInfo {
	var earliest *streamer.RelayLogInfo
	for _, op := range p.operators {
		info := op.EarliestActiveRelayLog()
		if info == nil {
			continue
		} else if earliest == nil || info.Earlier(earliest) {
			earliest = info
		}
	}
	return earliest
}

/************ dummy purger **************/
type dummyPurger struct{}

// NewDummyPurger returns a dummy purger
func NewDummyPurger(cfg Config, baseRelayDir string, operators []RelayOperator, interceptors []PurgeInterceptor) Purger {
	return &dummyPurger{}
}

// Start implements interface of Purger
func (d *dummyPurger) Start() {}

// Close implements interface of Purger
func (d *dummyPurger) Close() {}

// Purging implements interface of Purger
func (d *dummyPurger) Purging() bool {
	return false
}

// Do implements interface of Purger
func (d *dummyPurger) Do(ctx context.Context, req *pb.PurgeRelayRequest) error {
	return nil
}
