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

	"github.com/siddontang/go/sync2"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/streamer"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

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

	cfg          config.PurgeConfig
	baseRelayDir string
	indexPath    string // server-uuid.index file path
	operators    []RelayOperator
	interceptors []PurgeInterceptor
	strategies   map[strategyType]PurgeStrategy

	tctx *tcontext.Context
}

// NewRelayPurger creates a new purger
func NewRelayPurger(cfg config.PurgeConfig, baseRelayDir string, operators []RelayOperator, interceptors []PurgeInterceptor) Purger {
	p := &RelayPurger{
		cfg:          cfg,
		baseRelayDir: baseRelayDir,
		indexPath:    filepath.Join(baseRelayDir, utils.UUIDIndexFilename),
		operators:    operators,
		interceptors: interceptors,
		strategies:   make(map[strategyType]PurgeStrategy),
		tctx:         tcontext.Background().WithLogger(log.With(zap.String("component", "relay purger"))),
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

	p.tctx.L().Info("starting relay log purger", zap.Reflect("config", p.cfg))

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

	p.tctx.L().Info("closing relay log purger")

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
		return terror.Annotatef(err, "parse UUID index file %s", p.indexPath)
	}

	if req.Inactive {
		ps := p.strategies[strategyInactive]
		args := &inactiveArgs{
			relayBaseDir: p.baseRelayDir,
			uuids:        uuids,
		}
		return p.doPurge(ps, args)
	} else if req.Time > 0 {
		ps := p.strategies[strategyTime]
		args := &timeArgs{
			relayBaseDir: p.baseRelayDir,
			safeTime:     time.Unix(req.Time, 0),
			uuids:        uuids,
		}
		return p.doPurge(ps, args)
	} else if len(req.Filename) > 0 {
		ps := p.strategies[strategyFilename]
		args := &filenameArgs{
			relayBaseDir: p.baseRelayDir,
			filename:     req.Filename,
			subDir:       req.SubDir,
			uuids:        uuids,
		}
		return p.doPurge(ps, args)
	}
	return terror.ErrRelayPurgeRequestNotValid.Generate(req)
}

// tryPurge tries to do purge by check condition first
func (p *RelayPurger) tryPurge() {
	strategy, args, err := p.check()
	if err != nil {
		p.tctx.L().Error("check whether need to purge relay log files in background", zap.Error(err))
		return
	}
	if strategy == nil {
		return
	}
	err = p.doPurge(strategy, args)
	if err != nil {
		p.tctx.L().Error("do purge", zap.Stringer("strategy", strategy.Type()), zap.Error(err))
	}
}

// doPurge does the purging operation
func (p *RelayPurger) doPurge(ps PurgeStrategy, args StrategyArgs) error {
	if !p.purgingStrategy.CompareAndSwap(uint32(strategyNone), uint32(ps.Type())) {
		return terror.ErrRelayOtherStrategyIsPurging.Generate(ps.Type())
	}
	defer p.purgingStrategy.Set(uint32(strategyNone))

	for _, inter := range p.interceptors {
		forbidden, msg := inter.ForbidPurge()
		if forbidden {
			return terror.ErrRelayPurgeIsForbidden.Generate(msg)
		}
	}

	// set ActiveRelayLog lazily to make it can be protected by purgingStrategy
	earliest := p.earliestActiveRelayLog()
	if earliest == nil {
		return terror.ErrRelayNoActiveRelayLog.Generate()
	}
	args.SetActiveRelayLog(earliest)

	p.tctx.L().Info("start purging relay log files", zap.Stringer("type", ps.Type()), zap.Reflect("args", args))
	return ps.Do(args)
}

func (p *RelayPurger) check() (PurgeStrategy, StrategyArgs, error) {
	p.tctx.L().Info("checking whether needing to purge relay log files")

	uuids, err := utils.ParseUUIDIndex(p.indexPath)
	if err != nil {
		return nil, nil, terror.Annotatef(err, "parse UUID index file %s", p.indexPath)
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
			return nil, nil, terror.Annotatef(err, "check with %s with args %+v", ps.Type(), args)
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
			return nil, nil, terror.Annotatef(err, "check with %s with args %+v", ps.Type(), args)
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
func NewDummyPurger(cfg config.PurgeConfig, baseRelayDir string, operators []RelayOperator, interceptors []PurgeInterceptor) Purger {
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
