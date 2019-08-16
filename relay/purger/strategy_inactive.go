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
	"fmt"
	"strings"

	"github.com/siddontang/go/sync2"
	"go.uber.org/zap"

	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/streamer"
	"github.com/pingcap/dm/pkg/terror"
)

// inactiveArgs represents args needed by inactiveStrategy
type inactiveArgs struct {
	relayBaseDir   string
	uuids          []string
	activeRelayLog *streamer.RelayLogInfo // earliest active relay log info
}

func (ia *inactiveArgs) SetActiveRelayLog(active *streamer.RelayLogInfo) {
	ia.activeRelayLog = active
}

func (ia *inactiveArgs) String() string {
	return fmt.Sprintf("(RelayBaseDir: %s, UUIDs: %s, ActiveRelayLog: %s)",
		ia.relayBaseDir, strings.Join(ia.uuids, ";"), ia.activeRelayLog)
}

// inactiveStrategy represents a relay purge strategy which purge all inactive relay log files
// definition of inactive relay log files:
//   * not writing by relay unit
//   * not reading by sync unit and will not be read by any running tasks
//     TODO zxc: judge tasks are running dumper / loader
type inactiveStrategy struct {
	purging sync2.AtomicInt32

	tctx *tcontext.Context
}

func newInactiveStrategy() PurgeStrategy {
	return &inactiveStrategy{
		tctx: tcontext.Background().WithLogger(log.With(zap.String("component", "relay purger"), zap.String("strategy", "inactive binlog file"))),
	}
}

func (s *inactiveStrategy) Check(args interface{}) (bool, error) {
	// do not support purge in the background
	return false, nil
}

func (s *inactiveStrategy) Do(args interface{}) error {
	if !s.purging.CompareAndSwap(0, 1) {
		return terror.ErrRelayThisStrategyIsPurging.Generate()
	}
	defer s.purging.Set(0)

	ia, ok := args.(*inactiveArgs)
	if !ok {
		return terror.ErrRelayPurgeArgsNotValid.Generate(args, args)
	}

	return purgeRelayFilesBeforeFile(s.tctx, ia.relayBaseDir, ia.uuids, ia.activeRelayLog)
}

func (s *inactiveStrategy) Purging() bool {
	return s.purging.Get() > 0
}

func (s *inactiveStrategy) Type() strategyType {
	return strategyInactive
}
