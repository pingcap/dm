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

package syncer

import (
	"time"

	"github.com/pingcap/failpoint"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/unit"
	tcontext "github.com/pingcap/dm/pkg/context"
	sm "github.com/pingcap/dm/syncer/safe-mode"
)

func (s *Syncer) enableSafeModeInitializationPhase(tctx *tcontext.Context, safeMode *sm.SafeMode) {
	safeMode.Reset(tctx)  // in initialization phase, reset first
	safeMode.Add(tctx, 1) // try to enable

	if s.cfg.SafeMode {
		safeMode.Add(tctx, 1) // add 1 but should no corresponding -1
		s.tctx.L().Info("enable safe-mode by config")
	}

	go func() {
		defer func() {
			err := safeMode.Add(tctx, -1) // try to disable after 5 minutes
			if err != nil {
				// send error to the fatal chan to interrupt the process
				s.runFatalChan <- unit.NewProcessError(pb.ErrorType_UnknownError, err)
			}
		}()

		initPhaseSeconds := 300

		failpoint.Inject("SafeModeInitPhaseSeconds", func(val failpoint.Value) {
			seconds, _ := val.(int)
			initPhaseSeconds = seconds
			s.tctx.L().Info("set initPhaseSeconds", zap.String("failpoint", "SafeModeInitPhaseSeconds"), zap.Int("value", seconds))
		})
		select {
		case <-tctx.Context().Done():
		case <-time.After(time.Duration(initPhaseSeconds) * time.Second):
		}
	}()
}
