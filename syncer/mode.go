package syncer

import (
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"golang.org/x/net/context"

	"github.com/pingcap/tidb-enterprise-tools/dm/pb"
	"github.com/pingcap/tidb-enterprise-tools/dm/unit"
	sm "github.com/pingcap/tidb-enterprise-tools/syncer/safe-mode"
)

func (s *Syncer) enableSafeModeInitializationPhase(ctx context.Context, safeMode *sm.SafeMode) {
	safeMode.Reset() // in initialization phase, reset first
	safeMode.Add(1)  // try to enable

	if s.cfg.SafeMode {
		safeMode.Add(1) // add 1 but should no corresponding -1
		log.Info("[syncer] enable safe-mode by config")
	}

	go func() {
		defer func() {
			err := safeMode.Add(-1) // try to disable after 5 minutes
			if err != nil {
				// send error to the fatal chan to interrupt the process
				s.runFatalChan <- unit.NewProcessError(pb.ErrorType_UnknownError, errors.ErrorStack(err))
			}
		}()

		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Minute):
		}
	}()
}
