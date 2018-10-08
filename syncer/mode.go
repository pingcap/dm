package syncer

import (
	"time"

	"golang.org/x/net/context"
)

func (s *Syncer) enableSafeModeInitializationPhase(ctx context.Context) {
	safeMode.Set(true)

	go func() {
		ctx2, cancel := context.WithCancel(ctx)
		defer func() {
			cancel()
			safeMode.Set(s.cfg.SafeMode)
		}()

		select {
		case <-ctx2.Done():
		case <-time.After(5 * time.Minute):
		}
	}()
}
