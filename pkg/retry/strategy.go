package retry

import (
	tcontext "github.com/pingcap/dm/pkg/context"
	"time"
)

// Speed represents enum of retry speed
type Speed uint8

const (
	// SpeedSlow represents slow retry speed, every retry should wait more time depends on increasing retry times
	SpeedSlow Speed = iota + 1
	// SpeedStable represents fixed retry speed, every retry should wait fix time
	SpeedStable
)

// Strategy define different kind of retry strategy
type Strategy interface {

	// FiniteRetryStrategy will retry `retryCount` times when failed to operate DB.
	// it will wait `firstRetryDuration` before it starts first retry, and then rest of retries wait time depends on retrySpeed.
	FiniteRetryStrategy(ctx *tcontext.Context, retryCount int, firstRetryDuration time.Duration, retrySpeed Speed,
		operateFn func(*tcontext.Context, int) (interface{}, error),
		retryFn func(int, error) bool) (interface{}, error)
}
