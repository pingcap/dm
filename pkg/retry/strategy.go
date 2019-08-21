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

package retry

import (
	"fmt"
	"time"

	tcontext "github.com/pingcap/dm/pkg/context"
)

// Speed represents enum of retry speed
type Speed uint8

const (
	// SpeedSlow represents slow retry speed, every retry should wait more time depends on increasing retry times
	SpeedSlow Speed = iota + 1
	// SpeedStable represents fixed retry speed, every retry should wait fix time
	SpeedStable
)

// Params define parameters that all retry strategies need
// it makes DefaultRetryStrategy more abstract for other implements
type Params struct {
	RetryCount         int
	FirstRetryDuration time.Duration

	RetrySpeed Speed

	// IsRetryableFn tells whether need retry when operateFn failed
	IsRetryableFn func(int, error) bool
}

// Strategy define different kind of retry strategy
type Strategy interface {

	// DefaultRetryStrategy define retry strategy,
	// Params define retry parameters for different retry strategy
	// operateFn define a normal operation
	DefaultRetryStrategy(ctx *tcontext.Context,
		params Params,
		operateFn func(*tcontext.Context, int) (interface{}, error),
	) (interface{}, error)
}

// FiniteRetryStrategy will retry `RetryCount` times when failed to operate DB.
type FiniteRetryStrategy struct {
}

// DefaultRetryStrategy wait `FirstRetryDuration` before it starts first retry, and then rest of retries wait time depends on RetrySpeed.
// ErrInvalidConn is a special error, need a public retry strategy, so return it to up layer continue retry.
func (*FiniteRetryStrategy) DefaultRetryStrategy(ctx *tcontext.Context, params Params,
	operateFn func(*tcontext.Context, int) (interface{}, error)) (interface{}, error) {
	var err error
	var ret interface{}
	for i := 0; i < params.RetryCount; i++ {
		ret, err = operateFn(ctx, i)
		if err != nil {
			if IsInvalidConnError(err) {
				ctx.L().Warn(fmt.Sprintf("met invalid connection error, in %dth retry", i))
				return nil, err
			}
			if params.IsRetryableFn(i, err) {
				duration := params.FirstRetryDuration

				switch params.RetrySpeed {
				case SpeedSlow:
					duration = time.Duration(i+1) * params.FirstRetryDuration
				default:
				}

				select {
				case <-ctx.Context().Done():
					return nil, err
				case <-time.After(duration):
				}
				continue
			}
		}
		break
	}
	return ret, err
}
