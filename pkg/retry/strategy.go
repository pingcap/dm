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

// retryInterval represents enum of retry wait interval
type retryInterval uint8

const (
	// LinearIncrease represents increase time wait retry policy, every retry should wait more time depends on increasing retry times
	LinearIncrease retryInterval = iota + 1
	// Stable represents fixed time wait retry policy, every retry should wait a fixed time
	Stable
)

// DefaultRetryParams define parameters for DefaultRetryStrategy
// it's a union set of all implements' parameters which implement DefaultRetryStrategy
type DefaultRetryParams struct {
	RetryCount         int
	FirstRetryDuration time.Duration

	RetryInterval retryInterval

	// IsRetryableFn tells whether we should retry when operateFn failed
	// params: (number of retry, error of operation)
	// return: (bool)
	//   1. true: means operateFn can be retried
	//   2. false: means operateFn cannot retry after receive this error
	IsRetryableFn func(int, error) bool
}

// Strategy define different kind of retry strategy
type Strategy interface {

	// DefaultRetryStrategy define retry strategy
	// params: (retry parameters for this strategy, a normal operation)
	// return: (result of operation, number of retry, error of operation)
	DefaultRetryStrategy(ctx *tcontext.Context,
		params DefaultRetryParams,
		// TODO: remove `number of retry` in operateFn, we need it now because loader.ExecuteSQL operation need it
		// operateFn:
		//   params: (context, number of retry)
		//   return: (result of operation, error of operation)
		operateFn func(*tcontext.Context, int) (interface{}, error),
	) (interface{}, int, error)
}

// FiniteRetryStrategy will retry `RetryCount` times when failed to operate DB.
type FiniteRetryStrategy struct {
}

// DefaultRetryStrategy wait `FirstRetryDuration` before it starts first retry, and then rest of retries wait time depends on RetryInterval.
// ErrInvalidConn is a special error, need a public retry strategy, so return it to up layer continue retry.
func (*FiniteRetryStrategy) DefaultRetryStrategy(ctx *tcontext.Context, params DefaultRetryParams,
	operateFn func(*tcontext.Context, int) (interface{}, error)) (interface{}, int, error) {
	var err error
	var ret interface{}
	i := 0
	for ; i < params.RetryCount; i++ {
		ret, err = operateFn(ctx, i)
		if err != nil {
			if IsInvalidConnError(err) {
				ctx.L().Warn(fmt.Sprintf("met invalid connection error, in %dth retry", i))
				return nil, i, err
			}
			if params.IsRetryableFn(i, err) {
				duration := params.FirstRetryDuration

				switch params.RetryInterval {
				case LinearIncrease:
					duration = time.Duration(i+1) * params.FirstRetryDuration
				default:
				}

				select {
				case <-ctx.Context().Done():
					return nil, i, err
				case <-time.After(duration):
				}
				continue
			}
		}
		break
	}
	return ret, i, err
}
