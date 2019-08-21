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
