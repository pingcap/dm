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
	"testing"
	"time"

	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/terror"

	. "github.com/pingcap/check"
)

func TestSuite(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testStrategySuite{})

type testStrategySuite struct {
}

func (t *testStrategySuite) TestFiniteRetryStrategy(c *C) {
	strategy := &FiniteRetryStrategy{}

	params := Params{
		RetryCount:         1,
		RetryInterval:      Stable,
		FirstRetryDuration: time.Second,
		IsRetryableFn: func(int, error) bool {
			return false
		},
	}
	ctx := tcontext.Background()

	operateFn := func(*tcontext.Context, int) (interface{}, error) {
		return nil, terror.ErrDBDriverError.Generate("test database error")
	}

	_, opCount, err := strategy.DefaultRetryStrategy(ctx, params, operateFn)
	c.Assert(opCount, Equals, 0)
	c.Assert(err.(*terror.Error).Code(), Equals, terror.ErrCode(10001))

	params.IsRetryableFn = func(int, error) bool {
		return true
	}
	_, opCount, err = strategy.DefaultRetryStrategy(ctx, params, operateFn)
	c.Assert(opCount, Equals, 1)
	c.Assert(err.(*terror.Error).Code(), Equals, terror.ErrCode(10001))

	operateFn = func(*tcontext.Context, int) (interface{}, error) {
		return nil, terror.ErrDBInvalidConn.Generate("test invalid connection")
	}

	// invalid connection will return ErrInvalidConn immediately no matter how many retries left
	_, opCount, err = strategy.DefaultRetryStrategy(ctx, params, operateFn)
	c.Assert(opCount, Equals, 0)
	c.Assert(err.(*terror.Error).Code(), Equals, terror.ErrCode(10003))

	params.RetryCount = 10
	operateFn = func(ctx *tcontext.Context, i int) (interface{}, error) {
		if i == 8 {
			return nil, terror.ErrDBInvalidConn.Generate("test invalid connection")
		}
		return nil, terror.ErrDBDriverError.Generate("test database error")
	}

	_, opCount, err = strategy.DefaultRetryStrategy(ctx, params, operateFn)
	c.Assert(opCount, Equals, 8)
	c.Assert(err.(*terror.Error).Code(), Equals, terror.ErrCode(10003))
}
