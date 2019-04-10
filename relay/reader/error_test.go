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

package reader

import (
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
)

var (
	_ = Suite(&testErrorSuite{})
)

type testErrorSuite struct {
}

func (t *testErrorSuite) TestRetryable(c *C) {
	err := errors.New("custom error")
	c.Assert(isRetryableError(err), IsFalse)

	cases := []error{
		context.DeadlineExceeded,
		errors.Annotate(context.DeadlineExceeded, "annotated"),
	}
	for _, cs := range cases {
		c.Assert(isRetryableError(cs), IsTrue)
	}
}

func (t *testErrorSuite) TestIgnorable(c *C) {
	err := errors.New("custom error")
	c.Assert(isIgnorableError(err), IsFalse)

	cases := []error{
		context.Canceled,
		errors.Annotate(context.Canceled, "annotated"),
	}
	for _, cs := range cases {
		c.Assert(isIgnorableError(cs), IsTrue)
	}
}
