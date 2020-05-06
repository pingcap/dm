// Copyright 2020 PingCAP, Inc.
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

package atomic2

import (
	"errors"
	"testing"

	"github.com/pingcap/check"
)

func TestT(t *testing.T) {
	check.TestingT(t)
}

var _ = check.Suite(&testAtomicSuite{})

type testAtomicSuite struct{}

func (t *testAtomicSuite) TestAtomicError(c *check.C) {
	var (
		e   AtomicError
		err = errors.New("test")
	)
	err2 := e.Get()
	c.Assert(err2, check.Equals, nil)

	e.Set(err)
	err2 = e.Get()
	c.Assert(err2, check.DeepEquals, err)

	err = errors.New("test2")
	err2 = e.Get()
	c.Assert(err2.Error(), check.Equals, "test")

	e.Set(nil)
	err2 = e.Get()
	c.Assert(err2, check.Equals, nil)
}
