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

	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testAtomicSuite{})

type testAtomicSuite struct{}

func (t *testAtomicSuite) TestAtomicError(c *C) {
	var (
		e   AtomicError
		err = errors.New("test")
	)
	err2 := e.Get()
	c.Assert(err2, Equals, nil)

	e.Set(err)
	err2 = e.Get()
	c.Assert(err2, DeepEquals, err)

	err = errors.New("test2")
	err2 = e.Get()
	c.Assert(err2.Error(), Equals, "test")
	c.Assert(err2, Not(Equals), err)

	e.Set(nil)
	err2 = e.Get()
	c.Assert(err2, Equals, nil)
}

func (t *testAtomicSuite) TestAtomicString(c *C) {
	var (
		s   AtomicString
		str = "test"
	)
	str2 := s.Get()
	c.Assert(str2, Equals, "")

	s.Set(str)
	str2 = s.Get()
	c.Assert(str2, Equals, str)

	originStr := str
	str = "test2" //nolint:ineffassign
	str2 = s.Get()
	c.Assert(str2, Equals, originStr)

	s.Set("")
	str2 = s.Get()
	c.Assert(str2, Equals, "")
}
