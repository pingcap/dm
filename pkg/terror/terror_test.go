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

package terror

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/check"
	perrors "github.com/pingcap/errors"
)

func TestT(t *testing.T) {
	check.TestingT(t)
}

var _ = check.Suite(&testTErrorSuite{})

type testTErrorSuite struct {
}

func (t *testTErrorSuite) TestTError(c *check.C) {
	var (
		code        = codeDBBadConn
		class       = ClassDatabase
		scope       = ScopeUpstream
		level       = LevelMedium
		message     = "bad connection"
		messageArgs = "message with args: %s"
		commonErr   = errors.New("common error")
	)

	c.Assert(ClassDatabase.String(), check.Equals, errClass2Str[ClassDatabase])
	c.Assert(ErrClass(10000).String(), check.Equals, "unknown error class: 10000")

	c.Assert(ScopeUpstream.String(), check.Equals, errScope2Str[ScopeUpstream])
	c.Assert(ErrScope(10000).String(), check.Equals, "unknown error scope: 10000")

	c.Assert(LevelHigh.String(), check.Equals, errLevel2Str[LevelHigh])
	c.Assert(ErrLevel(10000).String(), check.Equals, "unknown error level: 10000")

	// test Error basic API
	err := New(code, class, scope, level, message)
	c.Assert(err.Code(), check.Equals, code)
	c.Assert(err.Class(), check.Equals, class)
	c.Assert(err.Scope(), check.Equals, scope)
	c.Assert(err.Level(), check.Equals, level)
	c.Assert(err.Error(), check.Equals, fmt.Sprintf("[%d:%s:%s:%s] %s", code, class, scope, level, err.getMsg()))

	err.message = messageArgs
	c.Assert(err.getMsg(), check.Equals, err.message)
	err.args = []interface{}{"1062"}
	c.Assert(err.getMsg(), check.Equals, fmt.Sprintf(err.message, err.args...))

	// test Error Generate/Generatef
	err2 := err.Generate("1063")
	c.Assert(err.Equal(err2), check.IsTrue)
	c.Assert(fmt.Sprintf("%s", err2), check.Equals, fmt.Sprintf("[%d:%s:%s:%s] "+err.message, code, class, scope, level, "1063"))

	err3 := err.Generatef("new message format: %s", "1064")
	c.Assert(err.Equal(err3), check.IsTrue)
	c.Assert(fmt.Sprintf("%s", err3), check.Equals, fmt.Sprintf("[%d:%s:%s:%s] new message format: %s", code, class, scope, level, "1064"))

	// test Error Delegate
	err4 := err.Delegate(commonErr, "1065")
	c.Assert(err.Equal(err4), check.IsTrue)
	c.Assert(fmt.Sprintf("%s", err4), check.Equals, fmt.Sprintf("[%d:%s:%s:%s] "+err.message+": %s", code, class, scope, level, "1065", commonErr))
	c.Assert(perrors.Cause(err4), check.Equals, commonErr)

	// test Error Annotate
	oldMsg := err.getMsg()
	err5 := Annotate(err, "annotate error")
	c.Assert(err.Equal(err5), check.IsTrue)
	c.Assert(fmt.Sprintf("%s", err5), check.Equals, fmt.Sprintf("[%d:%s:%s:%s] annotate error: %s", code, class, scope, level, oldMsg))

	c.Assert(Annotate(nil, ""), check.IsNil)
	annotateErr := Annotate(commonErr, "annotate")
	_, ok := annotateErr.(*Error)
	c.Assert(ok, check.IsFalse)
	c.Assert(perrors.Cause(annotateErr), check.Equals, commonErr)

	// test Error Annotatef
	oldMsg = err.getMsg()
	err6 := Annotatef(err, "annotatef error %s", "1066")
	c.Assert(err.Equal(err6), check.IsTrue)
	c.Assert(fmt.Sprintf("%s", err6), check.Equals, fmt.Sprintf("[%d:%s:%s:%s] annotatef error 1066: %s", code, class, scope, level, oldMsg))

	c.Assert(Annotatef(nil, ""), check.IsNil)
	annotateErr = Annotatef(commonErr, "annotatef %s", "1067")
	_, ok = annotateErr.(*Error)
	c.Assert(ok, check.IsFalse)
	c.Assert(perrors.Cause(annotateErr), check.Equals, commonErr)

	// test format
	c.Assert(fmt.Sprintf("%q", err), check.Equals, fmt.Sprintf("%q", err.Error()))
	// err has no stack trace
	c.Assert(fmt.Sprintf("%+v", err), check.Equals, err.Error())
	c.Assert(fmt.Sprintf("%v", err), check.Equals, err.Error())
	// err2 has stack trace
	verbose := strings.Split(fmt.Sprintf("%+v", err2), "\n")
	c.Assert(len(verbose) > 5, check.IsTrue)
	c.Assert(verbose[0], check.Equals, err2.Error())
	c.Assert(verbose[1], check.Matches, ".*\\(\\*Error\\)\\.Generate")
	c.Assert(fmt.Sprintf("%v", err2), check.Equals, err2.Error())
}
