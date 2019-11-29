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

package log_test

import (
	"context"
	"testing"

	"github.com/pingcap/dm/pkg/log"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"go.uber.org/zap"
)

func TestLog(t *testing.T) {
	TestingT(t)
}

type testLogSuite struct{}

var _ = Suite(&testLogSuite{})

func (s *testLogSuite) TestTestLogger(c *C) {
	logger, buffer := log.MakeTestLogger()
	logger.Warn("the message", zap.Int("number", 123456), zap.Ints("array", []int{7, 8, 9}))
	c.Assert(
		buffer.Stripped(), Equals,
		`{"$lvl":"WARN","$msg":"the message","number":123456,"array":[7,8,9]}`,
	)
	logger.ErrorFilterContextCanceled("the message", zap.Int("number", 123456),
		zap.Ints("array", []int{7, 8, 9}), zap.Error(context.Canceled))
	c.Assert(buffer.Stripped(), Equals, "")
	logger.ErrorFilterContextCanceled("the message", zap.Int("number", 123456),
		zap.Ints("array", []int{7, 8, 9}), log.ShortError(errors.Annotate(context.Canceled, "extra info")))
	c.Assert(buffer.Stripped(), Equals, "")
}
