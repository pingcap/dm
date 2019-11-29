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

package log

import (
	"context"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
)

func TestLog(t *testing.T) {
	TestingT(t)
}

type testLogSuite struct{}

var _ = Suite(&testLogSuite{})

func (s *testLogSuite) TestTestLogger(c *C) {
	logger, buffer := makeTestLogger()
	logger.Warn("the message", zap.Int("number", 123456), zap.Ints("array", []int{7, 8, 9}))
	c.Assert(
		buffer.Stripped(), Equals,
		`{"$lvl":"WARN","$msg":"the message","number":123456,"array":[7,8,9]}`,
	)
	buffer.Reset()
	logger.ErrorFilterContextCanceled("the message", zap.Int("number", 123456),
		zap.Ints("array", []int{7, 8, 9}), zap.Error(context.Canceled))
	c.Assert(buffer.Stripped(), Equals, "")
	buffer.Reset()
	logger.ErrorFilterContextCanceled("the message", zap.Int("number", 123456),
		zap.Ints("array", []int{7, 8, 9}), ShortError(errors.Annotate(context.Canceled, "extra info")))
	c.Assert(buffer.Stripped(), Equals, "")
}

// makeTestLogger creates a Logger instance which produces JSON logs.
func makeTestLogger() (Logger, *zaptest.Buffer) {
	buffer := new(zaptest.Buffer)
	logger := zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(zapcore.EncoderConfig{
			LevelKey:       "$lvl",
			MessageKey:     "$msg",
			EncodeLevel:    zapcore.CapitalLevelEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
		}),
		buffer,
		zap.DebugLevel,
	))
	return Logger{Logger: logger}, buffer
}
