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

package context

import (
	"context"

	"github.com/pingcap/dm/pkg/log"
)

// Context is used to in dm to record some context field like
// * go context
// * logger
type Context struct {
	Ctx    context.Context
	Logger log.Logger
}

// Background return a nop context
func Background() *Context {
	return &Context{
		Ctx:    context.Background(),
		Logger: log.L(),
	}
}

// WithContext set go context
func (c *Context) WithContext(ctx context.Context) *Context {
	return &Context{
		Ctx:    ctx,
		Logger: c.Logger,
	}
}

// GetContext returns real context
func (c *Context) GetContext() context.Context {
	return c.Ctx
}

// WithLogger set logger
func (c *Context) WithLogger(logger log.Logger) *Context {
	return &Context{
		Ctx:    c.Ctx,
		Logger: logger,
	}
}

// L returns real logger
func (c *Context) L() log.Logger {
	return c.Logger
}
