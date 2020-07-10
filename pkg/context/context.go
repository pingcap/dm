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
	"time"

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

// NewContext return a new Context
func NewContext(ctx context.Context, logger log.Logger) *Context {
	return &Context{
		Ctx:    ctx,
		Logger: logger,
	}
}

// WithContext set go context
func (c *Context) WithContext(ctx context.Context) *Context {
	return &Context{
		Ctx:    ctx,
		Logger: c.Logger,
	}
}

// WithTimeout sets a timeout associated context.
func (c *Context) WithTimeout(timeout time.Duration) (*Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(c.Ctx, timeout)
	return &Context{
		Ctx:    ctx,
		Logger: c.Logger,
	}, cancel
}

// Context returns real context
func (c *Context) Context() context.Context {
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

type userCancelFlag struct{}

// WithUserCancelFlag add an flag to context, to indicate if cancel() is called by user
func WithUserCancelFlag(ctx context.Context) context.Context {
	// no need for a lock, because write to this variable should happens before cancel() and read happens
	// after ctx.Done()
	cancelByUser := false
	return context.WithValue(ctx, userCancelFlag{}, &cancelByUser)
}

// SetUserCancelFlag set flag for context WithUserCancelFlag, this function should be called before cancel()
// return true if successful set, false otherwise
func SetUserCancelFlag(ctx context.Context) bool {
	p := ctx.Value(userCancelFlag{})
	if p == nil {
		return false
	}
	if p, ok := p.(*bool); !ok {
		return false
	} else {
		*p = true
	}
	return true
}

// GetUserCancelFlag indicate if this context is canceled by user, should be called after ctx.Done()
func GetUserCancelFlag(ctx context.Context) bool {
	p := ctx.Value(userCancelFlag{})
	if p == nil {
		return false
	}
	if p, ok := p.(*bool); !ok {
		return false
	} else {
		return *p == true
	}
}

// ResetUserCancelFlag reset (clean) fla for context WithUserCancelFlag, this function should be called before funchtion
// runs who using this context
// return true if successful reset, false otherwise
func ResetUserCancelFlag(ctx context.Context) bool {
	p := ctx.Value(userCancelFlag{})
	if p == nil {
		return false
	}
	if p, ok := p.(*bool); !ok {
		return false
	} else {
		*p = false
	}
	return true
}