// Copyright 2021 PingCAP, Inc.
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

package openapi

import (
	"testing"

	"github.com/pingcap/check"

	"github.com/pingcap/dm/pkg/terror"
)

var _ = check.Suite(&taskSuite{})

type taskSuite struct{}

func TestTask(t *testing.T) {
	check.TestingT(t)
}

func (t *taskSuite) TestTaskAdjust(c *check.C) {
	meta := "test"
	// test no error
	task1 := &Task{MetaSchema: &meta, OnDuplicate: TaskOnDuplicateError}
	c.Assert(task1.Adjust(), check.IsNil)
	c.Assert(*task1.MetaSchema, check.Equals, meta)

	// test error
	task2 := &Task{}
	c.Assert(terror.ErrOpenAPICommonError.Equal(task2.Adjust()), check.IsTrue)

	// test default meta
	task3 := &Task{OnDuplicate: TaskOnDuplicateError}
	c.Assert(task3.Adjust(), check.IsNil)
	c.Assert(*task3.MetaSchema, check.Equals, defaultMetaSchema)
}
