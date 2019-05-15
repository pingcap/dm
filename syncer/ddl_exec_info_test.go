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

package syncer

import (
	"time"
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/dm/dm/pb"
)

var _ = Suite(&testDDLExecInfoSuite{})

type testDDLExecInfoSuite struct{}

func (t *testDDLExecInfoSuite) TestDDLExecItem(c *C) {
	ddlExecInfo := NewDDLExecInfo()
	c.Assert(ddlExecInfo.status.Get(), Equals, ddlExecIdle)

	ddlExecInfo.Renew()
	c.Assert(ddlExecInfo.status.Get(), Equals, ddlExecIdle)

	ddls := []string{"create database test"}
	go func() {
		select {
		case _ = <-ddlExecInfo.Chan(ddls):
		}
	}()

	time.Sleep(100*time.Millisecond)
	c.Assert(ddlExecInfo.BlockingDDLs(), DeepEquals, ddls)

	ddlExecInfo.ClearBlockingDDL()
	c.Assert(ddlExecInfo.BlockingDDLs(), IsNil)

	ctx, _ := context.WithTimeout(context.Background(), time.Second)
	err := ddlExecInfo.Send(ctx, newDDLExecItem(new(pb.ExecDDLRequest)))
	c.Assert(err, IsNil)

	ddlExecInfo.Close()
	c.Assert(ddlExecInfo.status.Get(), Equals, ddlExecClosed)
}