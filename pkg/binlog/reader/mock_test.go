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
	"errors"
	"time"

	. "github.com/pingcap/check"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

var (
	_ = Suite(&testMockReaderSuite{})
)

type testMockReaderSuite struct {
}

type testCase struct {
	ev  *replication.BinlogEvent
	err error
}

func (t *testMockReaderSuite) TestRead(c *C) {
	r := NewMockReader()

	// some interface methods do nothing
	c.Assert(r.StartSyncByPos(mysql.Position{}), IsNil)
	c.Assert(r.StartSyncByGTID(nil), IsNil)
	c.Assert(r.Status(), IsNil)
	c.Assert(r.Close(), IsNil)

	cases := []testCase{
		{
			ev: &replication.BinlogEvent{
				RawData: []byte{1},
			},
			err: nil,
		},
		{
			ev: &replication.BinlogEvent{
				RawData: []byte{2},
			},
			err: nil,
		},
		{
			ev:  nil,
			err: errors.New("1"),
		},
		{
			ev: &replication.BinlogEvent{
				RawData: []byte{3},
			},
			err: nil,
		},
		{
			ev:  nil,
			err: errors.New("2"),
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	mockR := r.(*MockReader)
	go func() {
		for _, cs := range cases {
			if cs.err != nil {
				c.Assert(mockR.PushError(ctx, cs.err), IsNil)
			} else {
				c.Assert(mockR.PushEvent(ctx, cs.ev), IsNil)
			}
		}
	}()

	got := make([]testCase, 0, len(cases))
	for {
		ev, err := r.GetEvent(ctx)
		if err != nil {
			got = append(got, testCase{ev: nil, err: err})
		} else {
			got = append(got, testCase{ev: ev, err: nil})
		}
		if len(got) == len(cases) || ctx.Err() != nil {
			break
		}
	}

	c.Assert(ctx.Err(), IsNil)
	c.Assert(got, DeepEquals, cases)

	cancel() // cancel manually
	c.Assert(mockR.PushError(ctx, cases[0].err), Equals, ctx.Err())
	c.Assert(mockR.PushEvent(ctx, cases[0].ev), Equals, ctx.Err())
	ev, err := r.GetEvent(ctx)
	c.Assert(ev, IsNil)
	c.Assert(err, Equals, ctx.Err())
}
