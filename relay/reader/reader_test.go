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
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/replication"

	br "github.com/pingcap/dm/pkg/binlog/reader"
)

var (
	_ = Suite(&testReaderSuite{})
)

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testReaderSuite struct {
}

func (t *testReaderSuite) TestInterface(c *C) {
	cases := []*replication.BinlogEvent{
		{RawData: []byte{1}},
		{RawData: []byte{2}},
		{RawData: []byte{3}},
	}

	cfg := &Config{
		SyncConfig: replication.BinlogSyncerConfig{
			ServerID: 101,
		},
		MasterID: "test-master",
	}

	// test with position
	r := NewReader(cfg)
	t.testInterfaceWithReader(c, r, cases)

	// test with GTID
	cfg.EnableGTID = true
	r = NewReader(cfg)
	t.testInterfaceWithReader(c, r, cases)
}

func (t *testReaderSuite) testInterfaceWithReader(c *C, r Reader, cases []*replication.BinlogEvent) {
	// replace underlying reader with a mock reader for testing
	concreteR := r.(*reader)
	c.Assert(concreteR, NotNil)
	mockR := br.NewMockReader()
	concreteR.in = mockR

	// start reader
	err := r.Start()
	c.Assert(err, IsNil)
	err = r.Start() // call multi times
	c.Assert(err, NotNil)

	// getEvent by pushing event to mock reader
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	concreteMR := mockR.(*br.MockReader)
	go func() {
		for _, cs := range cases {
			c.Assert(concreteMR.PushEvent(ctx, cs), IsNil)
		}
	}()
	obtained := make([]*replication.BinlogEvent, 0, len(cases))
	for {
		ev, err2 := r.GetEvent(ctx)
		c.Assert(err2, IsNil)
		obtained = append(obtained, ev)
		if len(obtained) == len(cases) {
			break
		}
	}
	c.Assert(obtained, DeepEquals, cases)

	// close reader
	err = r.Close()
	c.Assert(err, IsNil)
	err = r.Close()
	c.Assert(err, NotNil) // call multi times

	// getEvent from a closed reader
	ev, err := r.GetEvent(ctx)
	c.Assert(err, NotNil)
	c.Assert(ev, IsNil)
}

func (t *testReaderSuite) TestGetEventWithError(c *C) {
	cfg := &Config{
		SyncConfig: replication.BinlogSyncerConfig{
			ServerID: 101,
		},
		MasterID: "test-master",
	}

	r := NewReader(cfg)
	// replace underlying reader with a mock reader for testing
	concreteR := r.(*reader)
	c.Assert(concreteR, NotNil)
	mockR := br.NewMockReader()
	concreteR.in = mockR

	errOther := errors.New("other error")
	in := []error{
		context.DeadlineExceeded, // should be handled in the outer
		context.Canceled,         // ignorable
		errOther,
	}
	expected := []error{
		context.DeadlineExceeded,
		nil, // from ignorable
		errOther,
	}

	err := r.Start()
	c.Assert(err, IsNil)

	// getEvent by pushing event to mock reader
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	concreteMR := mockR.(*br.MockReader)
	go func() {
		for _, cs := range in {
			c.Assert(concreteMR.PushError(ctx, cs), IsNil)
		}
	}()

	obtained := make([]error, 0, len(expected))
	for {
		ev, err2 := r.GetEvent(ctx)
		err2 = errors.Cause(err2)
		c.Assert(ev, IsNil)
		obtained = append(obtained, err2)
		if err2 == errOther {
			break // all received
		}
	}
	c.Assert(obtained, DeepEquals, expected)
}
