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

package writer

import (
	"strings"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/siddontang/go-mysql/replication"

	"github.com/pingcap/dm/pkg/binlog/event"
)

var (
	_ = check.Suite(&testFileWriterSuite{})
)

func TestSuite(t *testing.T) {
	check.TestingT(t)
}

type testFileWriterSuite struct {
}

func (t *testFileWriterSuite) TestInterfaceMethods(c *check.C) {
	var (
		cfg = &FileConfig{
			RelayDir: c.MkDir(),
			Filename: "test-mysql-bin.000001",
		}
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
			Flags:     0x01,
		}
		latestPos uint32 = 4
		ev, _            = event.GenFormatDescriptionEvent(header, latestPos)
	)

	w := NewFileWriter(cfg)
	c.Assert(w, check.NotNil)

	// not prepared
	res, err := w.WriteEvent(ev)
	c.Assert(err, check.NotNil)
	c.Assert(strings.Contains(err.Error(), stageNew.String()), check.IsTrue)
	c.Assert(res, check.IsNil)
	err = w.Flush()
	c.Assert(err, check.NotNil)
	c.Assert(strings.Contains(err.Error(), stageNew.String()), check.IsTrue)

	// start writer
	err = w.Start()
	c.Assert(err, check.IsNil)
	c.Assert(w.Start(), check.NotNil) // re-start is invalid

	// write event
	res, err = w.WriteEvent(ev)
	c.Assert(err, check.IsNil)
	c.Assert(res, check.NotNil)

	// flush buffered data
	c.Assert(w.Flush(), check.IsNil)

	// close the writer
	c.Assert(w.Close(), check.IsNil)
	c.Assert(w.Close(), check.NotNil) // re-close is invalid
}

func (t *testFileWriterSuite) TestRelayDir(c *check.C) {
	cfg := &FileConfig{}

	// no dir specified
	w1 := NewFileWriter(cfg)
	defer w1.Close()
	err := w1.Start()
	c.Assert(err, check.NotNil)
	c.Assert(err, check.ErrorMatches, ".*no such file or directory.*")

	// invalid dir
	cfg.RelayDir = "invalid\x00path"
	w2 := NewFileWriter(cfg)
	defer w2.Close()
	err = w2.Start()
	c.Assert(err, check.NotNil)
	c.Assert(strings.Contains(err.Error(), "invalid argument"), check.IsTrue)

	// valid directory, but no filename specified
	cfg.RelayDir = c.MkDir()
	w3 := NewFileWriter(cfg)
	defer w3.Close()
	err = w3.Start()
	c.Assert(err, check.NotNil)
	c.Assert(strings.Contains(err.Error(), "is a directory"), check.IsTrue)

	// valid directory, valid filename
	cfg.Filename = "test-mysql-bin.000001"
	w4 := NewFileWriter(cfg)
	defer w4.Close()
	err = w4.Start()
	c.Assert(err, check.IsNil)
}
