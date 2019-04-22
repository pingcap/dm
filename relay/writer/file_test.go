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
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
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
	c.Assert(err, check.ErrorMatches, fmt.Sprintf(".*%s.*", stageNew))
	c.Assert(res, check.IsNil)
	err = w.Flush()
	c.Assert(err, check.ErrorMatches, fmt.Sprintf(".*%s.*", stageNew))

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
	var (
		cfg    = &FileConfig{}
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
			Flags:     0x01,
		}
		latestPos uint32 = 4
	)
	ev, err := event.GenFormatDescriptionEvent(header, latestPos)
	c.Assert(err, check.IsNil)

	// no dir specified
	w1 := NewFileWriter(cfg)
	defer w1.Close()
	c.Assert(w1.Start(), check.IsNil)
	result, err := w1.WriteEvent(ev)
	c.Assert(err, check.ErrorMatches, ".*no such file or directory.*")
	c.Assert(result, check.IsNil)

	// invalid dir
	cfg.RelayDir = "invalid\x00path"
	w2 := NewFileWriter(cfg)
	defer w2.Close()
	c.Assert(w2.Start(), check.IsNil)
	result, err = w2.WriteEvent(ev)
	c.Assert(err, check.ErrorMatches, ".*invalid argument.*")
	c.Assert(result, check.IsNil)

	// valid directory, but no filename specified
	cfg.RelayDir = c.MkDir()
	w3 := NewFileWriter(cfg)
	defer w3.Close()
	c.Assert(w3.Start(), check.IsNil)
	result, err = w3.WriteEvent(ev)
	c.Assert(err, check.ErrorMatches, ".*is a directory.*")
	c.Assert(result, check.IsNil)

	// valid directory, valid filename
	cfg.Filename = "test-mysql-bin.000001"
	w4 := NewFileWriter(cfg)
	defer w4.Close()
	c.Assert(w4.Start(), check.IsNil)
	result, err = w4.WriteEvent(ev)
	c.Assert(err, check.IsNil)
	c.Assert(result, check.NotNil)
}

func (t *testFileWriterSuite) TestFormatDescriptionEvent(c *check.C) {
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
	)
	formatDescEv, err := event.GenFormatDescriptionEvent(header, latestPos)
	c.Assert(err, check.IsNil)

	// write FormatDescriptionEvent to empty file
	w := NewFileWriter(cfg)
	defer w.Close()
	c.Assert(w.Start(), check.IsNil)
	result, err := w.WriteEvent(formatDescEv)
	c.Assert(err, check.IsNil)
	c.Assert(result, check.NotNil)
	c.Assert(result.Ignore, check.IsFalse)

	// write FormatDescriptionEvent again, ignore
	result, err = w.WriteEvent(formatDescEv)
	c.Assert(err, check.IsNil)
	c.Assert(result, check.NotNil)
	c.Assert(result.Ignore, check.IsTrue)

	// write another event
	queryEv, err := event.GenQueryEvent(header, latestPos, 0, 0, 0, nil, []byte("schema"), []byte("BEGIN"))
	c.Assert(err, check.IsNil)
	result, err = w.WriteEvent(queryEv)
	c.Assert(err, check.IsNil)
	c.Assert(result, check.NotNil)
	c.Assert(result.Ignore, check.IsFalse)

	// write FormatDescriptionEvent again, ignore
	result, err = w.WriteEvent(formatDescEv)
	c.Assert(err, check.IsNil)
	c.Assert(result, check.NotNil)
	c.Assert(result.Ignore, check.IsTrue)

	// check events by reading them back
	events := make([]*replication.BinlogEvent, 0, 2)
	var count = 0
	onEventFunc := func(e *replication.BinlogEvent) error {
		count++
		if count > 2 {
			c.Fatalf("too many events received, %+v", e.Header)
		}
		events = append(events, e)
		return nil
	}
	filename := filepath.Join(cfg.RelayDir, cfg.Filename)
	err = replication.NewBinlogParser().ParseFile(filename, 0, onEventFunc)
	c.Assert(err, check.IsNil)
	c.Assert(events, check.HasLen, 2)
	c.Assert(events[0], check.DeepEquals, formatDescEv)
	c.Assert(events[1], check.DeepEquals, queryEv)
}

func (t *testFileWriterSuite) TestRotateEventWithFormatDescriptionEvent(c *check.C) {
	var (
		cfg = &FileConfig{
			RelayDir: c.MkDir(),
			Filename: "test-mysql-bin.000001",
		}
		nextFilename        = "test-mysql-bin.000002"
		nextFilePos  uint64 = 4
		header              = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
			Flags:     0x01,
		}
		fakeHeader = &replication.EventHeader{
			Timestamp: 0, // mark as fake
			ServerID:  11,
			Flags:     0x01,
		}
		latestPos uint32 = 4
	)

	rotateEv, err := event.GenRotateEvent(header, latestPos, []byte(nextFilename), nextFilePos)
	c.Assert(err, check.IsNil)
	c.Assert(rotateEv, check.NotNil)

	fakeRotateEv, err := event.GenRotateEvent(fakeHeader, latestPos, []byte(nextFilename), nextFilePos)
	c.Assert(err, check.IsNil)
	c.Assert(fakeRotateEv, check.NotNil)

	formatDescEv, err := event.GenFormatDescriptionEvent(header, latestPos)
	c.Assert(err, check.IsNil)
	c.Assert(formatDescEv, check.NotNil)

	// 1: non-fake RotateEvent before FormatDescriptionEvent, invalid
	w1 := NewFileWriter(cfg)
	defer w1.Close()
	c.Assert(w1.Start(), check.IsNil)
	result, err := w1.WriteEvent(rotateEv)
	c.Assert(err, check.ErrorMatches, ".*no binlog file opened.*")
	c.Assert(result, check.IsNil)

	// 2. fake RotateEvent before FormatDescriptionEvent
	cfg.RelayDir = c.MkDir() // use a new relay directory
	w2 := NewFileWriter(cfg)
	defer w2.Close()
	c.Assert(w2.Start(), check.IsNil)
	result, err = w2.WriteEvent(fakeRotateEv)
	c.Assert(err, check.IsNil)
	c.Assert(result, check.NotNil)
	c.Assert(result.Ignore, check.IsTrue) // ignore fake RotateEvent

	result, err = w2.WriteEvent(formatDescEv)
	c.Assert(err, check.IsNil)
	c.Assert(result, check.NotNil)
	c.Assert(result.Ignore, check.IsFalse)

	// cfg.Filename should be empty, next file should contain only one FormatDescriptionEvent
	filename1 := filepath.Join(cfg.RelayDir, cfg.Filename)
	filename2 := filepath.Join(cfg.RelayDir, nextFilename)
	_, err = os.Stat(filename1)
	c.Assert(os.IsNotExist(err), check.IsTrue)
	data, err := ioutil.ReadFile(filename2)
	c.Assert(err, check.IsNil)
	fileHeaderLen := len(replication.BinLogFileHeader)
	c.Assert(len(data), check.Equals, fileHeaderLen+len(formatDescEv.RawData))
	c.Assert(data[fileHeaderLen:], check.DeepEquals, formatDescEv.RawData)

	// 3. FormatDescriptionEvent before fake RotateEvent
	cfg.RelayDir = c.MkDir() // use a new relay directory
	w3 := NewFileWriter(cfg)
	defer w3.Close()
	c.Assert(w3.Start(), check.IsNil)
	result, err = w3.WriteEvent(formatDescEv)
	c.Assert(err, check.IsNil)
	c.Assert(result, check.NotNil)
	c.Assert(result.Ignore, check.IsFalse)

	result, err = w3.WriteEvent(fakeRotateEv)
	c.Assert(err, check.IsNil)
	c.Assert(result, check.NotNil)
	c.Assert(result.Ignore, check.IsTrue)

	// cfg.Filename should contain only one FormatDescriptionEvent, next file should be empty
	filename1 = filepath.Join(cfg.RelayDir, cfg.Filename)
	filename2 = filepath.Join(cfg.RelayDir, nextFilename)
	_, err = os.Stat(filename2)
	c.Assert(os.IsNotExist(err), check.IsTrue)
	data, err = ioutil.ReadFile(filename1)
	c.Assert(err, check.IsNil)
	c.Assert(len(data), check.Equals, fileHeaderLen+len(formatDescEv.RawData))
	c.Assert(data[fileHeaderLen:], check.DeepEquals, formatDescEv.RawData)

	// 4. FormatDescriptionEvent before non-fake RotateEvent
	cfg.RelayDir = c.MkDir() // use a new relay directory
	w4 := NewFileWriter(cfg)
	defer w4.Close()
	c.Assert(w4.Start(), check.IsNil)
	result, err = w4.WriteEvent(formatDescEv)
	c.Assert(err, check.IsNil)
	c.Assert(result, check.NotNil)
	c.Assert(result.Ignore, check.IsFalse)

	result, err = w4.WriteEvent(rotateEv)
	c.Assert(err, check.IsNil)
	c.Assert(result, check.NotNil)
	c.Assert(result.Ignore, check.IsFalse)

	// cfg.Filename should contain both one FormatDescriptionEvent and one RotateEvent, next file should be empty
	filename1 = filepath.Join(cfg.RelayDir, cfg.Filename)
	filename2 = filepath.Join(cfg.RelayDir, nextFilename)
	_, err = os.Stat(filename2)
	c.Assert(os.IsNotExist(err), check.IsTrue)
	data, err = ioutil.ReadFile(filename1)
	c.Assert(err, check.IsNil)
	c.Assert(len(data), check.Equals, fileHeaderLen+len(formatDescEv.RawData)+len(rotateEv.RawData))
	c.Assert(data[fileHeaderLen:fileHeaderLen+len(formatDescEv.RawData)], check.DeepEquals, formatDescEv.RawData)
	c.Assert(data[fileHeaderLen+len(formatDescEv.RawData):], check.DeepEquals, rotateEv.RawData)
}
