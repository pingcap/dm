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

// binlog events generator for MySQL used to generate some binlog events for tests.
// Readability takes precedence over performance.

package event

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"

	"github.com/pingcap/dm/pkg/gtid"
)

var _ = Suite(&testGeneratorMySQLSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testGeneratorMySQLSuite struct {
}

func (t *testGeneratorMySQLSuite) TestGenEventHeader(c *C) {
	var (
		timestamp        = uint32(time.Now().Unix())
		eventType        = replication.FORMAT_DESCRIPTION_EVENT
		serverID  uint32 = 11
		eventSize uint32 = 109
		logPos    uint32 = 123
		flags     uint16 = 0x01
	)

	eh, data, err := GenEventHeader(timestamp, eventType, serverID, eventSize, logPos, flags)
	c.Assert(err, IsNil)
	c.Assert(uint8(len(data)), Equals, eventHeaderLen)
	c.Assert(eh.EventType, Equals, eventType)
	c.Assert(eh.ServerID, Equals, serverID)
	c.Assert(eh.EventSize, Equals, eventSize)
	c.Assert(eh.LogPos, Equals, logPos)
	c.Assert(eh.Flags, Equals, flags)
	c.Assert(eh.Timestamp, LessEqual, timestamp)

	timestamp += 1000
	eh, _, err = GenEventHeader(timestamp, eventType, serverID, eventSize, logPos, flags)
	c.Assert(err, IsNil)
	c.Assert(eh.Timestamp, Equals, timestamp)

	eventType = replication.ROTATE_EVENT
	eh, _, err = GenEventHeader(timestamp, eventType, serverID, eventSize, logPos, flags)
	c.Assert(err, IsNil)
	c.Assert(eh.EventType, Equals, eventType)

	serverID = 22
	eh, _, err = GenEventHeader(timestamp, eventType, serverID, eventSize, logPos, flags)
	c.Assert(err, IsNil)
	c.Assert(eh.ServerID, Equals, serverID)

	eventSize = 100
	eh, _, err = GenEventHeader(timestamp, eventType, serverID, eventSize, logPos, flags)
	c.Assert(err, IsNil)
	c.Assert(eh.EventSize, Equals, eventSize)

	logPos = 456
	eh, _, err = GenEventHeader(timestamp, eventType, serverID, eventSize, logPos, flags)
	c.Assert(err, IsNil)
	c.Assert(eh.LogPos, Equals, logPos)

	flags |= 0x0040
	eh, _, err = GenEventHeader(timestamp, eventType, serverID, eventSize, logPos, flags)
	c.Assert(err, IsNil)
	c.Assert(eh.Flags, Equals, flags)
}

func (t *testGeneratorMySQLSuite) TestGenFormatDescriptionEvent(c *C) {
	var (
		timestamp        = uint32(time.Now().Unix())
		serverID  uint32 = 11
		latestPos uint32 = 4
		flags     uint16 = 0x01
	)
	ev, err := GenFormatDescriptionEvent(timestamp, serverID, latestPos, flags)
	c.Assert(err, IsNil)

	// verify the header
	c.Assert(ev.Header.Timestamp, Equals, timestamp)
	c.Assert(ev.Header.ServerID, Equals, serverID)
	c.Assert(ev.Header.LogPos, Equals, latestPos+ev.Header.EventSize)
	c.Assert(ev.Header.Flags, Equals, flags)

	// some fields of FormatDescriptionEvent are a little hard to test, so we try to parse a binlog file.
	dir := c.MkDir()
	name := filepath.Join(dir, "mysql-bin-test.000001")
	f, err := os.Create(name)
	c.Assert(err, IsNil)
	defer f.Close()

	// write a binlog file header
	_, err = f.Write(replication.BinLogFileHeader)
	c.Assert(err, IsNil)

	// write the FormatDescriptionEvent
	_, err = f.Write(ev.RawData)
	c.Assert(err, IsNil)

	// should only receive one FormatDescriptionEvent
	onEventFunc := func(e *replication.BinlogEvent) error {
		c.Assert(e.Header, DeepEquals, ev.Header)
		c.Assert(e.Event, DeepEquals, ev.Event)
		c.Assert(e.RawData, DeepEquals, ev.RawData)
		return nil
	}

	parser2 := replication.NewBinlogParser()
	parser2.SetVerifyChecksum(true)
	err = parser2.ParseFile(name, 0, onEventFunc)
	c.Assert(err, IsNil)
}

func (t *testGeneratorMySQLSuite) TestGenPreviousGTIDEvent(c *C) {
	var (
		timestamp        = uint32(time.Now().Unix())
		serverID  uint32 = 11
		latestPos uint32 = 4
		flags     uint16 = 0x01
		str              = "9f61c5f9-1eef-11e9-b6cf-0242ac140003:1-5"
	)

	// go-mysql has no PreviousGTIDEvent struct defined, so we try to parse a binlog file.
	// always needing a FormatDescriptionEvent in the binlog file.
	formatDescEv, err := GenFormatDescriptionEvent(timestamp, serverID, latestPos, flags)
	c.Assert(err, IsNil)

	// update latestPos
	latestPos = formatDescEv.Header.LogPos

	// generate a PreviousGTIDEvent
	gSet, err := gtid.ParserGTID(mysql.MySQLFlavor, str)
	c.Assert(err, IsNil)

	previousGTIDData, err := GenPreviousGTIDEvent(timestamp, serverID, latestPos, flags, gSet)
	c.Assert(err, IsNil)

	dir := c.MkDir()
	name1 := filepath.Join(dir, "mysql-bin-test.000001")
	f1, err := os.Create(name1)
	c.Assert(err, IsNil)
	defer f1.Close()

	// write a binlog file header
	_, err = f1.Write(replication.BinLogFileHeader)
	c.Assert(err, IsNil)

	// write a FormatDescriptionEvent event
	_, err = f1.Write(formatDescEv.RawData)
	c.Assert(err, IsNil)

	// write the PreviousGTIDEvent
	_, err = f1.Write(previousGTIDData)
	c.Assert(err, IsNil)

	var count = 0
	// should only receive one FormatDescriptionEvent
	onEventFunc := func(e *replication.BinlogEvent) error {
		count++
		switch count {
		case 1: // FormatDescriptionEvent
			c.Assert(e.Header, DeepEquals, formatDescEv.Header)
			c.Assert(e.Event, DeepEquals, formatDescEv.Event)
			c.Assert(e.RawData, DeepEquals, formatDescEv.RawData)
		case 2: // PreviousGTIDEvent
			c.Assert(e.Header.EventType, Equals, replication.PREVIOUS_GTIDS_EVENT)
			c.Assert(e.RawData, DeepEquals, previousGTIDData)
		default:
			c.Fatalf("too many binlog events got, current is %+v", e.Header)
		}
		return nil
	}

	parser2 := replication.NewBinlogParser()
	parser2.SetVerifyChecksum(true)
	err = parser2.ParseFile(name1, 0, onEventFunc)
	c.Assert(err, IsNil)

	// multi GTID
	str = "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14,406a3f61-690d-11e7-87c5-6c92bf46f384:1-94321383,53bfca22-690d-11e7-8a62-18ded7a37b78:1-495,686e1ab6-c47e-11e7-a42c-6c92bf46f384:1-34981190,03fc0263-28c7-11e7-a653-6c0b84d59f30:1-7041423,05474d3c-28c7-11e7-8352-203db246dd3d:1-170,10b039fc-c843-11e7-8f6a-1866daf8d810:1-308290454"
	gSet, err = gtid.ParserGTID(mysql.MySQLFlavor, str)
	c.Assert(err, IsNil)

	previousGTIDData, err = GenPreviousGTIDEvent(timestamp, serverID, latestPos, flags, gSet)
	c.Assert(err, IsNil)

	// write another file
	name2 := filepath.Join(dir, "mysql-bin-test.000002")
	f2, err := os.Create(name2)
	c.Assert(err, IsNil)
	defer f2.Close()

	// write a binlog file header
	_, err = f2.Write(replication.BinLogFileHeader)
	c.Assert(err, IsNil)

	// write a FormatDescriptionEvent event
	_, err = f2.Write(formatDescEv.RawData)
	c.Assert(err, IsNil)

	// write the PreviousGTIDEvent
	_, err = f2.Write(previousGTIDData)
	c.Assert(err, IsNil)

	count = 0 // reset count
	err = parser2.ParseFile(name2, 0, onEventFunc)
	c.Assert(err, IsNil)
}
