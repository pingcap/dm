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
	"bytes"
	"io"
	"io/ioutil"
	"path/filepath"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	gmysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"

	"github.com/pingcap/dm/pkg/binlog/event"
	"github.com/pingcap/dm/pkg/gtid"
)

var (
	_ = check.Suite(&testFileUtilSuite{})
)

type testFileUtilSuite struct {
}

func (t *testFileUtilSuite) TestCheckBinlogHeaderExist(c *check.C) {
	// file not exists
	filename := filepath.Join(c.MkDir(), "test-mysql-bin.000001")
	exist, err := checkBinlogHeaderExist(filename)
	c.Assert(err, check.ErrorMatches, ".*no such file or directory.*")
	c.Assert(exist, check.IsFalse)

	// empty file
	err = ioutil.WriteFile(filename, nil, 0644)
	c.Assert(err, check.IsNil)
	exist, err = checkBinlogHeaderExist(filename)
	c.Assert(err, check.IsNil)
	c.Assert(exist, check.IsFalse)

	// no enough data
	err = ioutil.WriteFile(filename, replication.BinLogFileHeader[:len(replication.BinLogFileHeader)-1], 0644)
	c.Assert(err, check.IsNil)
	exist, err = checkBinlogHeaderExist(filename)
	c.Assert(err, check.ErrorMatches, ".*has no enough data.*")
	c.Assert(exist, check.IsFalse)

	// equal
	err = ioutil.WriteFile(filename, replication.BinLogFileHeader, 0644)
	c.Assert(err, check.IsNil)
	exist, err = checkBinlogHeaderExist(filename)
	c.Assert(err, check.IsNil)
	c.Assert(exist, check.IsTrue)

	// more data
	err = ioutil.WriteFile(filename, bytes.Repeat(replication.BinLogFileHeader, 2), 0644)
	c.Assert(err, check.IsNil)
	exist, err = checkBinlogHeaderExist(filename)
	c.Assert(err, check.IsNil)
	c.Assert(exist, check.IsTrue)

	// invalid data
	invalidData := make([]byte, len(replication.BinLogFileHeader))
	copy(invalidData, replication.BinLogFileHeader)
	invalidData[0] = uint8(invalidData[0]) + 1
	err = ioutil.WriteFile(filename, invalidData, 0644)
	c.Assert(err, check.IsNil)
	exist, err = checkBinlogHeaderExist(filename)
	c.Assert(err, check.ErrorMatches, ".*header not valid.*")
	c.Assert(exist, check.IsFalse)
}

func (t *testFileUtilSuite) TestCheckFormatDescriptionEventExist(c *check.C) {
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
			Flags:     0x01,
		}
		latestPos uint32 = 4
	)
	formatDescEv, err := event.GenFormatDescriptionEvent(header, latestPos)
	c.Assert(err, check.IsNil)

	// file not exists
	filename := filepath.Join(c.MkDir(), "test-mysql-bin.000001")
	exist, err := checkFormatDescriptionEventExist(filename)
	c.Assert(err, check.ErrorMatches, ".*no such file or directory.*")
	c.Assert(exist, check.IsFalse)

	// empty file
	err = ioutil.WriteFile(filename, nil, 0644)
	c.Assert(err, check.IsNil)
	exist, err = checkFormatDescriptionEventExist(filename)
	c.Assert(err, check.ErrorMatches, ".*no binlog file header at the beginning.*")
	c.Assert(exist, check.IsFalse)

	// only file header
	err = ioutil.WriteFile(filename, replication.BinLogFileHeader, 0644)
	c.Assert(err, check.IsNil)
	exist, err = checkFormatDescriptionEventExist(filename)
	c.Assert(err, check.IsNil)
	c.Assert(exist, check.IsFalse)

	// no enough data, < EventHeaderSize
	var buff bytes.Buffer
	buff.Write(replication.BinLogFileHeader)
	buff.Write(formatDescEv.RawData[:replication.EventHeaderSize-1])
	err = ioutil.WriteFile(filename, buff.Bytes(), 0644)
	c.Assert(err, check.IsNil)
	exist, err = checkFormatDescriptionEventExist(filename)
	c.Assert(errors.Cause(err), check.Equals, io.EOF)
	c.Assert(exist, check.IsFalse)

	// no enough data, = EventHeaderSize
	buff.Reset()
	buff.Write(replication.BinLogFileHeader)
	buff.Write(formatDescEv.RawData[:replication.EventHeaderSize])
	err = ioutil.WriteFile(filename, buff.Bytes(), 0644)
	c.Assert(err, check.IsNil)
	exist, err = checkFormatDescriptionEventExist(filename)
	c.Assert(err, check.ErrorMatches, ".*get event err EOF.*")
	c.Assert(exist, check.IsFalse)

	// no enough data, > EventHeaderSize, < EventSize
	buff.Reset()
	buff.Write(replication.BinLogFileHeader)
	buff.Write(formatDescEv.RawData[:replication.EventHeaderSize+1])
	err = ioutil.WriteFile(filename, buff.Bytes(), 0644)
	c.Assert(err, check.IsNil)
	exist, err = checkFormatDescriptionEventExist(filename)
	c.Assert(err, check.ErrorMatches, ".*get event err EOF.*")
	c.Assert(exist, check.IsFalse)

	// exactly the event
	buff.Reset()
	buff.Write(replication.BinLogFileHeader)
	buff.Write(formatDescEv.RawData)
	dataCopy := make([]byte, buff.Len())
	copy(dataCopy, buff.Bytes())
	err = ioutil.WriteFile(filename, buff.Bytes(), 0644)
	c.Assert(err, check.IsNil)
	exist, err = checkFormatDescriptionEventExist(filename)
	c.Assert(err, check.IsNil)
	c.Assert(exist, check.IsTrue)

	// more than the event
	buff.Write([]byte("more data"))
	err = ioutil.WriteFile(filename, buff.Bytes(), 0644)
	c.Assert(err, check.IsNil)
	exist, err = checkFormatDescriptionEventExist(filename)
	c.Assert(err, check.IsNil)
	c.Assert(exist, check.IsTrue)

	// other event type
	queryEv, err := event.GenQueryEvent(header, latestPos, 0, 0, 0, nil, []byte("schema"), []byte("BEGIN"))
	c.Assert(err, check.IsNil)
	buff.Reset()
	buff.Write(replication.BinLogFileHeader)
	buff.Write(queryEv.RawData)
	err = ioutil.WriteFile(filename, buff.Bytes(), 0644)
	c.Assert(err, check.IsNil)
	exist, err = checkFormatDescriptionEventExist(filename)
	c.Assert(err, check.ErrorMatches, ".*expect FormatDescriptionEvent.*")
	c.Assert(exist, check.IsFalse)
}

func (t *testFileUtilSuite) TestCheckIsDuplicateEvent(c *check.C) {
	// use a binlog event generator to generate some binlog events.
	var (
		flavor                    = gmysql.MySQLFlavor
		serverID           uint32 = 11
		latestPos          uint32
		previousGTIDSetStr        = "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14,406a3f61-690d-11e7-87c5-6c92bf46f384:1-94321383,53bfca22-690d-11e7-8a62-18ded7a37b78:1-495,686e1ab6-c47e-11e7-a42c-6c92bf46f384:1-34981190,03fc0263-28c7-11e7-a653-6c0b84d59f30:1-7041423,05474d3c-28c7-11e7-8352-203db246dd3d:1-170,10b039fc-c843-11e7-8f6a-1866daf8d810:1-308290454"
		latestGTIDStr             = "3ccc475b-2343-11e7-be21-6c0b84d59f30:14"
		latestXID          uint64 = 10
		allEvents                 = make([]*replication.BinlogEvent, 0, 10)
		allData            bytes.Buffer
	)
	previousGTIDSet, err := gtid.ParserGTID(flavor, previousGTIDSetStr)
	c.Assert(err, check.IsNil)
	latestGTID, err := gtid.ParserGTID(flavor, latestGTIDStr)
	c.Assert(err, check.IsNil)
	g, err := event.NewGenerator(flavor, serverID, latestPos, latestGTID, previousGTIDSet, latestXID)
	c.Assert(err, check.IsNil)
	// file header with FormatDescriptionEvent and PreviousGTIDsEvent
	events, data, err := g.GenFileHeader()
	c.Assert(err, check.IsNil)
	allEvents = append(allEvents, events...)
	allData.Write(data)
	// CREATE DATABASE/TABLE
	queries := []string{
		"CRATE DATABASE `db`",
		"CREATE TABLE `db`.`tbl1` (c1 INT)",
		"CREATE TABLE `db`.`tbl2` (c1 INT)",
	}
	for _, query := range queries {
		events, data, err = g.GenDDLEvents("db", query)
		c.Assert(err, check.IsNil)
		allEvents = append(allEvents, events...)
		allData.Write(data)
	}
	// write the events to a file
	filename := filepath.Join(c.MkDir(), "test-mysql-bin.000001")
	err = ioutil.WriteFile(filename, allData.Bytes(), 0644)
	c.Assert(err, check.IsNil)

	// all events in the file
	for _, ev := range allEvents {
		duplicate, err2 := checkIsDuplicateEvent(filename, ev)
		c.Assert(err2, check.IsNil)
		c.Assert(duplicate, check.IsTrue)
	}

	// event not in the file, because its start pos > file size
	events, _, err = g.GenDDLEvents("", "BEGIN")
	c.Assert(err, check.IsNil)
	duplicate, err := checkIsDuplicateEvent(filename, events[0])
	c.Assert(err, check.IsNil)
	c.Assert(duplicate, check.IsFalse)

	// event not in the file, because event start pos < file size < event end pos, invalid
	lastEvent := allEvents[len(allEvents)-1]
	header := *lastEvent.Header // clone
	latestPos = lastEvent.Header.LogPos - lastEvent.Header.EventSize
	eventSize := lastEvent.Header.EventSize + 1 // greater event size
	dummyEv, err := event.GenDummyEvent(&header, latestPos, eventSize)
	c.Assert(err, check.IsNil)
	duplicate, err = checkIsDuplicateEvent(filename, dummyEv)
	c.Assert(err, check.ErrorMatches, ".*file size.*is between event's start pos.*")
	c.Assert(duplicate, check.IsFalse)

	// event's start pos not match any event in the file, invalid
	latestPos = lastEvent.Header.LogPos - lastEvent.Header.EventSize - 1 // start pos mismatch
	eventSize = lastEvent.Header.EventSize
	dummyEv, err = event.GenDummyEvent(&header, latestPos, eventSize)
	c.Assert(err, check.IsNil)
	duplicate, err = checkIsDuplicateEvent(filename, dummyEv)
	c.Assert(err, check.ErrorMatches, ".*get event from.*")
	c.Assert(duplicate, check.IsFalse)

	// event's start/end pos matched, but content mismatched, invalid
	latestPos = lastEvent.Header.LogPos - lastEvent.Header.EventSize
	eventSize = lastEvent.Header.EventSize
	dummyEv, err = event.GenDummyEvent(&header, latestPos, eventSize)
	c.Assert(err, check.IsNil)
	duplicate, err = checkIsDuplicateEvent(filename, dummyEv)
	c.Assert(err, check.ErrorMatches, ".*diff from passed-in event.*")
	c.Assert(duplicate, check.IsFalse)

	// file not exists, invalid
	filename += ".no-exist"
	duplicate, err = checkIsDuplicateEvent(filename, lastEvent)
	c.Assert(err, check.ErrorMatches, ".*get stat for.*")
	c.Assert(duplicate, check.IsFalse)
}
