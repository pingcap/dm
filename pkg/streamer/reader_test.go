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

package streamer

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/replication"

	"github.com/pingcap/dm/pkg/binlog/event"
)

var _ = Suite(&testReaderSuite{})

type testReaderSuite struct {
}

func (t *testReaderSuite) TestParseFileBase(c *C) {
	var (
		filename     = "test-mysql-bin.000001"
		baseDir      = c.MkDir()
		offset       int64
		firstParse   = true
		possibleLast = false
		baseEvents   = t.genBinlogEvents(c, 0)
		s            = newLocalStreamer()
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// no valid currentUUID provide, failed
	currentUUID := "invalid-current-uuid"
	relayDir := filepath.Join(baseDir, currentUUID)
	fullPath := filepath.Join(relayDir, filename)
	cfg := &BinlogReaderConfig{RelayDir: relayDir}
	r := NewBinlogReader(cfg)
	needSwitch, needReParse, latestPos, nextUUID, nextBinlogName, err := r.parseFile(
		ctx, s, filename, offset, relayDir, firstParse, currentUUID, possibleLast)
	c.Assert(err, ErrorMatches, ".*invalid-current-uuid.*")
	c.Assert(needSwitch, IsFalse)
	c.Assert(needReParse, IsFalse)
	c.Assert(latestPos, Equals, int64(0))
	c.Assert(nextUUID, Equals, "")
	c.Assert(nextBinlogName, Equals, "")

	// change to valid currentUUID
	currentUUID = "b60868af-5a6f-11e9-9ea3-0242ac160006.000001"
	relayDir = filepath.Join(baseDir, currentUUID)
	fullPath = filepath.Join(relayDir, filename)
	cfg = &BinlogReaderConfig{RelayDir: relayDir}
	r = NewBinlogReader(cfg)

	// relay log file not exists, failed
	needSwitch, needReParse, latestPos, nextUUID, nextBinlogName, err = r.parseFile(
		ctx, s, filename, offset, relayDir, firstParse, currentUUID, possibleLast)
	c.Assert(err, ErrorMatches, ".*no such file or directory.*")

	// empty relay log file, failed, got EOF
	err = os.MkdirAll(relayDir, 0744)
	c.Assert(err, IsNil)
	f, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY, 0644)
	c.Assert(err, IsNil)
	defer f.Close()
	needSwitch, needReParse, latestPos, nextUUID, nextBinlogName, err = r.parseFile(
		ctx, s, filename, offset, relayDir, firstParse, currentUUID, possibleLast)
	c.Assert(errors.Cause(err), Equals, io.EOF)

	// write some events to binlog file
	_, err = f.Write(replication.BinLogFileHeader)
	c.Assert(err, IsNil)
	for _, ev := range baseEvents {
		_, err = f.Write(ev.RawData)
		c.Assert(err, IsNil)
	}

	t.purgeStreamer(c, s)

	// base test with only one valid binlog file
	needSwitch, needReParse, latestPos, nextUUID, nextBinlogName, err = r.parseFile(
		ctx, s, filename, offset, relayDir, firstParse, currentUUID, possibleLast)
	c.Assert(err, IsNil)
	c.Assert(needSwitch, IsFalse)
	c.Assert(needReParse, IsFalse)
	c.Assert(latestPos, Equals, int64(baseEvents[len(baseEvents)-1].Header.LogPos))
	c.Assert(nextUUID, Equals, "")
	c.Assert(nextBinlogName, Equals, "")

	// try get events back, firstParse should have fake RotateEvent
	var fakeRotateEventCount int
	var i = 0
	for {
		ev, err2 := s.GetEvent(ctx)
		c.Assert(err2, IsNil)
		if ev.Header.Timestamp == 0 || ev.Header.LogPos == 0 {
			if ev.Header.EventType == replication.ROTATE_EVENT {
				fakeRotateEventCount++
			}
			continue // ignore fake event
		}
		c.Assert(ev, DeepEquals, baseEvents[i])
		i++
		if i >= len(baseEvents) {
			break
		}
	}
	c.Assert(fakeRotateEventCount, Equals, 1)
	t.verifyNoEventsInStreamer(c, s)

	// try get events back, not firstParse should have no fake RotateEvent
	firstParse = false
	needSwitch, needReParse, latestPos, nextUUID, nextBinlogName, err = r.parseFile(
		ctx, s, filename, offset, relayDir, firstParse, currentUUID, possibleLast)
	c.Assert(err, IsNil)
	c.Assert(needSwitch, IsFalse)
	c.Assert(needReParse, IsFalse)
	c.Assert(latestPos, Equals, int64(baseEvents[len(baseEvents)-1].Header.LogPos))
	c.Assert(nextUUID, Equals, "")
	c.Assert(nextBinlogName, Equals, "")
	fakeRotateEventCount = 0
	i = 0
	for {
		ev, err2 := s.GetEvent(ctx)
		c.Assert(err2, IsNil)
		if ev.Header.Timestamp == 0 || ev.Header.LogPos == 0 {
			if ev.Header.EventType == replication.ROTATE_EVENT {
				fakeRotateEventCount++
			}
			continue // ignore fake event
		}
		c.Assert(ev, DeepEquals, baseEvents[i])
		i++
		if i >= len(baseEvents) {
			break
		}
	}
	c.Assert(fakeRotateEventCount, Equals, 0)
	t.verifyNoEventsInStreamer(c, s)

	// generate another non-fake RotateEvent
	rotateEv, err := event.GenRotateEvent(baseEvents[0].Header, uint32(latestPos), []byte("mysql-bin.888888"), 4)
	c.Assert(err, IsNil)
	_, err = f.Write(rotateEv.RawData)
	c.Assert(err, IsNil)

	// latest is still the end_log_pos of the last event, not the next relay file log file's position
	needSwitch, needReParse, latestPos, nextUUID, nextBinlogName, err = r.parseFile(
		ctx, s, filename, offset, relayDir, firstParse, currentUUID, possibleLast)
	c.Assert(err, IsNil)
	c.Assert(needSwitch, IsFalse)
	c.Assert(needReParse, IsFalse)
	c.Assert(latestPos, Equals, int64(rotateEv.Header.LogPos))
	c.Assert(nextUUID, Equals, "")
	c.Assert(nextBinlogName, Equals, "")
}

func (t *testReaderSuite) genBinlogEvents(c *C, latestPos uint32) []*replication.BinlogEvent {
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
		}
		events = make([]*replication.BinlogEvent, 0, 10)
	)

	if latestPos <= 4 { // generate a FormatDescriptionEvent if needed
		ev, err := event.GenFormatDescriptionEvent(header, 4)
		c.Assert(err, IsNil)
		latestPos = ev.Header.LogPos
		events = append(events, ev)
	}

	// for these tests, generates some DDL events is enough
	count := 5 + rand.Intn(5)
	for i := 0; i < count; i++ {
		schema := []byte(fmt.Sprintf("db_%d", i))
		query := []byte(fmt.Sprintf("CREATE TABLE %d (c1 INT)", i))
		ev, err := event.GenQueryEvent(header, latestPos, 0, 0, 0, nil, schema, query)
		c.Assert(err, IsNil)
		latestPos = ev.Header.LogPos
		events = append(events, ev)
	}

	return events
}

func (t *testReaderSuite) purgeStreamer(c *C, s Streamer) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	for {
		_, err := s.GetEvent(ctx)
		if err == nil {
			continue
		} else if err == ctx.Err() {
			return
		} else {
			c.Fatalf("purge streamer with error %v", err)
		}
	}
}

func (t *testReaderSuite) verifyNoEventsInStreamer(c *C, s Streamer) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	ev, err := s.GetEvent(ctx)
	if err != ctx.Err() {
		c.Fatalf("got event %v with error %v from streamer", ev, err)
	}
}
