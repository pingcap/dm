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
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/siddontang/go-mysql/mysql"
	gmysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"

	"github.com/pingcap/dm/pkg/binlog/event"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

var parseFileTimeout = 10 * time.Second

var _ = Suite(&testReaderSuite{})

type testReaderSuite struct {
	lastPos  uint32
	lastGTID gtid.Set
}

func TestReader(t *testing.T) {
	TestingT(t)
}

func (t *testReaderSuite) SetUpSuite(c *C) {
	var err error
	t.lastPos = 0
	t.lastGTID, err = gtid.ParserGTID(mysql.MySQLFlavor, "ba8f633f-1f15-11eb-b1c7-0242ac110002:0")
	c.Assert(err, IsNil)
}

func (t *testReaderSuite) TestParseFileBase(c *C) {
	var (
		filename         = "test-mysql-bin.000001"
		baseDir          = c.MkDir()
		offset           int64
		firstParse       = true
		possibleLast     = false
		baseEvents, _, _ = t.genBinlogEvents(c, t.lastPos, t.lastGTID)
		s                = newLocalStreamer()
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// no valid currentUUID provide, failed
	currentUUID := "invalid-current-uuid"
	relayDir := filepath.Join(baseDir, currentUUID)
	cfg := &BinlogReaderConfig{RelayDir: baseDir, Flavor: mysql.MySQLFlavor}
	r := NewBinlogReader(log.L(), cfg)
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
	fullPath := filepath.Join(relayDir, filename)
	cfg = &BinlogReaderConfig{RelayDir: baseDir, Flavor: mysql.MySQLFlavor}
	r = NewBinlogReader(log.L(), cfg)

	// relay log file not exists, failed
	needSwitch, needReParse, latestPos, nextUUID, nextBinlogName, err = r.parseFile(
		ctx, s, filename, offset, relayDir, firstParse, currentUUID, possibleLast)
	c.Assert(err, ErrorMatches, ".*(no such file or directory|The system cannot find the path specified).*")
	c.Assert(needSwitch, IsFalse)
	c.Assert(needReParse, IsFalse)
	c.Assert(latestPos, Equals, int64(0))
	c.Assert(nextUUID, Equals, "")
	c.Assert(nextBinlogName, Equals, "")

	// empty relay log file, failed, got EOF
	err = os.MkdirAll(relayDir, 0700)
	c.Assert(err, IsNil)
	f, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY, 0600)
	c.Assert(err, IsNil)
	defer f.Close()
	needSwitch, needReParse, latestPos, nextUUID, nextBinlogName, err = r.parseFile(
		ctx, s, filename, offset, relayDir, firstParse, currentUUID, possibleLast)
	c.Assert(errors.Cause(err), Equals, io.EOF)
	c.Assert(needSwitch, IsFalse)
	c.Assert(needReParse, IsFalse)
	c.Assert(latestPos, Equals, int64(0))
	c.Assert(nextUUID, Equals, "")
	c.Assert(nextBinlogName, Equals, "")

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
	t.purgeStreamer(c, s)

	// parse from a non-zero offset
	offset = int64(rotateEv.Header.LogPos - rotateEv.Header.EventSize)
	needSwitch, needReParse, latestPos, nextUUID, nextBinlogName, err = r.parseFile(
		ctx, s, filename, offset, relayDir, firstParse, currentUUID, possibleLast)
	c.Assert(err, IsNil)
	c.Assert(needSwitch, IsFalse)
	c.Assert(needReParse, IsFalse)
	c.Assert(latestPos, Equals, int64(rotateEv.Header.LogPos))
	c.Assert(nextUUID, Equals, "")
	c.Assert(nextBinlogName, Equals, "")

	// should only got a RotateEvent and a FormatDescriptionEven
	i = 0
	for {
		ev, err2 := s.GetEvent(ctx)
		c.Assert(err2, IsNil)
		switch ev.Header.EventType {
		case replication.ROTATE_EVENT:
			c.Assert(ev.RawData, DeepEquals, rotateEv.RawData)
			i++
		case replication.FORMAT_DESCRIPTION_EVENT:
			i++
		default:
			c.Fatalf("got unexpected event %+v", ev.Header)
		}
		if i >= 2 {
			break
		}
	}
	t.verifyNoEventsInStreamer(c, s)
}

func (t *testReaderSuite) TestParseFileRelaySubDirUpdated(c *C) {
	var (
		filename                      = "test-mysql-bin.000001"
		nextFilename                  = "test-mysql-bin.000002"
		baseDir                       = c.MkDir()
		offset                        int64
		firstParse                    = true
		possibleLast                  = true
		baseEvents, lastPos, lastGTID = t.genBinlogEvents(c, t.lastPos, t.lastGTID)
		currentUUID                   = "b60868af-5a6f-11e9-9ea3-0242ac160006.000001"
		relayDir                      = filepath.Join(baseDir, currentUUID)
		fullPath                      = filepath.Join(relayDir, filename)
		nextPath                      = filepath.Join(relayDir, nextFilename)
		s                             = newLocalStreamer()
		cfg                           = &BinlogReaderConfig{RelayDir: baseDir, Flavor: mysql.MySQLFlavor}
		r                             = NewBinlogReader(log.L(), cfg)
	)

	// create the current relay log file and write some events
	err := os.MkdirAll(relayDir, 0700)
	c.Assert(err, IsNil)
	f, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY, 0600)
	c.Assert(err, IsNil)
	defer f.Close()
	_, err = f.Write(replication.BinLogFileHeader)
	c.Assert(err, IsNil)
	for _, ev := range baseEvents {
		_, err = f.Write(ev.RawData)
		c.Assert(err, IsNil)
	}

	// no valid update for relay sub dir, timeout
	ctx1, cancel1 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel1()
	needSwitch, needReParse, latestPos, nextUUID, nextBinlogName, err := r.parseFile(
		ctx1, s, filename, offset, relayDir, firstParse, currentUUID, possibleLast)
	c.Assert(errors.Cause(err), Equals, ctx1.Err())
	c.Assert(needSwitch, IsFalse)
	c.Assert(needReParse, IsFalse)
	c.Assert(latestPos, Equals, int64(0))
	c.Assert(nextUUID, Equals, "")
	c.Assert(nextBinlogName, Equals, "")
	t.purgeStreamer(c, s)

	// current relay log file updated, need to re-parse it
	var wg sync.WaitGroup
	wg.Add(1)
	extraEvents, _, _ := t.genBinlogEvents(c, lastPos, lastGTID)
	go func() {
		defer wg.Done()
		time.Sleep(500 * time.Millisecond) // wait parseFile started
		_, err2 := f.Write(extraEvents[0].RawData)
		c.Assert(err2, IsNil)
	}()
	ctx2, cancel2 := context.WithTimeout(context.Background(), parseFileTimeout)
	defer cancel2()
	needSwitch, needReParse, latestPos, nextUUID, nextBinlogName, err = r.parseFile(
		ctx2, s, filename, offset, relayDir, firstParse, currentUUID, possibleLast)
	c.Assert(err, IsNil)
	c.Assert(needSwitch, IsFalse)
	c.Assert(needReParse, IsTrue)
	c.Assert(latestPos, Equals, int64(baseEvents[len(baseEvents)-1].Header.LogPos))
	c.Assert(nextUUID, Equals, "")
	c.Assert(nextBinlogName, Equals, "")
	wg.Wait()
	t.purgeStreamer(c, s)

	// new relay log file created, need to re-collect files
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(500 * time.Millisecond) // wait parseFile started
		err2 := ioutil.WriteFile(nextPath, replication.BinLogFileHeader, 0600)
		c.Assert(err2, IsNil)
	}()
	ctx3, cancel3 := context.WithTimeout(context.Background(), parseFileTimeout)
	defer cancel3()
	needSwitch, needReParse, latestPos, nextUUID, nextBinlogName, err = r.parseFile(
		ctx3, s, filename, offset, relayDir, firstParse, currentUUID, possibleLast)
	c.Assert(err, IsNil)
	c.Assert(needSwitch, IsFalse)
	c.Assert(needReParse, IsFalse)
	c.Assert(latestPos, Equals, int64(extraEvents[0].Header.LogPos))
	c.Assert(nextUUID, Equals, "")
	c.Assert(nextBinlogName, Equals, "")
	wg.Wait()
	t.purgeStreamer(c, s)
}

func (t *testReaderSuite) TestParseFileRelayNeedSwitchSubDir(c *C) {
	var (
		filename         = "test-mysql-bin.000001"
		nextFilename     = "test-mysql-bin.666888"
		baseDir          = c.MkDir()
		offset           int64
		firstParse       = true
		possibleLast     = true
		baseEvents, _, _ = t.genBinlogEvents(c, t.lastPos, t.lastGTID)
		currentUUID      = "b60868af-5a6f-11e9-9ea3-0242ac160006.000001"
		switchedUUID     = "b60868af-5a6f-11e9-9ea3-0242ac160007.000002"
		relayDir         = filepath.Join(baseDir, currentUUID)
		nextRelayDir     = filepath.Join(baseDir, switchedUUID)
		fullPath         = filepath.Join(relayDir, filename)
		nextFullPath     = filepath.Join(nextRelayDir, nextFilename)
		s                = newLocalStreamer()
		cfg              = &BinlogReaderConfig{RelayDir: baseDir, Flavor: mysql.MySQLFlavor}
		r                = NewBinlogReader(log.L(), cfg)
	)

	// create the current relay log file and write some events
	err := os.MkdirAll(relayDir, 0700)
	c.Assert(err, IsNil)
	f, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY, 0600)
	c.Assert(err, IsNil)
	defer f.Close()
	_, err = f.Write(replication.BinLogFileHeader)
	c.Assert(err, IsNil)
	for _, ev := range baseEvents {
		_, err = f.Write(ev.RawData)
		c.Assert(err, IsNil)
	}

	// invalid UUID in UUID list, error
	r.uuids = []string{currentUUID, "invalid.uuid"}
	ctx1, cancel1 := context.WithTimeout(context.Background(), parseFileTimeout)
	defer cancel1()
	needSwitch, needReParse, latestPos, nextUUID, nextBinlogName, err := r.parseFile(
		ctx1, s, filename, offset, relayDir, firstParse, currentUUID, possibleLast)
	c.Assert(err, ErrorMatches, ".*not valid.*")
	c.Assert(needSwitch, IsFalse)
	c.Assert(needReParse, IsFalse)
	c.Assert(latestPos, Equals, int64(0))
	c.Assert(nextUUID, Equals, "")
	c.Assert(nextBinlogName, Equals, "")
	t.purgeStreamer(c, s)

	// next sub dir exits, need to switch
	r.uuids = []string{currentUUID, switchedUUID}
	err = os.MkdirAll(nextRelayDir, 0700)
	c.Assert(err, IsNil)
	err = ioutil.WriteFile(nextFullPath, replication.BinLogFileHeader, 0600)
	c.Assert(err, IsNil)

	// has relay log file in next sub directory, need to switch
	ctx2, cancel2 := context.WithTimeout(context.Background(), parseFileTimeout)
	defer cancel2()
	needSwitch, needReParse, latestPos, nextUUID, nextBinlogName, err = r.parseFile(
		ctx2, s, filename, offset, relayDir, firstParse, currentUUID, possibleLast)
	c.Assert(err, IsNil)
	c.Assert(needSwitch, IsTrue)
	c.Assert(needReParse, IsFalse)
	c.Assert(latestPos, Equals, int64(0))
	c.Assert(nextUUID, Equals, switchedUUID)
	c.Assert(nextBinlogName, Equals, nextFilename)
	t.purgeStreamer(c, s)

	// NOTE: if we want to test the returned `needReParse` of `needSwitchSubDir`,
	// then we need to mock `fileSizeUpdated` or inject some delay or delay.
}

func (t *testReaderSuite) TestParseFileRelayWithIgnorableError(c *C) {
	var (
		filename         = "test-mysql-bin.000001"
		baseDir          = c.MkDir()
		offset           int64
		firstParse       = true
		possibleLast     = true
		baseEvents, _, _ = t.genBinlogEvents(c, t.lastPos, t.lastGTID)
		currentUUID      = "b60868af-5a6f-11e9-9ea3-0242ac160006.000001"
		relayDir         = filepath.Join(baseDir, currentUUID)
		fullPath         = filepath.Join(relayDir, filename)
		s                = newLocalStreamer()
		cfg              = &BinlogReaderConfig{RelayDir: baseDir, Flavor: mysql.MySQLFlavor}
		r                = NewBinlogReader(log.L(), cfg)
	)

	// create the current relay log file and write some events
	err := os.MkdirAll(relayDir, 0700)
	c.Assert(err, IsNil)
	f, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY, 0600)
	c.Assert(err, IsNil)
	defer f.Close()

	// file has no data, meet io.EOF error (when reading file header) and ignore it. but will get `context deadline exceeded` error
	ctx1, cancel1 := context.WithTimeout(context.Background(), parseFileTimeout)
	defer cancel1()
	needSwitch, needReParse, latestPos, nextUUID, nextBinlogName, err := r.parseFile(
		ctx1, s, filename, offset, relayDir, firstParse, currentUUID, possibleLast)
	c.Assert(errors.Cause(err), Equals, context.DeadlineExceeded)
	c.Assert(needSwitch, IsFalse)
	c.Assert(needReParse, IsFalse)
	c.Assert(latestPos, Equals, int64(0))
	c.Assert(nextUUID, Equals, "")
	c.Assert(nextBinlogName, Equals, "")

	_, err = f.Write(replication.BinLogFileHeader)
	c.Assert(err, IsNil)
	for _, ev := range baseEvents {
		_, err = f.Write(ev.RawData)
		c.Assert(err, IsNil)
	}
	_, err = f.Write([]byte("some invalid binlog event data"))
	c.Assert(err, IsNil)

	// meet `err EOF` error (when parsing binlog event) ignored
	ctx2, cancel2 := context.WithTimeout(context.Background(), parseFileTimeout)
	defer cancel2()
	needSwitch, needReParse, latestPos, nextUUID, nextBinlogName, err = r.parseFile(
		ctx2, s, filename, offset, relayDir, firstParse, currentUUID, possibleLast)
	c.Assert(err, IsNil)
	c.Assert(needSwitch, IsFalse)
	c.Assert(needReParse, IsTrue)
	c.Assert(latestPos, Equals, int64(baseEvents[len(baseEvents)-1].Header.LogPos))
	c.Assert(nextUUID, Equals, "")
	c.Assert(nextBinlogName, Equals, "")
}

func (t *testReaderSuite) TestUpdateUUIDs(c *C) {
	var (
		baseDir = c.MkDir()
		cfg     = &BinlogReaderConfig{RelayDir: baseDir, Flavor: mysql.MySQLFlavor}
		r       = NewBinlogReader(log.L(), cfg)
	)
	c.Assert(r.uuids, HasLen, 0)

	// index file not exists, got nothing
	err := r.updateUUIDs()
	c.Assert(err, IsNil)
	c.Assert(r.uuids, HasLen, 0)

	// valid UUIDs in the index file, got them back
	UUIDs := []string{
		"b60868af-5a6f-11e9-9ea3-0242ac160006.000001",
		"b60868af-5a6f-11e9-9ea3-0242ac160007.000002",
	}
	uuidBytes := t.uuidListToBytes(c, UUIDs)
	err = ioutil.WriteFile(r.indexPath, uuidBytes, 0600)
	c.Assert(err, IsNil)

	err = r.updateUUIDs()
	c.Assert(err, IsNil)
	c.Assert(r.uuids, DeepEquals, UUIDs)
}

func (t *testReaderSuite) TestStartSyncByPos(c *C) {
	var (
		filenamePrefix                = "test-mysql-bin.00000"
		baseDir                       = c.MkDir()
		baseEvents, lastPos, lastGTID = t.genBinlogEvents(c, t.lastPos, t.lastGTID)
		eventsBuf                     bytes.Buffer
		UUIDs                         = []string{
			"b60868af-5a6f-11e9-9ea3-0242ac160006.000001",
			"b60868af-5a6f-11e9-9ea3-0242ac160007.000002",
			"b60868af-5a6f-11e9-9ea3-0242ac160008.000003",
		}
		cfg      = &BinlogReaderConfig{RelayDir: baseDir, Flavor: mysql.MySQLFlavor}
		r        = NewBinlogReader(log.L(), cfg)
		startPos = gmysql.Position{Name: "test-mysql-bin|000001.000001"} // from the first relay log file in the first sub directory
	)

	// prepare binlog data
	_, err := eventsBuf.Write(replication.BinLogFileHeader)
	c.Assert(err, IsNil)
	for _, ev := range baseEvents {
		_, err = eventsBuf.Write(ev.RawData)
		c.Assert(err, IsNil)
	}

	// create the index file
	uuidBytes := t.uuidListToBytes(c, UUIDs)
	err = ioutil.WriteFile(r.indexPath, uuidBytes, 0600)
	c.Assert(err, IsNil)

	// create sub directories
	for _, uuid := range UUIDs {
		subDir := filepath.Join(baseDir, uuid)
		err = os.MkdirAll(subDir, 0700)
		c.Assert(err, IsNil)
	}

	// 1. generate relay log files
	// 1 for the first sub directory, 2 for the second directory and 3 for the third directory
	// so, write the same events data into (1+2+3) files.
	for i := 0; i < 3; i++ {
		for j := 1; j < i+2; j++ {
			filename := filepath.Join(baseDir, UUIDs[i], filenamePrefix+strconv.Itoa(j))
			err = ioutil.WriteFile(filename, eventsBuf.Bytes(), 0600)
			c.Assert(err, IsNil)
		}
	}

	// start the reader
	s, err := r.StartSyncByPos(startPos)
	c.Assert(err, IsNil)

	// get events from the streamer
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	obtainBaseEvents := make([]*replication.BinlogEvent, 0, (1+2+3)*len(baseEvents))
	for {
		ev, err2 := s.GetEvent(ctx)
		c.Assert(err2, IsNil)
		if ev.Header.Timestamp == 0 || ev.Header.LogPos == 0 {
			continue // ignore fake event
		}
		obtainBaseEvents = append(obtainBaseEvents, ev)
		if len(obtainBaseEvents) == cap(obtainBaseEvents) {
			break
		}
	}
	t.verifyNoEventsInStreamer(c, s)

	// verify obtain base events
	for i := 0; i < len(obtainBaseEvents); i += len(baseEvents) {
		c.Assert(obtainBaseEvents[i:i+len(baseEvents)], DeepEquals, baseEvents)
	}

	// 2. write more events to the last file
	lastFilename := filepath.Join(baseDir, UUIDs[2], filenamePrefix+strconv.Itoa(3))
	extraEvents, _, _ := t.genBinlogEvents(c, lastPos, lastGTID)
	lastF, err := os.OpenFile(lastFilename, os.O_WRONLY|os.O_APPEND, 0600)
	c.Assert(err, IsNil)
	defer lastF.Close()
	for _, ev := range extraEvents {
		_, err = lastF.Write(ev.RawData)
		c.Assert(err, IsNil)
	}

	// read extra events back
	obtainExtraEvents := make([]*replication.BinlogEvent, 0, len(extraEvents))
	for {
		ev, err2 := s.GetEvent(ctx)
		c.Assert(err2, IsNil)
		if ev.Header.Timestamp == 0 || ev.Header.LogPos == 0 || ev.Header.EventType == replication.FORMAT_DESCRIPTION_EVENT {
			continue // ignore fake event and FormatDescriptionEvent, go-mysql may send extra FormatDescriptionEvent
		}
		obtainExtraEvents = append(obtainExtraEvents, ev)
		if len(obtainExtraEvents) == cap(obtainExtraEvents) {
			break
		}
	}
	t.verifyNoEventsInStreamer(c, s)

	// verify obtain extra events
	c.Assert(obtainExtraEvents, DeepEquals, extraEvents)

	// 3. create new file in the last directory
	lastFilename = filepath.Join(baseDir, UUIDs[2], filenamePrefix+strconv.Itoa(4))
	err = ioutil.WriteFile(lastFilename, eventsBuf.Bytes(), 0600)
	c.Assert(err, IsNil)

	obtainExtraEvents2 := make([]*replication.BinlogEvent, 0, len(baseEvents)-1)
	for {
		ev, err2 := s.GetEvent(ctx)
		c.Assert(err2, IsNil)
		if ev.Header.Timestamp == 0 || ev.Header.LogPos == 0 || ev.Header.EventType == replication.FORMAT_DESCRIPTION_EVENT {
			continue // ignore fake event and FormatDescriptionEvent, go-mysql may send extra FormatDescriptionEvent
		}
		obtainExtraEvents2 = append(obtainExtraEvents2, ev)
		if len(obtainExtraEvents2) == cap(obtainExtraEvents2) {
			break
		}
	}
	t.verifyNoEventsInStreamer(c, s)

	// verify obtain extra events
	c.Assert(obtainExtraEvents2, DeepEquals, baseEvents[1:])

	// NOTE: load new UUIDs dynamically not supported yet

	// close the reader
	r.Close()
}

func (t *testReaderSuite) TestStartSyncByGTID(c *C) {
	var (
		baseDir         = c.MkDir()
		events          []*replication.BinlogEvent
		cfg             = &BinlogReaderConfig{RelayDir: baseDir, Flavor: mysql.MySQLFlavor}
		r               = NewBinlogReader(log.L(), cfg)
		lastPos         uint32
		lastGTID        gtid.Set
		previousGset, _ = gtid.ParserGTID(mysql.MySQLFlavor, "")
	)
	type FileEventType struct {
		filename   string
		eventTypes []replication.EventType
	}

	testCase := []struct {
		serverUUID     string
		uuid           string
		gtidStr        string
		fileEventTypes []FileEventType
	}{
		{
			"ba8f633f-1f15-11eb-b1c7-0242ac110002",
			"ba8f633f-1f15-11eb-b1c7-0242ac110002.000001",
			"ba8f633f-1f15-11eb-b1c7-0242ac110002:0",
			[]FileEventType{
				{
					"mysql.000001",
					[]replication.EventType{
						replication.PREVIOUS_GTIDS_EVENT,
						replication.QUERY_EVENT,
						replication.XID_EVENT,
						replication.QUERY_EVENT,
						replication.XID_EVENT,
						replication.ROTATE_EVENT,
					},
				},
				{
					"mysql.000002",
					[]replication.EventType{
						replication.PREVIOUS_GTIDS_EVENT,
						replication.QUERY_EVENT,
						replication.XID_EVENT,
						replication.QUERY_EVENT,
						replication.XID_EVENT,
						replication.ROTATE_EVENT,
					},
				},
				{
					"mysql.000003",
					[]replication.EventType{
						replication.PREVIOUS_GTIDS_EVENT,
						replication.QUERY_EVENT,
						replication.XID_EVENT,
						replication.QUERY_EVENT,
						replication.XID_EVENT,
					},
				},
			},
		},
		{
			"bf6227a7-1f15-11eb-9afb-0242ac110004",
			"bf6227a7-1f15-11eb-9afb-0242ac110004.000002",
			"bf6227a7-1f15-11eb-9afb-0242ac110004:20",
			[]FileEventType{
				{
					"mysql.000001",
					[]replication.EventType{
						replication.PREVIOUS_GTIDS_EVENT,
						replication.QUERY_EVENT,
						replication.XID_EVENT,
						replication.QUERY_EVENT,
						replication.XID_EVENT,
						replication.ROTATE_EVENT,
					},
				}, {
					"mysql.000002",
					[]replication.EventType{
						replication.PREVIOUS_GTIDS_EVENT,
						replication.QUERY_EVENT,
						replication.XID_EVENT,
						replication.QUERY_EVENT,
						replication.XID_EVENT,
						replication.ROTATE_EVENT,
					},
				}, {
					"mysql.000003",
					[]replication.EventType{
						replication.PREVIOUS_GTIDS_EVENT,
					},
				},
			},
		}, {
			"bcbf9d42-1f15-11eb-a41c-0242ac110003",
			"bcbf9d42-1f15-11eb-a41c-0242ac110003.000003",
			"bcbf9d42-1f15-11eb-a41c-0242ac110003:30",
			[]FileEventType{
				{
					"mysql.000001",
					[]replication.EventType{
						replication.PREVIOUS_GTIDS_EVENT,
						replication.QUERY_EVENT,
						replication.XID_EVENT,
						replication.QUERY_EVENT,
						replication.XID_EVENT,
					},
				},
			},
		},
	}

	for _, subDir := range testCase {
		r.uuids = append(r.uuids, subDir.uuid)
	}

	// write index file
	uuidBytes := t.uuidListToBytes(c, r.uuids)
	err := ioutil.WriteFile(r.indexPath, uuidBytes, 0600)
	c.Assert(err, IsNil)

	var allEvents []*replication.BinlogEvent

	// prePosMap record previous uuid's last file's size
	// when master switch, we get the previous uuid's last pos now
	prePosMap := make(map[string]uint32)
	for _, subDir := range testCase {
		prePosMap[subDir.serverUUID] = lastPos
		lastPos = 4
		lastGTID, err = gtid.ParserGTID(mysql.MySQLFlavor, subDir.gtidStr)
		c.Assert(err, IsNil)
		uuidDir := path.Join(baseDir, subDir.uuid)
		err = os.MkdirAll(uuidDir, 0700)
		c.Assert(err, IsNil)

		for _, fileEventType := range subDir.fileEventTypes {
			events, lastPos, lastGTID, previousGset = t.genEvents(c, fileEventType.eventTypes, lastPos, lastGTID, previousGset)
			allEvents = append(allEvents, events...)

			// write binlog file
			f, err2 := os.OpenFile(path.Join(uuidDir, fileEventType.filename), os.O_CREATE|os.O_WRONLY, 0600)
			c.Assert(err2, IsNil)
			_, err = f.Write(replication.BinLogFileHeader)
			c.Assert(err, IsNil)
			for _, ev := range events {
				_, err = f.Write(ev.RawData)
				c.Assert(err, IsNil)
			}
			f.Close()

			// write meta file
			meta := Meta{BinLogName: fileEventType.filename, BinLogPos: lastPos, BinlogGTID: previousGset.String()}
			metaFile, err2 := os.Create(path.Join(uuidDir, utils.MetaFilename))
			c.Assert(err2, IsNil)
			err = toml.NewEncoder(metaFile).Encode(meta)
			c.Assert(err, IsNil)
			metaFile.Close()
		}
	}

	startGTID, err := gtid.ParserGTID(mysql.MySQLFlavor, "")
	c.Assert(err, IsNil)
	s, err := r.StartSyncByGTID(startGTID.Origin().Clone())
	c.Assert(err, IsNil)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var obtainBaseEvents []*replication.BinlogEvent
	for {
		ev, err2 := s.GetEvent(ctx)
		c.Assert(err2, IsNil)
		if ev.Header.Timestamp == 0 || ev.Header.LogPos == 0 {
			continue // ignore fake event
		}
		obtainBaseEvents = append(obtainBaseEvents, ev)
		// start after the first nil previous event
		if len(obtainBaseEvents) == len(allEvents)-1 {
			break
		}
	}

	preGset, err := gtid.ParserGTID(mysql.MySQLFlavor, "")
	c.Assert(err, IsNil)
	// allEvents: [FORMAT_DESCRIPTION_EVENT, PREVIOUS_GTIDS_EVENT, GTID_EVENT, QUERY_EVENT...]
	// obtainBaseEvents: [FORMAT_DESCRIPTION_EVENT(generated), GTID_EVENT, QUERY_EVENT...]
	for i, event := range obtainBaseEvents {
		if i == 0 {
			c.Assert(event.Header.EventType, Equals, replication.FORMAT_DESCRIPTION_EVENT)
			continue
		}
		c.Assert(event.Header, DeepEquals, allEvents[i+1].Header)
		switch ev := event.Event.(type) {
		case *replication.GTIDEvent:
			pos, err2 := r.getPosByGTID(preGset.Origin().Clone())
			u, _ := uuid.FromBytes(ev.SID)
			c.Assert(err2, IsNil)
			// new uuid dir
			if len(preGset.String()) != 0 && !strings.Contains(preGset.String(), u.String()) {
				c.Assert(pos.Pos, Equals, prePosMap[u.String()], Commentf("a %d", i))
			} else {
				c.Assert(pos.Pos, Equals, event.Header.LogPos-event.Header.EventSize, Commentf("b %d", i))
			}
		case *replication.QueryEvent:
			err2 := preGset.Set(ev.GSet)
			c.Assert(err2, IsNil)
		case *replication.XIDEvent:
			err2 := preGset.Set(ev.GSet)
			c.Assert(err2, IsNil)
		}
	}
}

func (t *testReaderSuite) TestStartSyncError(c *C) {
	var (
		baseDir = c.MkDir()
		UUIDs   = []string{
			"b60868af-5a6f-11e9-9ea3-0242ac160006.000001",
		}
		cfg      = &BinlogReaderConfig{RelayDir: baseDir, Flavor: mysql.MySQLFlavor}
		startPos = gmysql.Position{Name: "test-mysql-bin|000001.000001"} // from the first relay log file in the first sub directory
	)

	r := NewBinlogReader(log.L(), cfg)
	err := r.checkRelayPos(startPos)
	c.Assert(err, ErrorMatches, ".*empty UUIDs not valid.*")

	// no startup pos specified
	s, err := r.StartSyncByPos(gmysql.Position{})
	c.Assert(terror.ErrBinlogFileNotSpecified.Equal(err), IsTrue)
	c.Assert(s, IsNil)

	// empty UUIDs
	s, err = r.StartSyncByPos(startPos)
	c.Assert(err, ErrorMatches, ".*empty UUIDs not valid.*")
	c.Assert(s, IsNil)

	s, err = r.StartSyncByGTID(t.lastGTID.Origin().Clone())
	c.Assert(err, ErrorMatches, ".*no relay subdir match gtid.*")
	c.Assert(s, IsNil)

	// write UUIDs into index file
	r = NewBinlogReader(log.L(), cfg) // create a new reader
	uuidBytes := t.uuidListToBytes(c, UUIDs)
	err = ioutil.WriteFile(r.indexPath, uuidBytes, 0600)
	c.Assert(err, IsNil)

	// the startup relay log file not found
	s, err = r.StartSyncByPos(startPos)
	c.Assert(err, ErrorMatches, fmt.Sprintf(".*%s.*not found.*", startPos.Name))
	c.Assert(s, IsNil)

	s, err = r.StartSyncByGTID(t.lastGTID.Origin().Clone())
	c.Assert(err, ErrorMatches, ".*load meta data.*no such file or directory.*")
	c.Assert(s, IsNil)

	// can not re-start the reader
	r.running = true
	s, err = r.StartSyncByPos(startPos)
	c.Assert(terror.ErrReaderAlreadyRunning.Equal(err), IsTrue)
	c.Assert(s, IsNil)
	r.Close()

	r.running = true
	s, err = r.StartSyncByGTID(t.lastGTID.Origin().Clone())
	c.Assert(terror.ErrReaderAlreadyRunning.Equal(err), IsTrue)
	c.Assert(s, IsNil)
	r.Close()

	// too big startPos
	uuid := UUIDs[0]
	err = os.MkdirAll(filepath.Join(baseDir, uuid), 0700)
	c.Assert(err, IsNil)
	parsedStartPosName := "test-mysql-bin.000001"
	relayLogFilePath := filepath.Join(baseDir, uuid, parsedStartPosName)
	err = ioutil.WriteFile(relayLogFilePath, make([]byte, 100), 0600)
	c.Assert(err, IsNil)
	startPos.Pos = 10000
	s, err = r.StartSyncByPos(startPos)
	c.Assert(terror.ErrRelayLogGivenPosTooBig.Equal(err), IsTrue)
	c.Assert(s, IsNil)
}

func (t *testReaderSuite) genBinlogEvents(c *C, latestPos uint32, latestGTID gtid.Set) ([]*replication.BinlogEvent, uint32, gtid.Set) {
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
		evs, err := event.GenDDLEvents(mysql.MySQLFlavor, 1, latestPos, latestGTID, fmt.Sprintf("db_%d", i), fmt.Sprintf("CREATE TABLE %d (c1 INT)", i))
		c.Assert(err, IsNil)
		events = append(events, evs.Events...)
		latestPos = evs.LatestPos
		latestGTID = evs.LatestGTID
	}

	return events, latestPos, latestGTID
}

func (t *testReaderSuite) genEvents(c *C, eventTypes []replication.EventType, latestPos uint32, latestGTID gtid.Set, previousGset gtid.Set) ([]*replication.BinlogEvent, uint32, gtid.Set, gtid.Set) {
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
		}
		events    = make([]*replication.BinlogEvent, 0, 10)
		pGset     = previousGset.Clone()
		originSet = pGset.Origin()
	)

	if latestPos <= 4 { // generate a FormatDescriptionEvent if needed
		ev, err := event.GenFormatDescriptionEvent(header, 4)
		c.Assert(err, IsNil)
		latestPos = ev.Header.LogPos
		events = append(events, ev)
	}

	for i, eventType := range eventTypes {
		switch eventType {
		case replication.QUERY_EVENT:
			evs, err := event.GenDDLEvents(mysql.MySQLFlavor, 1, latestPos, latestGTID, fmt.Sprintf("db_%d", i), fmt.Sprintf("CREATE TABLE %d (c1 int)", i))
			c.Assert(err, IsNil)
			events = append(events, evs.Events...)
			latestPos = evs.LatestPos
			latestGTID = evs.LatestGTID
			ev, ok := evs.Events[0].Event.(*replication.GTIDEvent)
			c.Assert(ok, IsTrue)
			u, _ := uuid.FromBytes(ev.SID)
			gs := fmt.Sprintf("%s:%d", u.String(), ev.GNO)
			err = originSet.Update(gs)
			c.Assert(err, IsNil)
		case replication.XID_EVENT:
			insertDMLData := []*event.DMLData{
				{
					TableID:    uint64(i),
					Schema:     fmt.Sprintf("db_%d", i),
					Table:      strconv.Itoa(i),
					ColumnType: []byte{gmysql.MYSQL_TYPE_INT24},
					Rows:       [][]interface{}{{int32(1)}, {int32(2)}},
				},
			}
			evs, err := event.GenDMLEvents(mysql.MySQLFlavor, 1, latestPos, latestGTID, replication.WRITE_ROWS_EVENTv2, 10, insertDMLData)
			c.Assert(err, IsNil)
			events = append(events, evs.Events...)
			latestPos = evs.LatestPos
			latestGTID = evs.LatestGTID
			ev, ok := evs.Events[0].Event.(*replication.GTIDEvent)
			c.Assert(ok, IsTrue)
			u, _ := uuid.FromBytes(ev.SID)
			gs := fmt.Sprintf("%s:%d", u.String(), ev.GNO)
			err = originSet.Update(gs)
			c.Assert(err, IsNil)
		case replication.ROTATE_EVENT:
			ev, err := event.GenRotateEvent(header, latestPos, []byte("next_log"), 4)
			c.Assert(err, IsNil)
			events = append(events, ev)
			latestPos = 4
		case replication.PREVIOUS_GTIDS_EVENT:
			ev, err := event.GenPreviousGTIDsEvent(header, latestPos, pGset)
			c.Assert(err, IsNil)
			events = append(events, ev)
			latestPos = ev.Header.LogPos
		}
	}
	err := pGset.Set(originSet)
	c.Assert(err, IsNil)
	return events, latestPos, latestGTID, pGset
}

func (t *testReaderSuite) purgeStreamer(c *C, s Streamer) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	ev, err := s.GetEvent(ctx)
	if err != ctx.Err() {
		c.Fatalf("got event %v with error %v from streamer", ev, err)
	}
}

func (t *testReaderSuite) uuidListToBytes(c *C, UUIDs []string) []byte {
	var buf bytes.Buffer
	for _, uuid := range UUIDs {
		_, err := buf.WriteString(uuid)
		c.Assert(err, IsNil)
		_, err = buf.WriteString("\n")
		c.Assert(err, IsNil)
	}
	return buf.Bytes()
}
