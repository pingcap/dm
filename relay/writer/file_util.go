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
	"context"
	"io"
	"os"
	"time"

	"github.com/pingcap/errors"
	gmysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"

	"github.com/pingcap/dm/pkg/binlog/reader"
)

// checkBinlogHeaderExist checks if the file has a binlog file header.
func checkBinlogHeaderExist(filename string) (bool, error) {
	f, err := os.Open(filename)
	if err != nil {
		return false, errors.Annotatef(err, "open file %s", filename)
	}
	defer f.Close()

	fileHeaderLen := len(replication.BinLogFileHeader)
	buff := make([]byte, fileHeaderLen)
	n, err := f.Read(buff)
	if n == 0 && err == io.EOF {
		return false, nil // empty file
	} else if n != fileHeaderLen {
		return false, errors.Errorf("binlog file %s has no enough data, only got % X", filename, buff[:n])
	} else if err != nil {
		return false, errors.Annotate(err, "read binlog header")
	}

	if !bytes.Equal(buff, replication.BinLogFileHeader) {
		return false, errors.Errorf("binlog file %s header not valid, got % X, expect % X", filename, buff, replication.BinLogFileHeader)
	}
	return true, nil
}

// checkFormatDescriptionEventExist checks if the file has a valid FormatDescriptionEvent.
func checkFormatDescriptionEventExist(filename string) (bool, error) {
	// FormatDescriptionEvent always follows the binlog file header
	exist, err := checkBinlogHeaderExist(filename)
	if err != nil {
		return false, errors.Annotatef(err, "check binlog file header for %s", filename)
	} else if !exist {
		return false, errors.Errorf("no binlog file header at the beginning for %s", filename)
	}

	f, err := os.Open(filename)
	if err != nil {
		return false, errors.Annotatef(err, "open file %s", filename)
	}
	defer f.Close()

	// check whether only the file header
	fileHeaderLen := len(replication.BinLogFileHeader)
	fs, err := f.Stat()
	if err != nil {
		return false, errors.Errorf("get stat for %s", filename)
	} else if fs.Size() == int64(fileHeaderLen) {
		return false, nil // only the file header
	}

	// seek to the beginning of the FormatDescriptionEvent
	_, err = f.Seek(int64(fileHeaderLen), io.SeekStart)
	if err != nil {
		return false, errors.Annotatef(err, "seek to %d for %s", fileHeaderLen, filename)
	}

	// parse a FormatDescriptionEvent
	var found bool
	onEventFunc := func(e *replication.BinlogEvent) error {
		if e.Header.EventType != replication.FORMAT_DESCRIPTION_EVENT {
			return errors.Errorf("got %+v, expect FormatDescriptionEvent", e.Header)
		}
		found = true
		return nil
	}

	eof, err := replication.NewBinlogParser().ParseSingleEvent(f, onEventFunc)
	if err != nil {
		return false, errors.Annotatef(err, "parse %s", filename)
	} else if eof {
		return false, errors.Annotatef(io.EOF, "parse %s", filename)
	}
	return found, nil
}

// checkIsDuplicateEvent checks if the event is a duplicate event in the file.
// NOTE: handle cases when file size > 4GB
func checkIsDuplicateEvent(filename string, ev *replication.BinlogEvent) (bool, error) {
	// 1. check event start/end pos with the file size
	fs, err := os.Stat(filename)
	if err != nil {
		return false, errors.Annotatef(err, "get stat for %s", filename)
	}
	evStartPos := int64(ev.Header.LogPos - ev.Header.EventSize)
	evEndPos := int64(ev.Header.LogPos)
	if fs.Size() <= evStartPos {
		return false, nil // the event not in the file
	} else if fs.Size() < evEndPos {
		// the file can not hold the whole event, often because the file is corrupt
		return false, errors.Errorf("file size %d is between event's start pos (%d) and end pos (%d)",
			fs.Size(), evStartPos, evEndPos)
	}

	// 2. use a FileReader to read events from the start pos of the passed-in event
	rCfg := &reader.FileReaderConfig{
		EnableRawMode: true,
	}
	r := reader.NewFileReader(rCfg)
	syncStartPos := gmysql.Position{Name: filename, Pos: uint32(evStartPos)}
	err = r.StartSyncByPos(syncStartPos)
	if err != nil {
		return false, errors.Annotatef(err, "start to reader event from %s", syncStartPos)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second) // read the file should not be too slow.
	defer cancel()
	var ev2 *replication.BinlogEvent
	for {
		ev2, err = r.GetEvent(ctx)
		if err != nil {
			return false, errors.Annotatef(err, "get event from %s", syncStartPos)
		} else if ev2 == nil {
			// in fact, this should not happen
			return false, errors.Errorf("get none event from %s", syncStartPos)
		}
		if evStartPos > int64(len(replication.BinLogFileHeader)) &&
			ev2.Header.EventType == replication.FORMAT_DESCRIPTION_EVENT {
			// a FORMAT_DESCRIPTION_EVENT is always got before other events
			// skip it if we do need it
			continue
		}
		// read one event (except FORMAT_DESCRIPTION_EVENT) is enough
		// because we starting from the start pos of the passed-in event
		break
	}
	if ev2.Header.LogPos != ev.Header.LogPos || bytes.Compare(ev2.RawData, ev.RawData) != 0 {
		return false, errors.Errorf("event from %s is %+v, diff from passed-in event %+v",
			syncStartPos, ev2.Header, ev.Header)
	}

	// duplicate in the file
	return true, nil
}
