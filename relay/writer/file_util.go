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
	"os"

	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/replication"
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
