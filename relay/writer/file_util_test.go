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
	"github.com/siddontang/go-mysql/replication"

	"github.com/pingcap/dm/pkg/binlog/event"
)

var (
	_ = check.Suite(&testUtilSuite{})
)

type testUtilSuite struct {
}

func (t *testFileWriterSuite) TestCheckBinlogHeaderExist(c *check.C) {
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

func (t *testFileWriterSuite) TestCheckFormatDescriptionEventExist(c *check.C) {
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
