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
	"path/filepath"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
)

func TestSuite(t *testing.T) {
	TestingT(t)
}

var (
	_ = Suite(&testFileWriterSuite{})
)

type testFileWriterSuite struct {
}

func (t *testFileWriterSuite) TestWrite(c *C) {
	dir := c.MkDir()
	filename := filepath.Join(dir, "test-mysql-bin.000001")
	var (
		cfg = &FileWriterConfig{
			Filename: filename,
		}
		allData bytes.Buffer
	)

	w := NewFileWriter(cfg)
	c.Assert(w, NotNil)

	// check status, stageNew
	status := w.Status()
	fwStatus, ok := status.(*FileWriterStatus)
	c.Assert(ok, IsTrue)
	c.Assert(fwStatus.Stage, Equals, stageNew.String())
	c.Assert(fwStatus.Filename, Equals, filename)
	c.Assert(fwStatus.Offset, Equals, int64(allData.Len()))

	// not prepared
	data1 := []byte("test-data")
	err := w.Write(data1)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), stageNew.String()), IsTrue)
	err = w.Flush()
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), stageNew.String()), IsTrue)

	// start
	err = w.Start()
	c.Assert(err, IsNil)

	// check status, stagePrepared
	status = w.Status()
	fwStatus, ok = status.(*FileWriterStatus)
	c.Assert(ok, IsTrue)
	c.Assert(fwStatus.Stage, Equals, stagePrepared.String())
	c.Assert(fwStatus.Filename, Equals, filename)
	c.Assert(fwStatus.Offset, Equals, int64(allData.Len()))

	// re-prepare is invalid
	err = w.Start()
	c.Assert(err, NotNil)

	// write the data
	err = w.Write(data1)
	c.Assert(err, IsNil)
	allData.Write(data1)

	// write data again
	data2 := []byte("another-data")
	err = w.Write(data2)
	c.Assert(err, IsNil)
	allData.Write(data2)

	// test Flush interface method simply
	err = w.Flush()
	c.Assert(err, IsNil)

	// close the reader
	c.Assert(w.Close(), IsNil)

	// check status, stageClosed
	status = w.Status()
	fwStatus, ok = status.(*FileWriterStatus)
	c.Assert(ok, IsTrue)
	c.Assert(fwStatus.Stage, Equals, stageClosed.String())
	c.Assert(fwStatus.Filename, Equals, filename)
	c.Assert(fwStatus.Offset, Equals, int64(allData.Len()))

	// re-close is invalid
	c.Assert(w.Close(), NotNil)

	// can not Writer/Flush anymore
	c.Assert(w.Write(data2), NotNil)
	c.Assert(w.Flush(), NotNil)

	// try to read the data back
	f, err := os.Open(filename)
	c.Assert(err, IsNil)
	defer f.Close()

	dataLen := allData.Len()
	buf := make([]byte, dataLen)
	n, err := f.Read(buf)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, dataLen)
	c.Assert(buf, DeepEquals, allData.Bytes())

	n, err = f.Read(buf)
	c.Assert(errors.Cause(err), Equals, io.EOF)
}
