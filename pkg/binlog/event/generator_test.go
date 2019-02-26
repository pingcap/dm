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
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
	gmysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"

	"github.com/pingcap/dm/pkg/gtid"
)

var _ = Suite(&testGeneratorSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testGeneratorSuite struct {
}

func (t *testGeneratorSuite) TestGenEventHeader(c *C) {
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

func (t *testGeneratorSuite) TestGenFormatDescriptionEvent(c *C) {
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

func (t *testGeneratorSuite) TestGenPreviousGTIDsEvent(c *C) {
	var (
		timestamp        = uint32(time.Now().Unix())
		serverID  uint32 = 11
		latestPos uint32 = 4
		flags     uint16 = 0x01
		str              = "9f61c5f9-1eef-11e9-b6cf-0242ac140003:1-5"
	)

	// go-mysql has no PreviousGTIDsEvent struct defined, so we try to parse a binlog file.
	// always needing a FormatDescriptionEvent in the binlog file.
	formatDescEv, err := GenFormatDescriptionEvent(timestamp, serverID, latestPos, flags)
	c.Assert(err, IsNil)

	// update latestPos
	latestPos = formatDescEv.Header.LogPos

	// generate a PreviousGTIDsEvent
	gSet, err := gtid.ParserGTID(gmysql.MySQLFlavor, str)
	c.Assert(err, IsNil)

	previousGTIDData, err := GenPreviousGTIDsEvent(timestamp, serverID, latestPos, flags, gSet)
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

	// write the PreviousGTIDsEvent
	_, err = f1.Write(previousGTIDData)
	c.Assert(err, IsNil)

	var count = 0
	onEventFunc := func(e *replication.BinlogEvent) error {
		count++
		switch count {
		case 1: // FormatDescriptionEvent
			c.Assert(e.Header, DeepEquals, formatDescEv.Header)
			c.Assert(e.Event, DeepEquals, formatDescEv.Event)
			c.Assert(e.RawData, DeepEquals, formatDescEv.RawData)
		case 2: // PreviousGTIDsEvent
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
	gSet, err = gtid.ParserGTID(gmysql.MySQLFlavor, str)
	c.Assert(err, IsNil)

	previousGTIDData, err = GenPreviousGTIDsEvent(timestamp, serverID, latestPos, flags, gSet)
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

	// write the PreviousGTIDsEvent
	_, err = f2.Write(previousGTIDData)
	c.Assert(err, IsNil)

	count = 0 // reset count
	err = parser2.ParseFile(name2, 0, onEventFunc)
	c.Assert(err, IsNil)
}

func (t *testGeneratorSuite) TestGenGTIDEvent(c *C) {
	var (
		timestamp            = uint32(time.Now().Unix())
		serverID      uint32 = 11
		latestPos     uint32 = 4
		flags         uint16 = 0x01
		gtidFlags            = GTIDFlagsCommitYes
		prevGTIDsStr         = "9f61c5f9-1eef-11e9-b6cf-0242ac140003:1-5"
		uuid                 = "9f61c5f9-1eef-11e9-b6cf-0242ac140003"
		gno           int64  = 6
		lastCommitted int64
	)
	sid, err := ParseSID(uuid)
	c.Assert(err, IsNil)

	// always needing a FormatDescriptionEvent in the binlog file.
	formatDescEv, err := GenFormatDescriptionEvent(timestamp, serverID, latestPos, flags)
	c.Assert(err, IsNil)
	latestPos = formatDescEv.Header.LogPos // update latestPos

	// also needing a PreviousGTIDsEvent after FormatDescriptionEvent
	gSet, err := gtid.ParserGTID(gmysql.MySQLFlavor, prevGTIDsStr)
	c.Assert(err, IsNil)
	previousGTIDData, err := GenPreviousGTIDsEvent(timestamp, serverID, latestPos, flags, gSet)
	c.Assert(err, IsNil)
	latestPos += uint32(len(previousGTIDData)) // update latestPos

	gtidEv, err := GenGTIDEvent(timestamp, serverID, latestPos, flags, gtidFlags, uuid, gno, lastCommitted, lastCommitted+1)
	c.Assert(err, IsNil)

	// verify the header
	c.Assert(gtidEv.Header.Timestamp, Equals, timestamp)
	c.Assert(gtidEv.Header.ServerID, Equals, serverID)
	c.Assert(gtidEv.Header.LogPos, Equals, latestPos+gtidEv.Header.EventSize)
	c.Assert(gtidEv.Header.Flags, Equals, flags)

	// verify the body
	gtidEvBody, ok := gtidEv.Event.(*replication.GTIDEvent)
	c.Assert(ok, IsTrue)
	c.Assert(gtidEvBody, NotNil)
	c.Assert(gtidEvBody.CommitFlag, Equals, gtidFlags)
	c.Assert(gtidEvBody.SID, DeepEquals, sid.Bytes())
	c.Assert(gtidEvBody.GNO, Equals, gno)
	c.Assert(gtidEvBody.LastCommitted, Equals, lastCommitted)
	c.Assert(gtidEvBody.SequenceNumber, Equals, lastCommitted+1)

	// write a binlog file, then try to parse it
	dir := c.MkDir()
	name := filepath.Join(dir, "mysql-bin-test.000001")
	f, err := os.Create(name)
	c.Assert(err, IsNil)
	defer f.Close()

	// write a binlog file.
	_, err = f.Write(replication.BinLogFileHeader)
	c.Assert(err, IsNil)
	_, err = f.Write(formatDescEv.RawData)
	c.Assert(err, IsNil)
	_, err = f.Write(previousGTIDData)
	c.Assert(err, IsNil)

	// write GTIDEvent.
	_, err = f.Write(gtidEv.RawData)
	c.Assert(err, IsNil)

	var count = 0
	onEventFunc := func(e *replication.BinlogEvent) error {
		count++
		switch count {
		case 1: // FormatDescriptionEvent
			c.Assert(e.Header, DeepEquals, formatDescEv.Header)
			c.Assert(e.Event, DeepEquals, formatDescEv.Event)
			c.Assert(e.RawData, DeepEquals, formatDescEv.RawData)
		case 2: // PreviousGTIDsEvent
			c.Assert(e.Header.EventType, Equals, replication.PREVIOUS_GTIDS_EVENT)
			c.Assert(e.RawData, DeepEquals, previousGTIDData)
		case 3: // GTIDEvent
			c.Assert(e.Header.EventType, Equals, replication.GTID_EVENT)
			c.Assert(e.RawData, DeepEquals, gtidEv.RawData)
		default:
			c.Fatalf("too many binlog events got, current is %+v", e.Header)
		}
		return nil
	}
	parser2 := replication.NewBinlogParser()
	parser2.SetVerifyChecksum(true)
	err = parser2.ParseFile(name, 0, onEventFunc)
	c.Assert(err, IsNil)
}

func (t *testGeneratorSuite) TestGenQueryEvent(c *C) {
	var (
		timestamp            = uint32(time.Now().Unix())
		serverID      uint32 = 11
		latestPos     uint32 = 4
		flags         uint16 = 0x01
		slaveProxyID  uint32 = 2
		executionTime uint32 = 12
		errorCode     uint16 = 13
		statusVars    []byte // nil
		schema        []byte // nil
		query         []byte // nil
	)

	// empty query, invalid
	queryEv, err := GenQueryEvent(timestamp, serverID, latestPos, flags, slaveProxyID, executionTime, errorCode, statusVars, schema, query)
	c.Assert(err, NotNil)
	c.Assert(queryEv, IsNil)

	// valid query
	query = []byte("BEGIN")
	queryEv, err = GenQueryEvent(timestamp, serverID, latestPos, flags, slaveProxyID, executionTime, errorCode, statusVars, schema, query)
	c.Assert(err, IsNil)
	c.Assert(queryEv, NotNil)

	// verify the header
	c.Assert(queryEv.Header.Timestamp, Equals, timestamp)
	c.Assert(queryEv.Header.ServerID, Equals, serverID)
	c.Assert(queryEv.Header.LogPos, Equals, latestPos+queryEv.Header.EventSize)
	c.Assert(queryEv.Header.Flags, Equals, flags)

	// verify the body
	queryEvBody, ok := queryEv.Event.(*replication.QueryEvent)
	c.Assert(ok, IsTrue)
	c.Assert(queryEvBody, NotNil)
	c.Assert(queryEvBody.SlaveProxyID, Equals, slaveProxyID)
	c.Assert(queryEvBody.ExecutionTime, Equals, executionTime)
	c.Assert(queryEvBody.ErrorCode, Equals, errorCode)
	c.Assert(queryEvBody.StatusVars, DeepEquals, []byte{})
	c.Assert(queryEvBody.Schema, DeepEquals, []byte{})
	c.Assert(queryEvBody.Query, DeepEquals, query)

	// non-empty schema
	schema = []byte("db")
	query = []byte("CREATE TABLE db.tbl (c1 int)")
	queryEv, err = GenQueryEvent(timestamp, serverID, latestPos, flags, slaveProxyID, executionTime, errorCode, statusVars, schema, query)
	c.Assert(err, IsNil)
	c.Assert(queryEv, NotNil)

	// verify the body
	queryEvBody, ok = queryEv.Event.(*replication.QueryEvent)
	c.Assert(ok, IsTrue)
	c.Assert(queryEvBody, NotNil)
	c.Assert(queryEvBody.Schema, DeepEquals, schema)
	c.Assert(queryEvBody.Query, DeepEquals, query)

	// non-empty statusVars
	statusVars = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x20, 0x00, 0xa0, 0x55, 0x00, 0x00, 0x00, 0x00, 0x06, 0x03, 0x73, 0x74, 0x64, 0x04, 0x21, 0x00, 0x21, 0x00, 0x08, 0x00, 0x0c, 0x01, 0x73, 0x68, 0x61, 0x72, 0x64, 0x5f, 0x64, 0x62, 0x5f, 0x31, 0x00}
	queryEv, err = GenQueryEvent(timestamp, serverID, latestPos, flags, slaveProxyID, executionTime, errorCode, statusVars, schema, query)
	c.Assert(err, IsNil)
	c.Assert(queryEv, NotNil)

	// verify the body
	queryEvBody, ok = queryEv.Event.(*replication.QueryEvent)
	c.Assert(ok, IsTrue)
	c.Assert(queryEvBody, NotNil)
	c.Assert(queryEvBody.StatusVars, DeepEquals, statusVars)
}

func (t *testGeneratorSuite) TestGenTableMapEvent(c *C) {
	var (
		timestamp         = uint32(time.Now().Unix())
		serverID   uint32 = 11
		latestPos  uint32 = 123
		flags      uint16 = 0x01
		tableID    uint64 = 108
		schema     []byte // nil
		table      []byte // nil
		columnType []byte // nil
	)

	// invalid schema, table and columnType
	tableMapEv, err := GenTableMapEvent(timestamp, serverID, latestPos, flags, tableID, schema, table, columnType)
	c.Assert(err, NotNil)
	c.Assert(tableMapEv, IsNil)

	// valid schema, invalid table and columnType
	schema = []byte("db")
	tableMapEv, err = GenTableMapEvent(timestamp, serverID, latestPos, flags, tableID, schema, table, columnType)
	c.Assert(err, NotNil)
	c.Assert(tableMapEv, IsNil)

	// valid schema and table, invalid columnType
	table = []byte("tbl")
	tableMapEv, err = GenTableMapEvent(timestamp, serverID, latestPos, flags, tableID, schema, table, columnType)
	c.Assert(err, NotNil)
	c.Assert(tableMapEv, IsNil)

	// all valid
	columnType = []byte{gmysql.MYSQL_TYPE_LONG}
	tableMapEv, err = GenTableMapEvent(timestamp, serverID, latestPos, flags, tableID, schema, table, columnType)
	c.Assert(err, IsNil)
	c.Assert(tableMapEv, NotNil)

	// verify the header
	c.Assert(tableMapEv.Header.Timestamp, Equals, timestamp)
	c.Assert(tableMapEv.Header.ServerID, Equals, serverID)
	c.Assert(tableMapEv.Header.LogPos, Equals, latestPos+tableMapEv.Header.EventSize)
	c.Assert(tableMapEv.Header.Flags, Equals, flags)

	// verify the body
	tableMapEvBody, ok := tableMapEv.Event.(*replication.TableMapEvent)
	c.Assert(ok, IsTrue)
	c.Assert(tableMapEvBody, NotNil)
	c.Assert(tableMapEvBody.TableID, Equals, tableID)
	c.Assert(tableMapEvBody.Flags, Equals, tableMapFlags)
	c.Assert(tableMapEvBody.Schema, DeepEquals, schema)
	c.Assert(tableMapEvBody.Table, DeepEquals, table)
	c.Assert(tableMapEvBody.ColumnCount, Equals, uint64(len(columnType)))
	c.Assert(tableMapEvBody.ColumnType, DeepEquals, columnType)

	// multi column type
	columnType = []byte{gmysql.MYSQL_TYPE_STRING, gmysql.MYSQL_TYPE_NEWDECIMAL, gmysql.MYSQL_TYPE_VAR_STRING, gmysql.MYSQL_TYPE_BLOB}
	tableMapEv, err = GenTableMapEvent(timestamp, serverID, latestPos, flags, tableID, schema, table, columnType)
	c.Assert(err, IsNil)
	c.Assert(tableMapEv, NotNil)

	// verify the body
	tableMapEvBody, ok = tableMapEv.Event.(*replication.TableMapEvent)
	c.Assert(ok, IsTrue)
	c.Assert(tableMapEvBody, NotNil)
	c.Assert(tableMapEvBody.ColumnCount, Equals, uint64(len(columnType)))
	c.Assert(tableMapEvBody.ColumnType, DeepEquals, columnType)

	// unsupported column type
	columnType = []byte{gmysql.MYSQL_TYPE_NEWDATE}
	tableMapEv, err = GenTableMapEvent(timestamp, serverID, latestPos, flags, tableID, schema, table, columnType)
	c.Assert(err, NotNil)
	c.Assert(tableMapEv, IsNil)
}

func (t *testGeneratorSuite) TestGenRowsEvent(c *C) {
	var (
		timestamp         = uint32(time.Now().Unix())
		serverID   uint32 = 11
		latestPos  uint32 = 123
		flags      uint16 = 0x01
		tableID    uint64 = 108
		eventType         = replication.TABLE_MAP_EVENT
		rowsFlag          = RowFlagsEndOfStatement
		rows       [][]interface{}
		columnType []byte // nil
	)

	// invalid eventType, rows and columnType
	rowsEv, err := GenRowsEvent(timestamp, serverID, latestPos, flags, eventType, tableID, rowsFlag, rows, columnType)
	c.Assert(err, NotNil)
	c.Assert(rowsEv, IsNil)

	// valid eventType, invalid rows and columnType
	eventType = replication.WRITE_ROWS_EVENTv0
	rowsEv, err = GenRowsEvent(timestamp, serverID, latestPos, flags, eventType, tableID, rowsFlag, rows, columnType)
	c.Assert(err, NotNil)
	c.Assert(rowsEv, IsNil)

	// valid eventType and rows, invalid columnType
	row := []interface{}{int32(1)}
	rows = append(rows, row)
	rowsEv, err = GenRowsEvent(timestamp, serverID, latestPos, flags, eventType, tableID, rowsFlag, rows, columnType)
	c.Assert(err, NotNil)
	c.Assert(rowsEv, IsNil)

	// all valid
	columnType = []byte{gmysql.MYSQL_TYPE_LONG}
	rowsEv, err = GenRowsEvent(timestamp, serverID, latestPos, flags, eventType, tableID, rowsFlag, rows, columnType)
	c.Assert(err, IsNil)
	c.Assert(rowsEv, NotNil)

	// verify the header
	c.Assert(rowsEv.Header.Timestamp, Equals, timestamp)
	c.Assert(rowsEv.Header.ServerID, Equals, serverID)
	c.Assert(rowsEv.Header.LogPos, Equals, latestPos+rowsEv.Header.EventSize)
	c.Assert(rowsEv.Header.Flags, Equals, flags)
	c.Assert(rowsEv.Header.EventType, Equals, eventType)

	// verify the body
	rowsEvBody, ok := rowsEv.Event.(*replication.RowsEvent)
	c.Assert(ok, IsTrue)
	c.Assert(rowsEvBody, NotNil)
	c.Assert(rowsEvBody.Flags, Equals, rowsFlag)
	c.Assert(rowsEvBody.TableID, Equals, tableID)
	c.Assert(rowsEvBody.Table.TableID, Equals, tableID)
	c.Assert(rowsEvBody.ColumnCount, Equals, uint64(len(rows[0])))
	c.Assert(rowsEvBody.Version, Equals, 0) // WRITE_ROWS_EVENTv0
	c.Assert(rowsEvBody.ExtraData, IsNil)
	c.Assert(rowsEvBody.Rows, DeepEquals, rows)

	// multi rows, with different length, invalid
	rows = append(rows, []interface{}{int32(1), int32(2)})
	rowsEv, err = GenRowsEvent(timestamp, serverID, latestPos, flags, eventType, tableID, rowsFlag, rows, columnType)
	c.Assert(err, NotNil)
	c.Assert(rowsEv, IsNil)

	// multi rows, multi columns, valid
	rows = make([][]interface{}, 0, 2)
	rows = append(rows, []interface{}{int32(1), int32(2)})
	rows = append(rows, []interface{}{int32(3), int32(4)})
	columnType = []byte{gmysql.MYSQL_TYPE_LONG, gmysql.MYSQL_TYPE_LONG}
	rowsEv, err = GenRowsEvent(timestamp, serverID, latestPos, flags, eventType, tableID, rowsFlag, rows, columnType)
	c.Assert(err, IsNil)
	c.Assert(rowsEv, NotNil)
	// verify the body
	rowsEvBody, ok = rowsEv.Event.(*replication.RowsEvent)
	c.Assert(ok, IsTrue)
	c.Assert(rowsEvBody, NotNil)
	c.Assert(rowsEvBody.ColumnCount, Equals, uint64(len(rows[0])))
	c.Assert(rowsEvBody.Rows, DeepEquals, rows)

	// all valid event-type
	evTypes := []replication.EventType{
		replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2,
		replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2,
		replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2,
	}
	for _, eventType = range evTypes {
		rowsEv, err = GenRowsEvent(timestamp, serverID, latestPos, flags, eventType, tableID, rowsFlag, rows, columnType)
		c.Assert(err, IsNil)
		c.Assert(rowsEv, NotNil)
		c.Assert(rowsEv.Header.EventType, Equals, eventType)
	}

	// more column types
	rows = make([][]interface{}, 0, 1)
	rows = append(rows, []interface{}{int32(1), int8(2), int16(3), int32(4), int64(5),
		float32(1.23), float64(4.56), "string with type STRING"})
	columnType = []byte{gmysql.MYSQL_TYPE_LONG, gmysql.MYSQL_TYPE_TINY, gmysql.MYSQL_TYPE_SHORT, gmysql.MYSQL_TYPE_INT24, gmysql.MYSQL_TYPE_LONGLONG,
		gmysql.MYSQL_TYPE_FLOAT, gmysql.MYSQL_TYPE_DOUBLE, gmysql.MYSQL_TYPE_STRING}
	rowsEv, err = GenRowsEvent(timestamp, serverID, latestPos, flags, eventType, tableID, rowsFlag, rows, columnType)
	c.Assert(err, IsNil)
	c.Assert(rowsEv, NotNil)
	// verify the body
	rowsEvBody, ok = rowsEv.Event.(*replication.RowsEvent)
	c.Assert(ok, IsTrue)
	c.Assert(rowsEvBody, NotNil)
	c.Assert(rowsEvBody.ColumnCount, Equals, uint64(len(rows[0])))
	c.Assert(rowsEvBody.Rows, DeepEquals, rows)

	// column type mismatch
	rows[0][0] = int8(1)
	rowsEv, err = GenRowsEvent(timestamp, serverID, latestPos, flags, eventType, tableID, rowsFlag, rows, columnType)
	c.Assert(err, NotNil)
	c.Assert(rowsEv, IsNil)

	// NotSupported column type
	rows = make([][]interface{}, 0, 1)
	rows = append(rows, []interface{}{int32(1)})
	unsupportedTypes := []byte{gmysql.MYSQL_TYPE_VARCHAR, gmysql.MYSQL_TYPE_VAR_STRING,
		gmysql.MYSQL_TYPE_NEWDECIMAL, gmysql.MYSQL_TYPE_BIT,
		gmysql.MYSQL_TYPE_TIMESTAMP, gmysql.MYSQL_TYPE_TIMESTAMP2,
		gmysql.MYSQL_TYPE_DATETIME, gmysql.MYSQL_TYPE_DATETIME2,
		gmysql.MYSQL_TYPE_TIME, gmysql.MYSQL_TYPE_TIME2,
		gmysql.MYSQL_TYPE_YEAR, gmysql.MYSQL_TYPE_ENUM, gmysql.MYSQL_TYPE_SET,
		gmysql.MYSQL_TYPE_BLOB, gmysql.MYSQL_TYPE_JSON, gmysql.MYSQL_TYPE_GEOMETRY}
	for i := range unsupportedTypes {
		columnType = unsupportedTypes[i : i+1]
		rowsEv, err = GenRowsEvent(timestamp, serverID, latestPos, flags, eventType, tableID, rowsFlag, rows, columnType)
		c.Assert(err, NotNil)
		c.Assert(strings.Contains(err.Error(), "not supported"), IsTrue)
		c.Assert(rowsEv, IsNil)
	}
}

func (t *testGeneratorSuite) TestGenXIDEvent(c *C) {
	var (
		timestamp        = uint32(time.Now().Unix())
		serverID  uint32 = 11
		latestPos uint32 = 4
		flags     uint16 = 0x01
		xid       uint64 = 123
	)

	xidEv, err := GenXIDEvent(timestamp, serverID, latestPos, flags, xid)
	c.Assert(err, IsNil)
	c.Assert(xidEv, NotNil)

	// verify the header
	c.Assert(xidEv.Header.Timestamp, Equals, timestamp)
	c.Assert(xidEv.Header.ServerID, Equals, serverID)
	c.Assert(xidEv.Header.LogPos, Equals, latestPos+xidEv.Header.EventSize)
	c.Assert(xidEv.Header.Flags, Equals, flags)

	// verify the body
	xidEvBody, ok := xidEv.Event.(*replication.XIDEvent)
	c.Assert(ok, IsTrue)
	c.Assert(xidEvBody, NotNil)
	c.Assert(xidEvBody.XID, Equals, xid)
}

func (t *testGeneratorSuite) TestGenMariaDBGTIDListEvent(c *C) {
	var (
		timestamp          = uint32(time.Now().Unix())
		serverID  uint32   = 11
		latestPos uint32   = 4
		flags     uint16   = 0x01
		gSet      gtid.Set // invalid
	)

	// invalid gSet
	gtidListEv, err := GenMariaDBGTIDListEvent(timestamp, serverID, latestPos, flags, gSet)
	c.Assert(err, NotNil)
	c.Assert(gtidListEv, IsNil)

	// valid gSet with single GTID
	gSet, err = gtid.ParserGTID(gmysql.MariaDBFlavor, "1-2-3")
	c.Assert(err, IsNil)
	c.Assert(gSet, NotNil)
	mGSet, ok := gSet.Origin().(*gmysql.MariadbGTIDSet)
	c.Assert(ok, IsTrue)
	c.Assert(mGSet, NotNil)

	gtidListEv, err = GenMariaDBGTIDListEvent(timestamp, serverID, latestPos, flags, gSet)
	c.Assert(err, IsNil)
	c.Assert(gtidListEv, NotNil)

	// verify the header
	c.Assert(gtidListEv.Header.Timestamp, Equals, timestamp)
	c.Assert(gtidListEv.Header.ServerID, Equals, serverID)
	c.Assert(gtidListEv.Header.LogPos, Equals, latestPos+gtidListEv.Header.EventSize)
	c.Assert(gtidListEv.Header.Flags, Equals, flags)

	// verify the body
	gtidListEvBody, ok := gtidListEv.Event.(*replication.MariadbGTIDListEvent)
	c.Assert(ok, IsTrue)
	c.Assert(gtidListEvBody, NotNil)
	c.Assert(len(gtidListEvBody.GTIDs), Equals, 1)
	c.Assert(gtidListEvBody.GTIDs[0], DeepEquals, *mGSet.Sets[gtidListEvBody.GTIDs[0].DomainID])

	/* we need to fix go-mysql first
	// valid gSet with multi GTIDs
	gSet, err = gtid.ParserGTID(gmysql.MariaDBFlavor, "1-2-12,2-2-3,3-3-8,4-4-4")
	c.Assert(err, IsNil)
	c.Assert(gSet, NotNil)
	mGSet, ok = gSet.Origin().(*gmysql.MariadbGTIDSet)
	c.Assert(ok, IsTrue)
	c.Assert(mGSet, NotNil)

	gtidListEv, err = GenMariaDBGTIDListEvent(timestamp, serverID, latestPos, flags, gSet)
	c.Assert(err, IsNil)
	c.Assert(gtidListEv, NotNil)

	// verify the body
	gtidListEvBody, ok = gtidListEv.Event.(*replication.MariadbGTIDListEvent)
	c.Assert(ok, IsTrue)
	c.Assert(gtidListEvBody, NotNil)
	c.Assert(len(gtidListEvBody.GTIDs), Equals, 4)
	for _, mGTID := range gtidListEvBody.GTIDs {
		mGTID2, ok := mGSet.Sets[mGTID.DomainID]
		c.Assert(ok, IsTrue)
		c.Assert(mGTID, DeepEquals, *mGTID2)
	}*/
}

func (t *testGeneratorSuite) TestGenMariaDBGTIDEvent(c *C) {
	var (
		timestamp        = uint32(time.Now().Unix())
		serverID  uint32 = 11
		latestPos uint32 = 4
		flags     uint16 = 0x01
		seqNum    uint64 = 123
		domainID  uint32 = 456
	)

	gtidEv, err := GenMariaDBGTIDEvent(timestamp, serverID, latestPos, flags, seqNum, domainID)
	c.Assert(err, IsNil)
	c.Assert(gtidEv, NotNil)

	// verify the header
	c.Assert(gtidEv.Header.Timestamp, Equals, timestamp)
	c.Assert(gtidEv.Header.ServerID, Equals, serverID)
	c.Assert(gtidEv.Header.LogPos, Equals, latestPos+gtidEv.Header.EventSize)
	c.Assert(gtidEv.Header.Flags, Equals, flags)

	// verify the body
	gtidEvBody, ok := gtidEv.Event.(*replication.MariadbGTIDEvent)
	c.Assert(ok, IsTrue)
	c.Assert(gtidEvBody, NotNil)
	c.Assert(gtidEvBody.GTID.SequenceNumber, Equals, seqNum)
	c.Assert(gtidEvBody.GTID.DomainID, Equals, domainID)
}
