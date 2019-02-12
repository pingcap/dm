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
	"bytes"
	"encoding/binary"
	"hash/crc32"

	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/replication"

	"github.com/pingcap/dm/pkg/gtid"
)

const (
	// GTIDFlagsCommitYes represents a GTID flag with [commit=yes].
	// in `Row` binlog format, this will appear in GTID event before DDL query event.
	GTIDFlagsCommitYes uint8 = 1

	binlogVersion   uint16 = 4 // only binlog-version 4 supported now
	mysqlVersion           = "5.7.22-log"
	mysqlVersionLen        = 50                                 // fix-length
	eventHeaderLen         = uint8(replication.EventHeaderSize) // always 19
	crc32Len        uint32 = 4                                  // CRC32-length
)

var (
	// A array indexed by `Binlog-Event-Type - 1` to extract the length of the event specific header.
	// It is copied from a binlog file generated by MySQL 5.7.22-log.
	// The doc at https://dev.mysql.com/doc/internals/en/format-description-event.html does not include all of them.
	eventTypeHeaderLen = []byte{0x38, 0x0d, 0x00, 0x08, 0x00, 0x12, 0x00, 0x04, 0x04, 0x04, 0x04, 0x12, 0x00, 0x00, 0x5f, 0x00, 0x04, 0x1a, 0x08, 0x00, 0x00, 0x00, 0x08, 0x08, 0x08, 0x02, 0x00, 0x00, 0x00, 0x0a, 0x0a, 0x0a, 0x2a, 0x2a, 0x00, 0x12, 0x34, 0x00}
)

// GenEventHeader generates a EventHeader.
// ref: https://dev.mysql.com/doc/internals/en/binlog-event-header.html
func GenEventHeader(timestamp uint32, eventType replication.EventType, serverID uint32, eventSize uint32, logPos uint32, flags uint16) (*replication.EventHeader, []byte, error) {
	buf := new(bytes.Buffer)

	// timestamp, 4 bytes
	err := binary.Write(buf, binary.LittleEndian, timestamp)
	if err != nil {
		return nil, nil, errors.Annotatef(err, "write timestamp %d", timestamp)
	}

	// event_type, 1 byte
	err = binary.Write(buf, binary.LittleEndian, eventType)
	if err != nil {
		return nil, nil, errors.Annotatef(err, "write event_type %v", eventType)
	}

	// server_id, 4 bytes
	err = binary.Write(buf, binary.LittleEndian, serverID)
	if err != nil {
		return nil, nil, errors.Annotatef(err, "write server_id %d", serverID)
	}

	// event_size, 4 bytes
	err = binary.Write(buf, binary.LittleEndian, eventSize)
	if err != nil {
		return nil, nil, errors.Annotatef(err, "write event_size %d", eventSize)
	}

	// log_pos, 4 bytes
	err = binary.Write(buf, binary.LittleEndian, logPos)
	if err != nil {
		return nil, nil, errors.Annotatef(err, "write log_pos %d", logPos)
	}

	// flags, 2 bytes
	err = binary.Write(buf, binary.LittleEndian, flags)
	if err != nil {
		return nil, nil, errors.Annotatef(err, "write flags % X", flags)
	}

	eh := replication.EventHeader{}
	err = eh.Decode(buf.Bytes())
	if err != nil {
		return nil, nil, errors.Annotatef(err, "decode % X", buf.Bytes())
	}

	return &eh, buf.Bytes(), nil
}

// GenFormatDescriptionEvent generates a FormatDescriptionEvent.
// ref: https://dev.mysql.com/doc/internals/en/format-description-event.html.
func GenFormatDescriptionEvent(timestamp uint32, serverID uint32, latestPos uint32, flags uint16) (*replication.BinlogEvent, error) {
	buf := new(bytes.Buffer)

	// size of the event (header, post-header, payload, CRC32), 119 now
	eventSize := uint32(eventHeaderLen) + 2 + 50 + 4 + 1 + 39 + crc32Len

	// position of the next event
	logPos := latestPos + eventSize

	// generate header, `eventHeaderLen` bytes
	header, headerData, err := GenEventHeader(timestamp, replication.FORMAT_DESCRIPTION_EVENT, serverID, eventSize, logPos, flags)
	if err != nil {
		return nil, errors.Annotatef(err, "generate event header")
	}
	err = binary.Write(buf, binary.LittleEndian, headerData)
	if err != nil {
		return nil, errors.Annotatef(err, "write event header % X", headerData)
	}

	// binlog-version, 2 bytes
	err = binary.Write(buf, binary.LittleEndian, binlogVersion)
	if err != nil {
		return nil, errors.Annotatef(err, "write binlog-version %d", binlogVersion)
	}

	// mysql-server version, 50 bytes
	serverVer := make([]byte, mysqlVersionLen)
	copy(serverVer, []byte(mysqlVersion))
	err = binary.Write(buf, binary.LittleEndian, serverVer)
	if err != nil {
		return nil, errors.Annotatef(err, "write mysql-server %v", serverVer)
	}

	// create_timestamp, 4 bytes
	err = binary.Write(buf, binary.LittleEndian, timestamp)
	if err != nil {
		return nil, errors.Annotatef(err, "write create_timestamp %d", timestamp)
	}

	// event_header_length, 1 byte
	err = binary.Write(buf, binary.LittleEndian, eventHeaderLen)
	if err != nil {
		return nil, errors.Annotatef(err, "write event_header_length %d", eventHeaderLen)
	}

	// event type header length, 38 bytes now
	err = binary.Write(buf, binary.LittleEndian, eventTypeHeaderLen)
	if err != nil {
		return nil, errors.Annotatef(err, "write event type header length % X", eventTypeHeaderLen)
	}

	// checksum algorithm, 1 byte
	err = binary.Write(buf, binary.LittleEndian, replication.BINLOG_CHECKSUM_ALG_CRC32)
	if err != nil {
		return nil, errors.Annotatef(err, "write checksum algorithm % X", replication.BINLOG_CHECKSUM_ALG_CRC32)
	}

	// CRC32 checksum, 4 bytes
	checksum := crc32.ChecksumIEEE(buf.Bytes())
	err = binary.Write(buf, binary.LittleEndian, checksum)
	if err != nil {
		return nil, errors.Annotatef(err, "write CRC32 % X", checksum)
	}

	// decode event
	event := &replication.FormatDescriptionEvent{}
	err = event.Decode(buf.Bytes()[eventHeaderLen:]) // checksum also needed
	if err != nil {
		return nil, errors.Annotatef(err, "decode % X", buf.Bytes())
	}

	return &replication.BinlogEvent{RawData: buf.Bytes(), Header: header, Event: event}, nil
}

// GenPreviousGTIDsEvent generates a PreviousGTIDsEvent.
// go-mysql has no PreviousGTIDsEvent struct defined, so return the event's raw data instead.
// MySQL has no internal doc for PREVIOUS_GTIDS_EVENT.
// we ref:
//   a. https://github.com/vitessio/vitess/blob/28e7e5503a6c3d3b18d4925d95f23ebcb6f25c8e/go/mysql/binlog_event_mysql56.go#L56
//   b. https://dev.mysql.com/doc/internals/en/com-binlog-dump-gtid.html
func GenPreviousGTIDsEvent(timestamp uint32, serverID uint32, latestPos uint32, flags uint16, gSet gtid.Set) ([]byte, error) {
	if len(gSet.String()) == 0 {
		return nil, errors.NotValidf("empty GTID set")
	}

	origin := gSet.Origin()
	if origin == nil {
		return nil, errors.NotValidf("GTID set string %s for MySQL", gSet.String())
	}

	// event payload, GTID set encoded in it
	payload := origin.Encode()

	// size of the event (header, payload, CRC32)
	eventSize := uint32(eventHeaderLen) + uint32(len(payload)) + crc32Len

	// position of the next event
	logPos := latestPos + eventSize

	// generate header, `eventHeaderLen` bytes
	_, headerData, err := GenEventHeader(timestamp, replication.PREVIOUS_GTIDS_EVENT, serverID, eventSize, logPos, flags)
	if err != nil {
		return nil, errors.Annotatef(err, "generate event header")
	}

	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, headerData)
	if err != nil {
		return nil, errors.Annotatef(err, "write event header % X", headerData)
	}

	err = binary.Write(buf, binary.LittleEndian, payload)
	if err != nil {
		return nil, errors.Annotatef(err, "write event payload % X", payload)
	}

	// CRC32 checksum, 4 bytes
	checksum := crc32.ChecksumIEEE(buf.Bytes())
	err = binary.Write(buf, binary.LittleEndian, checksum)
	if err != nil {
		return nil, errors.Annotatef(err, "write CRC32 % X", checksum)
	}

	return buf.Bytes(), nil
}

// GenGTIDEvent generates a GTIDEvent.
// MySQL has no internal doc for GTID_EVENT.
// we ref the `GTIDEvent.Decode` in go-mysql.
// `uuid` is the UUID part of the GTID, like `9f61c5f9-1eef-11e9-b6cf-0242ac140003`.
// `gno` is the GNO part of the GTID, like `6`.
func GenGTIDEvent(timestamp uint32, serverID uint32, latestPos uint32, flags uint16, gtidFlags uint8, uuid string, gno int64, lastCommitted int64, sequenceNumber int64) (*replication.BinlogEvent, error) {
	payload := new(bytes.Buffer)

	// GTID flags, 1 byte
	err := binary.Write(payload, binary.LittleEndian, gtidFlags)
	if err != nil {
		return nil, errors.Annotatef(err, "write GTID flags % X", gtidFlags)
	}

	// SID, 16 bytes
	sid, err := ParseSID(uuid)
	if err != nil {
		return nil, errors.Annotatef(err, "parse UUID %s to SID", uuid)
	}
	err = binary.Write(payload, binary.LittleEndian, sid.Bytes())
	if err != nil {
		return nil, errors.Annotatef(err, "write SID % X", sid.Bytes())
	}

	// GNO, 8 bytes
	err = binary.Write(payload, binary.LittleEndian, gno)
	if err != nil {
		return nil, errors.Annotatef(err, "write GNO %d", gno)
	}

	// length of TypeCode, 1 byte
	err = binary.Write(payload, binary.LittleEndian, uint8(replication.LogicalTimestampTypeCode))
	if err != nil {
		return nil, errors.Annotatef(err, "write length of TypeCode %d", replication.LogicalTimestampTypeCode)
	}

	// lastCommitted, 8 bytes
	err = binary.Write(payload, binary.LittleEndian, lastCommitted)
	if err != nil {
		return nil, errors.Annotatef(err, "write last committed sequence number %d", lastCommitted)
	}

	// sequenceNumber, 8 bytes
	err = binary.Write(payload, binary.LittleEndian, sequenceNumber)
	if err != nil {
		return nil, errors.Annotatef(err, "write sequence number %d", sequenceNumber)
	}

	// size of the event (header, payload, CRC32)
	eventSize := uint32(eventHeaderLen) + uint32(payload.Len()) + crc32Len

	// position of the next event
	logPos := latestPos + eventSize

	// generate header, `eventHeaderLen` bytes
	header, headerData, err := GenEventHeader(timestamp, replication.GTID_EVENT, serverID, eventSize, logPos, flags)
	if err != nil {
		return nil, errors.Annotatef(err, "generate event header")
	}

	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, headerData)
	if err != nil {
		return nil, errors.Annotatef(err, "write event header % X", headerData)
	}

	err = binary.Write(buf, binary.LittleEndian, payload.Bytes())
	if err != nil {
		return nil, errors.Annotatef(err, "write event payload % X", payload.Bytes())
	}

	// decode event, before write checksum
	event := &replication.GTIDEvent{}
	err = event.Decode(buf.Bytes()[eventHeaderLen:])
	if err != nil {
		return nil, errors.Annotatef(err, "decode % X", buf.Bytes())
	}

	// CRC32 checksum, 4 bytes
	checksum := crc32.ChecksumIEEE(buf.Bytes())
	err = binary.Write(buf, binary.LittleEndian, checksum)
	if err != nil {
		return nil, errors.Annotatef(err, "write CRC32 % X", checksum)
	}

	return &replication.BinlogEvent{RawData: buf.Bytes(), Header: header, Event: event}, nil
}

// GenQueryEvent generates a QueryEvent.
// ref: https://dev.mysql.com/doc/internals/en/query-event.html
// ref: https://cloud.tencent.com/info/f86a7d1370a6beba28da726ab4a71c50.html
// `statusVars` should be generated out of this function, we can implement it later.
// `len(query)` must > 0.
func GenQueryEvent(timestamp uint32, serverID uint32, latestPos uint32, flags uint16, slaveProxyID uint32, executionTime uint32, errorCode uint16, statusVars []byte, schema []byte, query []byte) (*replication.BinlogEvent, error) {
	if len(query) == 0 {
		return nil, errors.NotValidf("empty query")
	}

	// Post-header
	postHeader := new(bytes.Buffer)

	// slave_proxy_id (thread_id), 4 bytes
	err := binary.Write(postHeader, binary.LittleEndian, slaveProxyID)
	if err != nil {
		return nil, errors.Annotatef(err, "write slave_proxy_id %d", slaveProxyID)
	}

	// executionTime, 4 bytes
	err = binary.Write(postHeader, binary.LittleEndian, executionTime)
	if err != nil {
		return nil, errors.Annotatef(err, "write execution %d", executionTime)
	}

	// schema length, 1 byte
	schemaLength := uint8(len(schema))
	err = binary.Write(postHeader, binary.LittleEndian, schemaLength)
	if err != nil {
		return nil, errors.Annotatef(err, "write schema length %d", schemaLength)
	}

	// error code, 2 bytes
	err = binary.Write(postHeader, binary.LittleEndian, errorCode)
	if err != nil {
		return nil, errors.Annotatef(err, "write error code %d", errorCode)
	}

	// status-vars length, 2 bytes
	statusVarsLength := uint16(len(statusVars))
	err = binary.Write(postHeader, binary.LittleEndian, statusVarsLength)
	if err != nil {
		return nil, errors.Annotatef(err, "write status-vars length %d", statusVarsLength)
	}

	// Payload
	payload := new(bytes.Buffer)

	// status-vars, status-vars length bytes
	if statusVarsLength > 0 {
		err = binary.Write(payload, binary.LittleEndian, statusVars)
		if err != nil {
			return nil, errors.Annotatef(err, "write status-vars % X", statusVars)
		}
	}

	// schema, schema length bytes
	if schemaLength > 0 {
		err = binary.Write(payload, binary.LittleEndian, schema)
		if err != nil {
			return nil, errors.Annotatef(err, "write schema % X", schema)
		}
	}

	// 0x00, 1 byte
	err = binary.Write(payload, binary.LittleEndian, uint8(0x00))
	if err != nil {
		return nil, errors.Annotatef(err, "write 0x00")
	}

	// query, len(query) bytes
	err = binary.Write(payload, binary.LittleEndian, query)
	if err != nil {
		return nil, errors.Annotatef(err, "write query % X", query)
	}

	// size of the event (header, post-header, payload, CRC32)
	eventSize := uint32(eventHeaderLen) + uint32(postHeader.Len()) + uint32(payload.Len()) + crc32Len

	// position of the next event
	logPos := latestPos + eventSize

	// generate header, `eventHeaderLen` bytes
	header, headerData, err := GenEventHeader(timestamp, replication.QUERY_EVENT, serverID, eventSize, logPos, flags)
	if err != nil {
		return nil, errors.Annotatef(err, "generate event header")
	}

	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, headerData)
	if err != nil {
		return nil, errors.Annotatef(err, "write event header % X", headerData)
	}

	err = binary.Write(buf, binary.LittleEndian, postHeader.Bytes())
	if err != nil {
		return nil, errors.Annotatef(err, "write event post header % X", postHeader.Bytes())
	}

	err = binary.Write(buf, binary.LittleEndian, payload.Bytes())
	if err != nil {
		return nil, errors.Annotatef(err, "write event payload % X", payload.Bytes())
	}

	// decode event, before write checksum
	event := &replication.QueryEvent{}
	err = event.Decode(buf.Bytes()[eventHeaderLen:])
	if err != nil {
		return nil, errors.Annotatef(err, "decode % X", buf.Bytes())
	}

	// CRC32 checksum, 4 bytes
	checksum := crc32.ChecksumIEEE(buf.Bytes())
	err = binary.Write(buf, binary.LittleEndian, checksum)
	if err != nil {
		return nil, errors.Annotatef(err, "write CRC32 % X", checksum)
	}

	return &replication.BinlogEvent{RawData: buf.Bytes(), Header: header, Event: event}, nil
}
